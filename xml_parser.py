"""
TED XML Parser – eForms XML-Vollnotizen anreichern
===================================================
Holt den vollständigen XML-Inhalt einer TED-Notice und extrahiert:
  - Beschreibungstext (cbc:Description)
  - Lose (cac:ProcurementProjectLot) mit Titel, CPV, Wert
  - Vergabewert / Auftragswert (cbc:PayableAmount / cbc:EstimatedOverallContractAmount)
  - Gewinner / Lieferant (cac:WinningParty, cac:AwardedTenderedProject)
  - Laufzeit (cbc:DurationMeasure)

Verwendung:
  from xml_parser import enrich_tender_from_xml
  result = enrich_tender_from_xml("123456-2026")
  # result = {"description": "...", "lots": [...], "awards": [...]}

  # In Scraper / API direkt aufrufen:
  enrich_and_save(db, tender_id)
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET
from datetime import date
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ── Namensräume (eForms / UBL / UNCEFACT) ────────────────────────────────────
NS = {
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efext": "http://data.europa.eu/p27/eforms-ubl-extensions/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    # Ältere TED-Formate (TED-XML / F-Formulare)
    "ted": "http://publications.europa.eu/resource/schema/ted/R2.0.9/publication",
    "n2021": "http://publications.europa.eu/resource/schema/ted/2021/nuts",
}

TED_XML_URL = "https://ted.europa.eu/en/notice/{nd}/xml"
REQUEST_TIMEOUT = 30
PAUSE_BETWEEN_REQUESTS = 0.5   # Sekunden zwischen XML-Abrufen


# ── HTTP-Abruf ────────────────────────────────────────────────────────────────

def fetch_xml(nd: str, max_retries: int = 3) -> Optional[ET.Element]:
    """
    Lädt das XML einer TED-Notice und gibt das Root-Element zurück.
    nd = Notice-ID, z. B. "123456-2024"
    Gibt None zurück bei Fehler.
    Enthält Retry-Logik für 429 (Rate-Limit) und 503 (Server überlastet).
    """
    url = TED_XML_URL.format(nd=nd)
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT,
                             headers={"Accept": "application/xml, text/xml, */*",
                                      "User-Agent": "TED-API-Scraper/2.0 (research)"})
            if r.status_code == 404:
                log.debug(f"XML nicht gefunden: {nd}")
                return None
            if r.status_code == 429:
                wait = 30 * attempt
                log.warning(f"Rate-Limit (429) bei {nd}, warte {wait}s (Versuch {attempt}/{max_retries})")
                time.sleep(wait)
                continue
            if r.status_code in (503, 502):
                wait = 15 * attempt
                log.warning(f"Server nicht verfügbar ({r.status_code}) bei {nd}, warte {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            content = r.content
            if not content or not content.strip():
                log.debug(f"Leere XML-Antwort: {nd}")
                return None
            # HTML-Fehlerseite erkennen (TED gibt manchmal HTML statt XML)
            if content.lstrip()[:5].lower() in (b"<!doc", b"<html"):
                log.debug(f"HTML statt XML erhalten für {nd} (Notice evtl. nicht öffentlich)")
                return None
            root = ET.fromstring(content)
            return root
        except ET.ParseError as e:
            log.warning(f"XML-Parse-Fehler {nd}: {e}")
            return None
        except requests.exceptions.Timeout:
            wait = 10 * attempt
            log.warning(f"Timeout bei {nd} (Versuch {attempt}/{max_retries}), warte {wait}s")
            if attempt < max_retries:
                time.sleep(wait)
        except requests.RequestException as e:
            log.warning(f"HTTP-Fehler beim XML-Abruf {nd}: {e}")
            return None
    log.warning(f"XML-Abruf für {nd} nach {max_retries} Versuchen aufgegeben")
    return None


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _text(elem, *path, ns=NS) -> str:
    """Traversiert einen XPath-Pfad und gibt den Text-Inhalt zurück."""
    current = elem
    for tag in path:
        if current is None:
            return ""
        found = current.find(tag, ns)
        current = found
    if current is None or current.text is None:
        return ""
    return current.text.strip()


def _all(elem, *path, ns=NS):
    """Gibt alle Elemente des letzten Tags im Pfad zurück."""
    current = elem
    for tag in path[:-1]:
        if current is None:
            return []
        current = current.find(tag, ns)
    if current is None:
        return []
    return current.findall(path[-1], ns)


def _attr(elem, *path, attr: str, ns=NS) -> str:
    """Gibt ein Attribut eines Elements zurück."""
    current = elem
    for tag in path:
        if current is None:
            return ""
        current = current.find(tag, ns)
    if current is None:
        return ""
    return current.get(attr, "")


def _float(text: str) -> Optional[float]:
    """Parst einen Float-String sicher."""
    try:
        return float(text.replace(",", ".").strip())
    except (ValueError, AttributeError):
        return None


def _parse_date(text: str) -> Optional[date]:
    """Parst ein ISO-Datum."""
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


# ── Beschreibungstext ─────────────────────────────────────────────────────────

def _extract_description(root: ET.Element) -> str:
    """
    Extrahiert den Hauptbeschreibungstext.
    Sucht in mehreren möglichen Pfaden (eForms und alte TED-XML-Formulare).
    """
    candidates = []

    # eForms (neueres Format): ProcurementProject > Description
    desc = _text(root, "cac:ProcurementProject", "cbc:Description")
    if desc:
        candidates.append(desc)

    # eForms: ContractFolderID-Beschreibung auf oberster Ebene
    desc2 = _text(root, "cbc:Description")
    if desc2 and desc2 != desc:
        candidates.append(desc2)

    # eForms: BusinessParty > BusinessDescription (manchmal)
    # Suche in allen cbc:Description überall
    if not candidates:
        for el in root.iter("{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}Description"):
            t = (el.text or "").strip()
            if t and len(t) > 50:
                candidates.append(t)
                break

    # Altes TED-XML-Format: OBJECT_CONTRACT > SHORT_DESCR
    if not candidates:
        for tag in ["SHORT_DESCR", "DESCRIPTION", "OBJECT_DESCR"]:
            for el in root.iter(tag):
                t = (el.text or "").strip()
                if t:
                    candidates.append(t)
                    break

    return "\n\n".join(candidates[:3])


# ── Lose ──────────────────────────────────────────────────────────────────────

def _extract_lots(root: ET.Element) -> list[dict]:
    """
    Extrahiert alle Lose aus einem eForms-XML.
    Gibt eine Liste von Dicts zurück:
      {"lot_number", "title", "description", "cpv_codes", "estimated_value", "currency", "deadline"}
    """
    lots = []

    # eForms: ProcurementProjectLot Elemente
    for lot_el in root.findall(".//cac:ProcurementProjectLot", NS):
        lot_id_text = _text(lot_el, "cbc:ID")
        lot_num = None
        if lot_id_text:
            try:
                lot_num = int(lot_id_text)
            except ValueError:
                lot_num = None

        title = _text(lot_el, "cac:ProcurementProject", "cbc:Name")
        description = _text(lot_el, "cac:ProcurementProject", "cbc:Description")

        # CPV
        cpv_codes = []
        main_cpv = _text(lot_el,
                         "cac:ProcurementProject",
                         "cac:MainCommodityClassification",
                         "cbc:ItemClassificationCode")
        if main_cpv:
            cpv_codes.append(main_cpv)
        for add_cpv_el in lot_el.findall(".//cac:AdditionalCommodityClassification/cbc:ItemClassificationCode", NS):
            code = (add_cpv_el.text or "").strip()
            if code and code not in cpv_codes:
                cpv_codes.append(code)

        # Geschätzter Wert
        # eForms: cac:ProcurementProject/cac:RequestedTenderTotal/cbc:EstimatedOverallContractAmount
        val_text = _text(lot_el,
                         "cac:ProcurementProject",
                         "cac:RequestedTenderTotal",
                         "cbc:EstimatedOverallContractAmount")
        if not val_text:
            val_text = _text(lot_el,
                             "cac:ProcurementProject",
                             "cac:RequestedTenderValue",
                             "cbc:MaximumAmount")
        if not val_text:
            val_text = _text(lot_el,
                             "cac:ProcurementProject",
                             "cbc:EstimatedOverallContractAmount")
        # Currency from whichever element has the value
        currency = (
            _attr(lot_el, "cac:ProcurementProject", "cac:RequestedTenderTotal",
                  "cbc:EstimatedOverallContractAmount", attr="currencyID") or
            _attr(lot_el, "cac:ProcurementProject",
                  "cbc:EstimatedOverallContractAmount", attr="currencyID")
        )

        # Frist
        deadline_text = _text(lot_el,
                               "cac:TenderingProcess",
                               "cac:TenderSubmissionDeadlinePeriod",
                               "cbc:EndDate")

        # NUTS-Code
        _nuts_el = (
            lot_el.find("cac:RealizedLocation/cbc:ID", NS) or
            lot_el.find("cac:DeliveryTerms/cac:DeliveryLocation/cbc:ID", NS) or
            lot_el.find(".//cbc:CountrySubentityCode", NS)
        )
        lot_nuts = _nuts_el.text.strip() if _nuts_el is not None and _nuts_el.text else ""

        lots.append({
            "lot_number":       lot_num,
            "title":            title[:500] if title else None,
            "description":      description[:5000] if description else None,
            "cpv_codes":        cpv_codes,
            "estimated_value":  _float(val_text),
            "currency":         currency or "EUR",
            "deadline":         _parse_date(deadline_text),
            "nuts_code":        lot_nuts,
        })

    return lots


# ── Award-Informationen ───────────────────────────────────────────────────────

def _extract_awards(root: ET.Element) -> list[dict]:
    """
    Extrahiert Vergabe-/Zuschlagsinformationen aus eForms XML.

    eForms-Referenzkette:
      LotResult → LotTender(rank=1) → TenderingParty → Tenderer(ORG-x)
      Organizations/Organization(ORG-x) → Company/PartyName → Name

    Gibt Liste von Dicts zurück:
      {lot_id_text, supplier_name, supplier_country,
       contract_value, currency, offers_received, award_date}
    """
    EXT = "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1"
    EBC = "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1"
    CAC = NS["cac"]
    CBC = NS["cbc"]

    def T(ns, local):
        return "{%s}%s" % (ns, local)

    def first_text(el, *tag_pairs):
        """Sucht den Text des ersten gefundenen Tags (ns, local)."""
        for ns, local in tag_pairs:
            found = el.find(".//%s" % T(ns, local))
            if found is not None and found.text:
                return found.text.strip()
        return None

    awards = []

    # ── 1. Organization-Map aufbauen: org_id → {name, country} ──────────────
    org_map = {}
    for org_el in root.iter(T(EXT, "Organization")):
        # ID: efac:Company/cac:PartyIdentification/cbc:ID  (= "ORG-0001")
        company_el = org_el.find(T(EXT, "Company"))
        if company_el is None:
            continue
        party_id_el = company_el.find(
            "%s/%s" % (T(CAC, "PartyIdentification"), T(CBC, "ID"))
        )
        org_id = party_id_el.text.strip() if party_id_el is not None and party_id_el.text else None
        if not org_id:
            continue
        # Name: Company/cac:PartyName/cbc:Name
        name_el = company_el.find(
            "%s/%s" % (T(CAC, "PartyName"), T(CBC, "Name"))
        )
        name = name_el.text.strip() if name_el is not None and name_el.text else None
        # Country: PostalAddress/Country/cbc:IdentificationCode
        country_el = company_el.find(
            ".//%s" % T(CBC, "IdentificationCode")
        )
        country = country_el.text.strip() if country_el is not None and country_el.text else None
        if org_id not in org_map:
            org_map[org_id] = {"name": name, "country": country}

    # ── 2. TenderingParty-Map: tp_id → [org_id, ...] ────────────────────────
    tp_map = {}
    for tp_el in root.iter(T(EXT, "TenderingParty")):
        tp_id_el = tp_el.find(T(CBC, "ID"))
        tp_id = tp_id_el.text.strip() if tp_id_el is not None and tp_id_el.text else None
        if not tp_id:
            continue
        tenderers = []
        for tenderer_el in tp_el.findall(T(EXT, "Tenderer")):
            org_id_el = tenderer_el.find(T(CBC, "ID"))
            if org_id_el is not None and org_id_el.text:
                tenderers.append(org_id_el.text.strip())
        # Prefer the entry that has Tenderer references (not just the reference stub)
        if tp_id not in tp_map or (tenderers and not tp_map[tp_id]):
            tp_map[tp_id] = tenderers

    # ── 3. LotTender-Map: lt_id → {payable, currency, tp_id, rank} ──────────
    lt_map = {}
    for lt_el in root.iter(T(EXT, "LotTender")):
        lt_id_el = lt_el.find(T(CBC, "ID"))
        lt_id = lt_id_el.text.strip() if lt_id_el is not None and lt_id_el.text else None
        if not lt_id:
            continue
        rank_el = lt_el.find(T(CBC, "RankCode"))
        rank = rank_el.text.strip() if rank_el is not None and rank_el.text else None

        # Vertragswert: cac:LegalMonetaryTotal/cbc:PayableAmount
        payable_el = lt_el.find(".//%s" % T(CBC, "PayableAmount"))
        payable = _float(payable_el.text) if payable_el is not None and payable_el.text else None
        currency = payable_el.get("currencyID") if payable_el is not None else None

        # TenderingParty-Referenz
        tp_ref_el = lt_el.find(".//%s/%s" % (T(EXT, "TenderingParty"), T(CBC, "ID")))
        tp_id = tp_ref_el.text.strip() if tp_ref_el is not None and tp_ref_el.text else None

        # Nur den Eintrag mit Rang 1 (Gewinner) speichern, oder ersten falls kein Rang
        if lt_id not in lt_map or rank == "1":
            lt_map[lt_id] = {
                "payable":  payable,
                "currency": currency,
                "tp_id":    tp_id,
                "rank":     rank,
            }

    # ── 4. LotResult verarbeiten ─────────────────────────────────────────────
    for lr_el in root.iter(T(EXT, "LotResult")):
        # Welches Los?
        lot_ref = first_text(lr_el, (EXT, "TenderLot"), (CBC, "ID")) or                   first_text(lr_el, (CBC, "ID"))

        # Welches LotTender wurde vergeben?
        awarded_lt_id = None
        for lt_ref_el in lr_el.findall(".//%s" % T(EXT, "LotTender")):
            lt_id_el = lt_ref_el.find(T(CBC, "ID"))
            if lt_id_el is not None and lt_id_el.text:
                awarded_lt_id = lt_id_el.text.strip()
                break

        # Anzahl Angebote
        offers_el = lr_el.find(".//%s" % T(EBC, "StatisticsNumeric"))
        offers = None
        if offers_el is not None and offers_el.text:
            try:
                offers = int(float(offers_el.text))
            except (ValueError, TypeError):
                pass

        # Award-Datum (SettledContract/IssueDate)
        award_date_text = first_text(lr_el, (EXT, "SettledContract"), (CBC, "IssueDate")) or                           first_text(lr_el, (CBC, "AwardDate"))

        # Gewinner über LotTender → TenderingParty → Organization
        supplier_name    = None
        supplier_country = None
        contract_value   = None
        currency         = "EUR"

        if awarded_lt_id and awarded_lt_id in lt_map:
            lt_info = lt_map[awarded_lt_id]
            contract_value = lt_info["payable"]
            currency       = lt_info["currency"] or "EUR"
            tp_id          = lt_info["tp_id"]

            if tp_id and tp_id in tp_map:
                org_ids = tp_map[tp_id]
                for org_id in org_ids:
                    if org_id in org_map and org_map[org_id]["name"]:
                        supplier_name    = org_map[org_id]["name"]
                        supplier_country = org_map[org_id]["country"]
                        break

        # Fallback: SettledContract/PayableAmount für Wert
        if contract_value is None:
            payable_el = lr_el.find(".//%s" % T(CBC, "PayableAmount"))
            if payable_el is not None and payable_el.text:
                contract_value = _float(payable_el.text)
                currency = payable_el.get("currencyID", "EUR")

        if supplier_name or contract_value is not None:
            awards.append({
                "lot_id_text":     lot_ref,
                "supplier_name":   supplier_name[:500] if supplier_name else None,
                "supplier_country": (supplier_country or "")[:3] or None,
                "contract_value":  contract_value,
                "currency":        currency,
                "offers_received": offers,
                "award_date":      _parse_date(award_date_text),
            })

    # ── Fallback: altes TED-XML-Format (F03, cac:AwardedTenderedProject) ─────
    if not awards:
        for atp in root.findall(".//cac:AwardedTenderedProject", NS):
            val_text = _text(atp, "cbc:TotalAmount") or _text(atp, "cac:LegalMonetaryTotal", "cbc:PayableAmount")
            currency_el = atp.find(".//{%s}TotalAmount" % NS["cbc"])
            currency = currency_el.get("currencyID", "EUR") if currency_el is not None else "EUR"
            for party in atp.findall(".//cac:WinningParty", NS):
                name_el = party.find("cac:PartyName/cbc:Name", NS)
                country_el = party.find("cac:PostalAddress/cac:Country/cbc:IdentificationCode", NS)
                if name_el is not None and name_el.text:
                    awards.append({
                        "lot_id_text":     None,
                        "supplier_name":   name_el.text.strip()[:500],
                        "supplier_country": (country_el.text.strip()[:3] if country_el is not None and country_el.text else None),
                        "contract_value":  _float(val_text),
                        "currency":        currency,
                        "offers_received": None,
                        "award_date":      None,
                    })

    return awards


def _extract_prior_notice_ref(root: ET.Element) -> Optional[str]:
    """
    Extrahiert die ND-Nummer der zugehörigen Original-Ausschreibung aus einer
    Award Notice. TED eForms enthält diese Referenz an mehreren möglichen Stellen.

    Gibt die ND-Nummer zurück (z.B. "123456-2025") oder None.
    """
    # eForms: TenderingProcess > NoticeDocumentReference > ID
    for ref_el in root.findall(".//cac:TenderingProcess/cac:NoticeDocumentReference", NS):
        nd_text = _text(ref_el, "cbc:ID")
        if nd_text and "-20" in nd_text:
            return nd_text.strip()

    # eForms: ContractFolderID manchmal als Referenz
    folder_id = _text(root, "cbc:ContractFolderID")
    if folder_id and "-20" in folder_id:
        return folder_id.strip()

    # Altes Format: PREVIOUS_PUBLICATION_NOTICE_F3 / CONTRACT_NOTICE
    for tag in ["PREVIOUS_PUBLICATION_NOTICE_F3", "CONTRACT_NOTICE_REF"]:
        for el in root.iter(tag):
            ref = el.get("REF_NO") or (el.text or "").strip()
            if ref:
                return ref

    # efac:NoticeResult > efac:TenderLotIdentification (manchmal Referenz)
    for ref_el in root.findall(".//efac:NoticeResult/cbc:NoticeTypeCode", NS):
        pass  # Nur Typ, keine ND-Nummer

    return None


# ── Gesamtaufruf ─────────────────────────────────────────────────────────────

def enrich_tender_from_xml(nd: str) -> Optional[dict]:
    """
    Holt und parst das XML einer TED-Notice.
    Gibt ein Dict zurück:
      {
        "description": str,
        "lots": [{"lot_number", "title", "description", "cpv_codes",
                   "estimated_value", "currency", "deadline"}, ...],
        "awards": [{"lot_id_text", "supplier_name", "supplier_country",
                    "contract_value", "currency", "offers_received", "award_date"}, ...],
      }
    Gibt None zurück wenn das XML nicht abgerufen werden konnte.
    """
    root = fetch_xml(nd)
    if root is None:
        return None

    return {
        "description":      _extract_description(root),
        "lots":             _extract_lots(root),
        "awards":           _extract_awards(root),
        "prior_notice_ref": _extract_prior_notice_ref(root),
    }


# ── DB-Integration ────────────────────────────────────────────────────────────

def enrich_and_save(db, tender_id: str, force: bool = False) -> bool:
    """
    Reichert einen bestehenden Tender in der DB mit XML-Daten an.
    Überspringt Notices, die schon eine Beschreibung haben (es sei denn force=True).

    db      = SQLAlchemy-Session
    tender_id = ND-Nummer (Primary Key in tenders-Tabelle)
    force   = auch schon angereicherte Notices neu laden

    Gibt True zurück bei Erfolg, False bei Fehler/Skip.
    """
    from database import Award, Lot, Supplier, Tender

    tender = db.get(Tender, tender_id)
    if tender is None:
        log.warning(f"Tender {tender_id} nicht in DB gefunden")
        return False

    # Skip wenn bereits angereichert (description vorhanden)
    if not force and tender.description:
        log.debug(f"Skip {tender_id}: schon angereichert")
        return True

    log.info(f"XML anreichern: {tender_id}")
    data = enrich_tender_from_xml(tender_id)
    if data is None:
        return False

    # 1. Beschreibung speichern
    if data["description"]:
        tender.description = data["description"][:10000]

    # 1b. Bei Award Notices: Verknüpfung zur Original-Ausschreibung setzen
    prior_ref = data.get("prior_notice_ref")
    if prior_ref and prior_ref != tender_id:
        # Prüfen ob die referenzierte Original-Notice in der DB ist
        from database import Tender as _Tender
        original = db.get(_Tender, prior_ref)
        if original:
            # Award-Referenz in der Original-Notice eintragen
            if not original.award_notice_id:
                original.award_notice_id = tender_id
                log.info(f"  Verknüpft: {prior_ref} → Award {tender_id}")
        else:
            log.debug(f"  Original-Notice {prior_ref} noch nicht in DB")

    # 2. Lose speichern – erst alle bestehenden löschen, dann frisch einfügen
    #    (verhindert Duplikate bei Re-Enrichment, da lot_number oft None ist)
    import json as _json
    if data["lots"]:
        db.query(Lot).filter(Lot.tender_id == tender_id).delete(synchronize_session=False)
        db.flush()

    for lot_data in data["lots"]:
        cpv_json = _json.dumps(lot_data["cpv_codes"]) if lot_data["cpv_codes"] else None

        if False:  # dead branch – kept for indentation alignment
            pass
        else:
            new_lot = Lot(
                tender_id=tender_id,
                lot_number=lot_data["lot_number"],
                title=lot_data["title"],
                description=lot_data["description"],
                cpv_codes=cpv_json,
                estimated_value=lot_data["estimated_value"],
                estimated_currency=lot_data["currency"],
                deadline_date=lot_data["deadline"],
            )
            db.add(new_lot)
            db.flush()

    # Primären NUTS-Code auf Tender schreiben
    if data.get("lots"):
        first_nuts = next(
            (l.get("nuts_code", "") for l in data["lots"] if l.get("nuts_code")),
            ""
        )
        if first_nuts and not tender.nuts_code:
            tender.nuts_code = first_nuts
            try:
                from enrichment import enrich_nuts
                nuts_info = enrich_nuts(first_nuts)
                tender.nuts_label = nuts_info.get("region", "")
                if not tender.country_label:
                    tender.country_label = nuts_info.get("country", "")
            except Exception:
                pass

    # 3. Awards (Zuschläge) speichern
    for award_data in data["awards"]:
        if not award_data["supplier_name"] and award_data["contract_value"] is None:
            continue

        # Supplier anlegen oder finden
        supplier_id = None
        if award_data["supplier_name"]:
            existing_sup = (
                db.query(Supplier)
                .filter(Supplier.name == award_data["supplier_name"])
                .first()
            )
            if existing_sup:
                supplier_id = existing_sup.id
            else:
                sup = Supplier(
                    name=award_data["supplier_name"],
                    country_code=award_data["supplier_country"],
                )
                db.add(sup)
                db.flush()
                supplier_id = sup.id

        # Lot-ID auflösen (lot_id_text → DB lot_id)
        lot_db_id = None
        if award_data["lot_id_text"] is not None:
            try:
                lot_num = int(award_data["lot_id_text"])
                lot_row = (
                    db.query(Lot)
                    .filter(Lot.tender_id == tender_id, Lot.lot_number == lot_num)
                    .first()
                )
                if lot_row:
                    lot_db_id = lot_row.id
            except (ValueError, TypeError):
                pass

        # Prüfen ob Award für diesen Tender + Supplier schon existiert
        existing_award = (
            db.query(Award)
            .filter(Award.tender_id == tender_id, Award.supplier_id == supplier_id)
            .first()
        )
        if not existing_award:
            new_award = Award(
                tender_id=tender_id,
                lot_id=lot_db_id,
                supplier_id=supplier_id,
                award_date=award_data["award_date"],
                contract_value=award_data["contract_value"],
                contract_currency=award_data["currency"],
                offers_received=award_data["offers_received"],
            )
            db.add(new_award)
        else:
            # Fehlende Felder ergänzen
            if existing_award.contract_value is None and award_data["contract_value"]:
                existing_award.contract_value = award_data["contract_value"]
            if existing_award.offers_received is None and award_data["offers_received"]:
                existing_award.offers_received = award_data["offers_received"]
            if existing_award.award_date is None and award_data["award_date"]:
                existing_award.award_date = award_data["award_date"]
            if existing_award.lot_id is None and lot_db_id:
                existing_award.lot_id = lot_db_id

    db.commit()
    log.info(f"  ✓ {tender_id}: {len(data['lots'])} Lose, {len(data['awards'])} Zuschläge, "
             f"Beschreibung: {'ja' if data['description'] else 'nein'}")
    return True


# ── Batch-Anreicherung ────────────────────────────────────────────────────────

def batch_enrich(limit: int = 100, days_back: int = 30, country: str = None,
                 force: bool = False, pause: float = PAUSE_BETWEEN_REQUESTS):
    """
    Reichert alle noch nicht angereicherten Tender in der DB mit XML-Daten an.
    Kann direkt als Skript aufgerufen werden.

    limit     = max. Anzahl Notices pro Durchlauf
    days_back = nur Notices aus den letzten N Tagen
    country   = Länderfilter (z.B. "DEU"), None = alle
    force     = auch schon angereicherte Notices neu laden
    pause     = Pause zwischen XML-Abrufen in Sekunden
    """
    from datetime import datetime, timedelta
    from database import SessionLocal, Tender

    cutoff = datetime.utcnow().date() - timedelta(days=days_back)

    db = SessionLocal()
    try:
        query = db.query(Tender).filter(Tender.published_date >= cutoff)
        if not force:
            query = query.filter(Tender.description.is_(None))
        if country:
            query = query.filter(Tender.country_code == country)
        query = query.order_by(Tender.published_date.desc()).limit(limit)
        tenders = query.all()

        log.info(f"Batch-Anreicherung: {len(tenders)} Notices (limit={limit}, days={days_back})")

        success = 0
        failed = 0
        linked = 0
        tender_ids = [t.id for t in tenders]
        db.close()  # close main session before threading

        def _enrich_one(tid):
            from database import SessionLocal as _SL
            _db = _SL()
            try:
                ok = enrich_and_save(_db, tid, force=force)
                _linked = 0
                if ok:
                    from database import Tender as _T
                    r = _db.get(_T, tid)
                    if r and r.award_notice_id:
                        _linked = 1
                return ok, _linked
            finally:
                _db.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(_enrich_one, tid): tid for tid in tender_ids}
            for fut in as_completed(futures):
                try:
                    ok, lnk = fut.result()
                    if ok:
                        success += 1
                        linked += lnk
                    else:
                        failed += 1
                except Exception as exc:
                    log.warning(f"Worker error for {futures[fut]}: {exc}")
                    failed += 1
                time.sleep(pause / 4)

        log.info(f"Batch fertig: {success} erfolgreich, {failed} fehlgeschlagen, {linked} neu verknüpft")
    except Exception:
        raise
    finally:
        pass  # sessions closed inside workers


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [XML] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="TED XML-Anreicherung")
    parser.add_argument("--limit",  type=int, default=50,
                        help="Max. Anzahl Notices pro Durchlauf (default: 50)")
    parser.add_argument("--days",   type=int, default=14,
                        help="Nur Notices der letzten N Tage (default: 14)")
    parser.add_argument("--land",   type=str, default=None,
                        help="Länderfilter, z.B. DEU (default: alle)")
    parser.add_argument("--force",  action="store_true",
                        help="Auch schon angereicherte Notices neu laden")
    parser.add_argument("--pause",  type=float, default=PAUSE_BETWEEN_REQUESTS,
                        help=f"Pause zwischen Requests in Sek. (default: {PAUSE_BETWEEN_REQUESTS})")
    parser.add_argument("--nd",     type=str, default=None,
                        help="Einzelne Notice-ID testen, z.B. 123456-2024")
    args = parser.parse_args()

    if args.nd:
        # Einzelne Notice testen
        result = enrich_tender_from_xml(args.nd)
        if result:
            import json
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        else:
            print(f"Kein XML für {args.nd} gefunden.")
    else:
        batch_enrich(
            limit=args.limit,
            days_back=args.days,
            country=args.land,
            force=args.force,
            pause=args.pause,
        )
