"""
TED OCDS-Scraper – alle EU-Länder, IT-Ausschreibungen
=======================================================
Scrapet sowohl Contract Notices (TD=3) als auch Award Notices (TD=7).
Verknüpft Zuschläge mit den zugehörigen Ausschreibungen.
Reichert CPV- und NUTS-Codes mit Labels an.

Ausführen:
  python scraper.py              # letzte 3 Tage (inkrementell)
  python scraper.py --days 30    # initiale Befüllung
  python scraper.py --days 90    # historische Daten
  python scraper.py --days 7 --land DEU   # nur Deutschland
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

try:
    import requests
except ImportError:
    sys.exit("pip install requests sqlalchemy")

from database import (
    Award, Buyer, Lot, RateLimit, SessionLocal, Supplier, Tender, init_db
)
from enrichment import enrich_cpv, enrich_nuts

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCRAPER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Konfiguration ─────────────────────────────────────────────────────────────
BASE_URL       = "https://api.ted.europa.eu/v3/notices/search"
CPV_PRAEFIX    = ["72", "48", "30"]
SEITEN_GROESSE = 50
PAUSE_SEK      = 0.4
MAX_VERSUCHE   = 3
FELDER         = ["ND", "PD", "TD", "FT", "TI", "AU", "RP", "PC", "DD", "CY", "NC", "PR", "AC", "OJ",
                  "publication-number", "publication-date", "notice-type", "form-type",
                  "buyer-name", "buyer-country", "buyer-city", "buyer-post-code",
                  "procedure-type", "deadline", "dispatch-date",
                  "title-lot", "description-lot", "estimated-value-lot",
                  "classification-cpv", "AA", "BI", "links"]

# Bezeichnungen für numerische Codes
TD_MAP = {
    # Numerisch (altes TED-Format)
    "1":  "Prior Information Notice",
    "2":  "Contract Notice",
    "3":  "Contract Notice",
    "7":  "Contract Award Notice",
    "13": "Corrigendum",
    "20": "Concession Notice",
    "21": "Concession Award",
    # F-Formulare
    "F01": "Prior Information Notice",
    "F02": "Contract Notice",
    "F03": "Contract Award Notice",
    "F20": "Modification Notice",
    # Textuell (ältere API-Versionen)
    "CONTRACT_NOTICE":          "Contract Notice",
    "ContractNotice":           "Contract Notice",
    "contract_notice":          "Contract Notice",
    "CONTRACT_AWARD_NOTICE":    "Contract Award Notice",
    "ContractAwardNotice":      "Contract Award Notice",
    "contract_award_notice":    "Contract Award Notice",
    "PRIOR_INFORMATION_NOTICE": "Prior Information Notice",
    # eForms FT-Feldwerte (neue TED-API)
    "cn":           "Contract Notice",
    "cn-desg":      "Contract Notice (Design)",
    "cn-social":    "Contract Notice (Social)",
    "can":          "Contract Award Notice",
    "can-social":   "Contract Award Notice (Social)",
    "can-desg":     "Contract Award Notice (Design)",
    "pin":          "Prior Information Notice",
    "pin-buyer":    "Prior Information Notice",
    "pin-cfc":      "Prior Information Notice (CFC)",
    "veat":         "Voluntary ex-ante Transparency Notice",
    "corr":         "Corrigendum",
}

NC_MAP = {
    "1": "Works", "2": "Supplies", "4": "Services",
    "5": "Mixed", "6": "Works (Concession)", "7": "Services (Concession)",
}

PR_MAP = {
    "1": "Open procedure", "2": "Restricted procedure",
    "3": "Negotiated procedure (with prior notice)",
    "4": "Negotiated procedure (without prior notice)",
    "6": "Competitive dialogue", "8": "Innovation partnership",
}

AC_MAP = {
    "1": "Lowest price",
    "2": "Most economically advantageous tender (MEAT)",
    "3": "Price",
}


# ── Text-Hilfsfunktionen ──────────────────────────────────────────────────────

def t(w, prio=("deu", "eng", "de", "en")):
    """Extrahiert Text aus mehrsprachigen TED-API-Feldern."""
    if w is None:
        return ""
    if isinstance(w, str):
        return w.strip()
    if isinstance(w, list):
        return "; ".join(x for x in (t(v, prio) for v in w) if x)
    if isinstance(w, dict):
        for lang in prio:
            v = w.get(lang)
            if v:
                return ("; ".join(str(x) for x in v) if isinstance(v, list) else str(v)).strip()
        for v in w.values():
            if v:
                return ("; ".join(str(x) for x in v) if isinstance(v, list) else str(v)).strip()
    return str(w).strip()


def einzel(w):
    """Gibt den ersten Wert zurück wenn Liste mit einem Element."""
    return w[0] if isinstance(w, list) and len(w) == 1 else w


def parse_date(w) -> date | None:
    """Parst TED-Datumsformate zu Python date."""
    s = t(w[0] if isinstance(w, list) and w else w)
    s = s.split("T")[0].split("+")[0].split("Z")[0].strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s[:len(fmt.replace("%Y", "0000").replace("%m", "00").replace("%d", "00"))], fmt).date()
        except ValueError:
            pass
    return None


# ── API-Kommunikation ─────────────────────────────────────────────────────────

def api_post(payload: dict, versuch: int = 1):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        r = requests.post(BASE_URL, headers=headers, json=payload, timeout=45)
        r.raise_for_status()
        return r.json()
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        if versuch < MAX_VERSUCHE:
            wait = versuch * 6
            log.warning(f"Netzwerkfehler ({type(e).__name__}), warte {wait}s (Versuch {versuch}/{MAX_VERSUCHE})")
            time.sleep(wait)
            return api_post(payload, versuch + 1)
        log.error(f"Abbruch nach {MAX_VERSUCHE} Versuchen: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP {e.response.status_code}: {e.response.text[:300]}")
        return None


def fetch_all(query: str, doc_type_label: str = "") -> list[dict]:
    """Holt alle Seiten einer TED-Suchanfrage."""
    alle = []
    for seite in range(1, 10_000):
        data = api_post({
            "query":  query,
            "page":   seite,
            "limit":  SEITEN_GROESSE,
            "fields": FELDER,
        })
        if data is None:
            log.error("API-Fehler, Abbruch.")
            break
        notices = data.get("notices", [])
        alle.extend(notices)
        label = f" [{doc_type_label}]" if doc_type_label else ""
        log.info(f"  Seite {seite}{label}: +{len(notices)} (gesamt: {len(alle)})")
        if len(notices) < SEITEN_GROESSE:
            break
        time.sleep(PAUSE_SEK)
    return alle


# ── Datenmapping ──────────────────────────────────────────────────────────────

def map_notice(n: dict) -> dict:
    """Mappt ein TED-Notice-Objekt auf ein normalisiertes Dict."""
    nd    = str(einzel(n.get("ND", "")) or "").strip()
    # TD (alt) oder FT (eForms) — welches auch immer befüllt ist
    _td_raw = einzel(n.get("TD") or n.get("FT") or n.get("FORM") or "")
    td    = str(_td_raw or "").strip()
    pc    = n.get("PC", [])
    cy    = str(einzel(n.get("CY", "")) or "").strip()
    # RP = NUTS region (alt). eForms kann auch RPC oder PLACE nutzen.
    nuts_raw = n.get("RP") or n.get("RPC") or n.get("PLACE") or ""
    nuts  = str(einzel(nuts_raw) or "").strip()
    # Falls CY leer: aus NUTS ableiten (erste 2 Zeichen = ISO-Alpha2, z.B. "DE" → "DEU")
    # Oder aus Titel: "Deutschland –" → DEU
    if not cy and nuts and len(nuts) >= 2:
        _iso2 = nuts[:2].upper()
        _iso2_to_3 = {
            "DE": "DEU", "FR": "FRA", "PL": "POL", "IT": "ITA", "ES": "ESP",
            "NL": "NLD", "BE": "BEL", "AT": "AUT", "SE": "SWE", "DK": "DNK",
            "FI": "FIN", "PT": "PRT", "CZ": "CZE", "SK": "SVK", "HU": "HUN",
            "RO": "ROU", "BG": "BGR", "HR": "HRV", "SI": "SVN", "EE": "EST",
            "LV": "LVA", "LT": "LTU", "LU": "LUX", "MT": "MLT", "CY": "CYP",
            "IE": "IRL", "GR": "GRC", "NO": "NOR", "CH": "CHE", "IS": "ISL",
        }
        cy = _iso2_to_3.get(_iso2, "")
    # NC/PR/AC können als Liste kommen — ersten Wert nehmen
    _nc_raw = n.get("NC", "")
    nc = str((_nc_raw[0] if isinstance(_nc_raw, list) and _nc_raw else _nc_raw) or "").strip()
    _pr_raw = n.get("PR", "")
    pr = str((_pr_raw[0] if isinstance(_pr_raw, list) and _pr_raw else _pr_raw) or "").strip()
    _ac_raw = n.get("AC", "")
    ac = str((_ac_raw[0] if isinstance(_ac_raw, list) and _ac_raw else _ac_raw) or "").strip()

    # eForms nutzt Text-Labels statt numerische Codes für NC/PR/AC
    # Mapping für eForms-Werte (proc-type, main-nature)
    EFORMS_NC = {
        "supplies": "2", "services": "4", "works": "1",
        "combined": "5",
    }
    EFORMS_PR = {
        "open": "1", "restricted": "2",
        "neg-w-call": "3", "neg-wo-call": "4",
        "comp-dial": "6", "innovation": "8",
        "oth-single": "4", "oth-mult": "3",
    }
    # Wenn NC ein eForms-Label ist, zur numerischen Form normalisieren
    if nc.lower() in EFORMS_NC:
        nc = EFORMS_NC[nc.lower()]
    if pr.lower() in EFORMS_PR:
        pr = EFORMS_PR[pr.lower()]

    # CPV anreichern
    cpv_list_raw = pc if isinstance(pc, list) else ([pc] if pc else [])
    # Deduplizieren (eForms liefert manchmal CPV-Codes mehrfach)
    seen = set()
    cpv_list = []
    for c in cpv_list_raw:
        cs = str(c).strip()
        if cs and cs not in seen:
            seen.add(cs)
            cpv_list.append(cs)
    cpv_main_code = cpv_list[0] if cpv_list else None
    cpv_info = enrich_cpv(str(cpv_main_code)) if cpv_main_code else {}

    # NUTS anreichern
    nuts_info = enrich_nuts(nuts) if nuts else {}

    return {
        "nd":               nd,
        "td":               td,
        "ted_url":          f"https://ted.europa.eu/udl?uri=TED:NOTICE:{nd}:TEXT:DE:HTML" if nd else "",
        "published_date":   parse_date(einzel(n.get("PD"))),
        "deadline_date":    parse_date(einzel(n.get("DD"))),
        "doc_type":         td,
        "doc_type_label":   TD_MAP.get(td, td),
        "title":            t(n.get("TI"))[:2000],
        "contracting_auth": t(n.get("AU"))[:500],
        # Geografisch
        "country_code":     cy,
        "nuts_code":        nuts,
        "nuts_label":       nuts_info.get("region", ""),
        "country_label":    nuts_info.get("country", ""),
        # CPV
        "cpv_codes":        json.dumps(cpv_list),
        "cpv_main":         str(cpv_main_code) if cpv_main_code else None,
        "cpv_main_label":   cpv_info.get("label", ""),
        "cpv_category":     cpv_info.get("division", ""),
        "cpv_category_label": cpv_info.get("category", ""),
        # Verfahren
        "contract_type":        nc,
        "contract_type_label":  NC_MAP.get(nc, nc),
        "procedure":            pr,
        "procedure_label":      PR_MAP.get(pr, pr),
        "award_criteria":       ac,
        "award_criteria_label": AC_MAP.get(ac, ac),
        # OJ-Referenz (für Award-Verknüpfung)
        "oj": t(n.get("OJ")),
    }


# ── Buyer-Lookup oder Anlegen ─────────────────────────────────────────────────

_buyer_cache: dict[str, int] = {}   # name → id (Session-Cache)

def get_or_create_buyer(db, name: str, nuts_code: str, country_code: str) -> int | None:
    if not name:
        return None
    key = name[:100]
    if key in _buyer_cache:
        return _buyer_cache[key]
    existing = db.query(Buyer).filter(Buyer.name == name[:500]).first()
    if existing:
        _buyer_cache[key] = existing.id
        return existing.id
    buyer = Buyer(name=name[:500], nuts_code=nuts_code, country_code=country_code)
    db.add(buyer)
    db.flush()
    _buyer_cache[key] = buyer.id
    return buyer.id


def get_or_create_supplier(db, name: str, country_code: str = None) -> int | None:
    if not name:
        return None
    existing = db.query(Supplier).filter(Supplier.name == name[:500]).first()
    if existing:
        return existing.id
    supplier = Supplier(name=name[:500], country_code=country_code)
    db.add(supplier)
    db.flush()
    return supplier.id


# ── Upsert-Logik ──────────────────────────────────────────────────────────────

def upsert_tender(db, mapped: dict) -> Tender:
    """Legt einen Tender an oder aktualisiert ihn."""
    nd = mapped["nd"]
    existing = db.get(Tender, nd)

    buyer_id = get_or_create_buyer(
        db,
        mapped["contracting_auth"],
        mapped["nuts_code"],
        mapped["country_code"],
    )

    fields = {
        "ted_url":           mapped["ted_url"],
        "doc_type":          mapped["doc_type"],
        "doc_type_label":    mapped["doc_type_label"],
        "published_date":    mapped["published_date"],
        "deadline_date":     mapped["deadline_date"],
        "title":             mapped["title"],
        "country_code":      mapped["country_code"],
        "nuts_code":         mapped["nuts_code"],
        "nuts_label":        mapped["nuts_label"],
        "country_label":     mapped["country_label"],
        "cpv_codes":         mapped["cpv_codes"],
        "cpv_main":          mapped["cpv_main"],
        "cpv_main_label":    mapped["cpv_main_label"],
        "cpv_category":      mapped["cpv_category"],
        "cpv_category_label":mapped["cpv_category_label"],
        "contract_type":     mapped["contract_type"],
        "contract_type_label":mapped["contract_type_label"],
        "procedure":         mapped["procedure"],
        "procedure_label":   mapped["procedure_label"],
        "award_criteria":    mapped["award_criteria"],
        "award_criteria_label":mapped["award_criteria_label"],
        "buyer_id":          buyer_id,
        "updated_at":        datetime.utcnow(),
    }

    if existing:
        for k, v in fields.items():
            setattr(existing, k, v)
        return existing
    else:
        tender = Tender(id=nd, scraped_at=datetime.utcnow(), **fields)
        db.add(tender)
        return tender


def upsert_award_notice(db, mapped: dict):
    """
    Verarbeitet einen Award Notice (can/TD=7).
    Speichert die Award Notice als eigenen Tender-Eintrag und versucht
    die Verknüpfung zur Original-Ausschreibung herzustellen.

    Verknüpfungs-Strategie (Priorität):
      1. XML-Referenz: NoticeDocumentReference im XML der Award Notice
         → direkter, zuverlässiger Verweis auf die Original-ND-Nummer
      2. OJ-Feld: aus der Search-API (manchmal vorhanden)
      3. Heuristik: gleicher Auftraggeber + innerhalb 365 Tage (Fallback)
    """
    nd   = mapped["nd"]
    auth = mapped["contracting_auth"]
    pub  = mapped["published_date"]

    # Award Notice als Tender-Eintrag speichern (für Vollständigkeit)
    existing = db.get(Tender, nd)
    if not existing:
        upsert_tender(db, mapped)
        db.flush()

    # ── Verknüpfung suchen ────────────────────────────────────────────────────
    linked_nd = None

    # 1. XML-Referenz (zuverlässigster Weg — wird nach dem Scrape per XML-Anreicherung gesetzt)
    #    Wir merken uns die Award-ND-Nummer im Tender-Eintrag, die XML-Anreicherung
    #    löst dann die Verknüpfung auf wenn sie das XML der Award Notice lädt.

    # 2. OJ-Feld aus Search-API prüfen (enthält manchmal "YYYYMMDD/S NNN" Format)
    oj = mapped.get("oj", "")
    if oj:
        # OJ-Ref kann Format haben: "2025/S 123-456789" → ND wäre "456789-2025"
        import re
        m = re.search(r'(\d{4})/S\s+\d+-(\d+)', str(oj))
        if m:
            year, num = m.group(1), m.group(2)
            candidate_nd = f"{num}-{year}"
            if db.get(Tender, candidate_nd):
                linked_nd = candidate_nd
                log.debug(f"Award {nd} → OJ-Ref {linked_nd}")

    # 3. Heuristik: gleicher Auftraggeber, innerhalb 365 Tage
    if not linked_nd and auth and pub:
        cutoff = pub - timedelta(days=365)
        cn_types = {"3", "F02", "cn", "cn-desg", "cn-social", "2"}
        candidates = (
            db.query(Tender)
            .join(Buyer, Tender.buyer_id == Buyer.id, isouter=True)
            .filter(
                Buyer.name == auth[:500],
                Tender.doc_type.in_(cn_types),
                Tender.published_date >= cutoff,
                Tender.published_date <= pub,
                Tender.id != nd,
            )
            .order_by(Tender.published_date.desc())
            .limit(1)
            .all()
        )
        if candidates:
            linked_nd = candidates[0].id
            log.debug(f"Award {nd} → Heuristik {linked_nd}")

    # ── Verknüpfung in DB schreiben ───────────────────────────────────────────
    if linked_nd:
        original = db.get(Tender, linked_nd)
        if original and not original.award_notice_id:
            original.award_notice_id = nd

        # Award-Datensatz anlegen (wird später durch XML-Anreicherung mit
        # Supplier, Wert etc. befüllt)
        award_exists = db.query(Award).filter(Award.award_notice_id == nd).first()
        if not award_exists:
            award = Award(
                tender_id       = linked_nd,
                award_notice_id = nd,
                published_date  = pub,
                award_date      = pub,
            )
            db.add(award)


# ── Haupt-Scraping-Logik ──────────────────────────────────────────────────────

def scrape(days: int, land: str = None):
    init_db()
    _buyer_cache.clear()
    db = SessionLocal()

    cpv_filter = " OR ".join(f"classification-cpv={p}*" for p in CPV_PRAEFIX)
    d_von = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    d_bis = datetime.now().strftime("%Y%m%d")

    land_filter = f" AND buyer-country={land.upper()}" if land else ""
    base_query  = f"({cpv_filter}){land_filter} AND publication-date>={d_von} AND publication-date<={d_bis}"

    log.info("=" * 60)
    log.info(f"TED OCDS-Scraper gestartet | letzte {days} Tage{' | ' + land if land else ''}")
    log.info("=" * 60)

    t0 = time.time()

    # Alle Notices auf einmal holen, dann nach TD aufteilen
    log.info("Scrape alle Notices (Contract + Award)...")
    alle_raw = fetch_all(base_query, "Notices")

    # Debug: zeige die ersten paar TD-Werte um das Format zu verstehen
    if alle_raw:
        sample_tds = [n.get("TD") for n in alle_raw[:5]]
        log.info(f"  Sample TD-Werte (erste 5): {sample_tds}")

    def td_value(n: dict) -> str:
        """
        Normalisiert den Notice-Typ auf einen vergleichbaren String.
        Prüft TD, FT und FORM (verschiedene API-Versionen liefern verschiedene Felder).
        """
        for field in ("TD", "FT", "FORM"):
            raw = n.get(field)
            if raw is None:
                continue
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            if raw:
                return str(raw).strip()
        return ""

    # Zuordnung: Alle bekannten Bezeichnungen für Contract Notice und Award Notice
    # TD (alt): "3"=CN, "7"=AN
    # FT (eForms): "cn" oder "can", "pin", "veat" etc.
    # FORM: "F02"=CN, "F03"=AN, "F01"=PIN
    CN_VALUES = {
        "3", "F02", "2", "CONTRACT_NOTICE", "ContractNotice", "contract_notice",
        "cn",        # eForms FT-Wert für Contract Notice
        "cn-desg",   # Design contest
        "cn-social", # Social services
        "pin",       # Prior Information Notice (auch oft mit Bewerbungsaufforderung)
        "F01", "1",
    }
    AN_VALUES = {
        "7", "F03", "CONTRACT_AWARD_NOTICE", "ContractAwardNotice", "contract_award_notice",
        "can",       # eForms FT-Wert für Contract Award Notice
        "can-social","can-desg",
        "F20", "20",
    }

    cn_raw   = [n for n in alle_raw if td_value(n) in CN_VALUES]
    an_raw   = [n for n in alle_raw if td_value(n) in AN_VALUES]
    sonstige = [n for n in alle_raw if n not in cn_raw and n not in an_raw]

    if sonstige:
        unique_td = {td_value(n) for n in sonstige}
        log.info(f"  Unbekannte FT/TD-Werte in Sonstige: {unique_td}")
        log.info(f"  Beispiel-Notice (Sonstige): {sonstige[0] if sonstige else 'keine'}")

    log.info(f"  Aufgeteilt: {len(cn_raw)} Contract Notices | {len(an_raw)} Award Notices | {len(sonstige)} Sonstige")

    # 1) Contract Notices speichern
    log.info("Speichere Contract Notices...")
    saved_cn = 0
    for n in cn_raw:
        if not n.get("ND"):
            continue
        mapped = map_notice(n)
        if not mapped["nd"]:
            continue
        upsert_tender(db, mapped)
        saved_cn += 1
        if saved_cn % 200 == 0:
            db.commit()
            log.info(f"  Zwischenspeicherung: {saved_cn} Contract Notices")
    # Sonstige Notices auch speichern (Vorinformationen, Korrekturen etc.)
    for n in sonstige:
        if not n.get("ND"):
            continue
        mapped = map_notice(n)
        if not mapped["nd"]:
            continue
        upsert_tender(db, mapped)
        saved_cn += 1
    db.commit()
    log.info(f"Contract + Sonstige Notices gespeichert: {saved_cn}")

    # 2) Award Notices verarbeiten
    log.info("Verarbeite Award Notices...")
    an_raw = an_raw  # bereits gefiltert
    saved_an = 0
    for n in an_raw:
        if not n.get("ND"):
            continue
        mapped = map_notice(n)
        if not mapped["nd"]:
            continue
        upsert_award_notice(db, mapped)
        saved_an += 1
        if saved_an % 100 == 0:
            db.commit()
            log.info(f"  Zwischenspeicherung: {saved_an} Award Notices")
    db.commit()

    # Statistik
    total_tenders = db.query(Tender).count()
    total_awards  = db.query(Award).count()
    total_buyers  = db.query(Buyer).count()

    log.info("=" * 60)
    log.info(f"Fertig in {time.time()-t0:.1f}s")
    log.info(f"  Contract Notices neu/aktualisiert: {saved_cn}")
    log.info(f"  Award Notices verarbeitet:         {saved_an}")
    log.info(f"  DB gesamt: {total_tenders} Tenders | {total_awards} Awards | {total_buyers} Buyers")
    log.info("=" * 60)

    db.close()
    return {"contract_notices": saved_cn, "award_notices": saved_an}


def scrape_and_enrich(days: int, land: str = None,
                      xml_limit: int = 200, xml_pause: float = 0.5):
    """
    Kombinierter Aufruf: Erst Scraper, dann XML-Anreicherung.
    xml_limit  = max. Anzahl Notices die per XML angereichert werden
    xml_pause  = Pause zwischen XML-Requests in Sekunden
    """
    result = scrape(days, land)

    log.info("")
    log.info("Starte XML-Anreicherung (vollständige Beschreibungen, Lose, Bieter)...")
    try:
        from xml_parser import batch_enrich
        batch_enrich(limit=xml_limit, days_back=days, country=land, pause=xml_pause)
    except Exception as e:
        log.warning(f"XML-Anreicherung fehlgeschlagen (nicht kritisch): {e}")

    return result


def scrape_historisch(tage_gesamt: int = 365, chunk_tage: int = 14,
                      land: str = None, xml_limit_pro_chunk: int = 100):
    """
    Historische Befüllung: scrapt rückwirkend in Chunks.
    Nutzt --resume-Logik: überspringt Zeitfenster die schon komplett sind.

    tage_gesamt          = wie weit zurück (z.B. 365 für 1 Jahr)
    chunk_tage           = Fenstergröße pro API-Aufruf (14 Tage empfohlen)
    land                 = Länderfilter
    xml_limit_pro_chunk  = XML-Anreicherungen pro Chunk
    """
    from datetime import date as ddate
    init_db()
    db = SessionLocal()

    heute = datetime.now().date()
    cpv_filter = " OR ".join(f"classification-cpv={p}*" for p in CPV_PRAEFIX)
    land_filter = f" AND buyer-country={land.upper()}" if land else ""

    log.info("=" * 60)
    log.info(f"Historischer Scrape: {tage_gesamt} Tage, Chunks à {chunk_tage} Tage")
    log.info("=" * 60)

    total_saved = 0
    chunk_num = 0

    # Chunks von aktuell → rückwärts
    for offset in range(0, tage_gesamt, chunk_tage):
        d_bis_dt  = heute - timedelta(days=offset)
        d_von_dt  = heute - timedelta(days=offset + chunk_tage - 1)
        d_von_str = d_von_dt.strftime("%Y%m%d")
        d_bis_str = d_bis_dt.strftime("%Y%m%d")

        # Resume-Check: gibt es schon Notices in diesem Zeitfenster?
        existing_count = (
            db.query(Tender)
            .filter(
                Tender.published_date >= d_von_dt,
                Tender.published_date <= d_bis_dt,
            )
            .count()
        )
        if existing_count > 5:  # Zeitfenster bereits befüllt → überspringen
            log.info(f"Chunk {d_von_str}–{d_bis_str}: {existing_count} bereits vorhanden, übersprungen")
            continue

        chunk_num += 1
        query = f"({cpv_filter}){land_filter} AND publication-date>={d_von_str} AND publication-date<={d_bis_str}"
        log.info(f"Chunk {chunk_num}: {d_von_str}–{d_bis_str}")

        _buyer_cache.clear()
        alle_raw = fetch_all(query, f"{d_von_str}–{d_bis_str}")
        if not alle_raw:
            continue

        CN_VALUES = {"3","F02","2","cn","cn-desg","cn-social","pin","F01","1",
                     "CONTRACT_NOTICE","ContractNotice","contract_notice"}
        AN_VALUES = {"7","F03","can","can-social","can-desg","F20","20",
                     "CONTRACT_AWARD_NOTICE","ContractAwardNotice","contract_award_notice"}

        def td_val(n):
            for f in ("TD","FT","FORM"):
                r = n.get(f)
                if r:
                    return str(r[0] if isinstance(r, list) else r).strip()
            return ""

        cn_raw   = [n for n in alle_raw if td_val(n) in CN_VALUES]
        an_raw   = [n for n in alle_raw if td_val(n) in AN_VALUES]
        sonstige = [n for n in alle_raw if n not in cn_raw and n not in an_raw]

        saved = 0
        for n in cn_raw + sonstige:
            if not n.get("ND"):
                continue
            mapped = map_notice(n)
            if mapped["nd"]:
                upsert_tender(db, mapped)
                saved += 1
        for n in an_raw:
            if not n.get("ND"):
                continue
            mapped = map_notice(n)
            if mapped["nd"]:
                upsert_award_notice(db, mapped)
                saved += 1
        db.commit()
        total_saved += saved
        log.info(f"  → {saved} gespeichert (CN={len(cn_raw)}, AN={len(an_raw)}, S={len(sonstige)})")

        # XML-Anreicherung für diesen Chunk
        try:
            from xml_parser import batch_enrich
            batch_enrich(limit=xml_limit_pro_chunk, days_back=offset + chunk_tage,
                         country=land, force=False)
        except Exception as e:
            log.warning(f"  XML-Anreicherung Fehler (nicht kritisch): {e}")

        time.sleep(1.0)  # Kurze Pause zwischen Chunks

    db.close()
    log.info("=" * 60)
    log.info(f"Historischer Scrape fertig: {total_saved} Notices in {chunk_num} Chunks")
    log.info("=" * 60)


def check_alerts(days_back: int = 1):
    """
    Prüft alle aktiven Alerts auf neue Treffer und sendet Webhook-Notifications.
    Wird täglich per Cron nach dem Scrape aufgerufen:
      python scraper.py --check-alerts

    Webhook-Payload (JSON POST):
      {
        "alert_id": 1,
        "alert_name": "IT Services Germany",
        "new_matches": 3,
        "tenders": [{"id": "...", "title": "...", "ted_url": "...", ...}]
      }
    """
    import json as _json
    try:
        import requests as _req
    except ImportError:
        log.warning("requests nicht installiert, Webhooks nicht moeglich")
        _req = None

    init_db()
    db = SessionLocal()

    try:
        from database import Alert, Tender, Lot
        from sqlalchemy import or_
        from sqlalchemy.orm import selectinload

        alerts = db.query(Alert).filter(Alert.active == 1).all()
        if not alerts:
            log.info("Keine aktiven Alerts gefunden.")
            return

        log.info(f"Pruefe {len(alerts)} aktive Alerts (letzte {days_back} Tage)...")
        cutoff = datetime.now().date() - timedelta(days=days_back)

        for alert in alerts:
            # Neue Tenders seit letztem Run finden
            since = alert.last_run.date() if alert.last_run else cutoff

            q = (
                db.query(Tender)
                .options(selectinload(Tender.buyer), selectinload(Tender.lots))
                .filter(Tender.published_date >= since)
            )
            if alert.keyword:
                kw = f"%{alert.keyword}%"
                q = q.filter(or_(
                    Tender.title.ilike(kw),
                    Tender.description.ilike(kw),
                ))
            if alert.country:
                q = q.filter(Tender.country_code == alert.country)
            if alert.cpv_prefix:
                q = q.filter(Tender.cpv_category == alert.cpv_prefix)
            if alert.doc_type:
                q = q.filter(Tender.doc_type == alert.doc_type)

            matches = q.order_by(Tender.published_date.desc()).limit(100).all()

            log.info(f"  Alert '{alert.name}' (ID={alert.id}): {len(matches)} neue Treffer")

            # Webhook senden wenn konfiguriert und Treffer vorhanden
            if matches and alert.webhook_url and _req:
                payload = {
                    "alert_id":    alert.id,
                    "alert_name":  alert.name,
                    "new_matches": len(matches),
                    "checked_at":  datetime.utcnow().isoformat() + "Z",
                    "tenders": [
                        {
                            "id":             m.id,
                            "title":          m.title[:200] if m.title else "",
                            "ted_url":        m.ted_url,
                            "published_date": m.published_date.isoformat() if m.published_date else None,
                            "deadline_date":  m.deadline_date.isoformat() if m.deadline_date else None,
                            "country":        m.country_code,
                            "buyer":          m.buyer.name[:100] if m.buyer else "",
                        }
                        for m in matches[:20]  # Max 20 im Webhook-Payload
                    ],
                }
                try:
                    resp = _req.post(
                        alert.webhook_url,
                        json=payload,
                        timeout=10,
                        headers={"Content-Type": "application/json"},
                    )
                    log.info(f"    Webhook gesendet → {resp.status_code}")
                except Exception as e:
                    log.warning(f"    Webhook fehlgeschlagen: {e}")

            # Alert-Status aktualisieren
            alert.last_run     = datetime.utcnow()
            alert.last_matches = len(matches)

        db.commit()
        log.info("Alert-Check abgeschlossen.")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TED OCDS-Scraper")
    parser.add_argument("--days", type=int, default=3,
                        help="Zeitraum in Tagen (Standard: 3 fuer taeglichen Lauf)")
    parser.add_argument("--land", type=str, default=None,
                        help="3-Letter ISO Laendercode, z.B. DEU. Mehrere: DEU,FRA,NLD")
    parser.add_argument("--kein-xml", action="store_true",
                        help="XML-Anreicherung ueberspringen (schneller, weniger Daten)")
    parser.add_argument("--xml-limit", type=int, default=200,
                        help="Max. Notices fuer XML-Anreicherung (default: 200)")
    parser.add_argument("--historisch", action="store_true",
                        help="Historischen Backfill starten (--days = wie weit zurueck)")
    parser.add_argument("--check-alerts", action="store_true",
                        help="Aktive Alerts pruefen und Webhook-Notifications senden")
    args = parser.parse_args()

    # Mehrere Laender unterstuetzen: --land DEU,FRA,NLD
    laender = [l.strip().upper() for l in args.land.split(",")] if args.land else [None]

    if args.check_alerts:
        check_alerts(days_back=args.days)
    elif args.historisch:
        for land in laender:
            scrape_historisch(
                tage_gesamt=args.days,
                chunk_tage=14,
                land=land,
                xml_limit_pro_chunk=50,
            )
    elif args.kein_xml:
        for land in laender:
            scrape(args.days, land)
    else:
        for land in laender:
            scrape_and_enrich(args.days, land, xml_limit=args.xml_limit)
