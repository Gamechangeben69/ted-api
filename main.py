"""
TED Tenders API – FastAPI Server (OCDS Edition)
================================================
Endpunkte:
  GET /tenders            Ausschreibungen suchen & filtern (OCDS-Daten)
  GET /tenders/{id}       Einzelne Ausschreibung mit Lots + Awards
  GET /awards             Zuschläge durchsuchen
  GET /stats              Statistiken nach Land, CPV, Typ
  GET /health             Health-Check für Monitoring

Rate-Limiting (via X-RapidAPI-Key Header):
  free  → 50 Anfragen / Tag
  basic → 500 Anfragen / Tag
  pro   → unbegrenzt

Starten:
  uvicorn main:app --host 0.0.0.0 --port 8000
"""

import json
import os
from datetime import date, datetime, timedelta
from typing import List, Optional

from pydantic import BaseModel

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session, selectinload

from database import Alert, Award, Buyer, Lot, RateLimit, SessionLocal, Supplier, Tender, get_db, init_db
from enrichment import enrich_cpv, enrich_nuts

# ── App-Initialisierung ───────────────────────────────────────────────────────

app = FastAPI(
    title="TED IT Tenders API",
    description=(
        "Real-time IT procurement notices from the EU Official Journal (TED/eTendering). "
        "Covers all 27 EU member states + Norway, Switzerland, Iceland. "
        "OCDS-inspired data model with lots, awards, buyer & supplier info. "
        "Updated daily via automated scraper."
    ),
    version="2.0.0",
    contact={"name": "TED IT Tenders API"},
    license_info={"name": "EU Open Data", "url": "https://ted.europa.eu/"},
    docs_url="/",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


# ── Rate-Limiting ─────────────────────────────────────────────────────────────

TIER_LIMITS = {
    "free":  50,
    "basic": 500,
    "pro":   999_999,
}

# API_KEYS=key1:basic;key2:pro  (Umgebungsvariable)
API_KEY_TIERS: dict[str, str] = {}
for _raw in os.environ.get("API_KEYS", "").split(";"):
    if ":" in _raw:
        _k, _t = _raw.strip().split(":", 1)
        if _k:
            API_KEY_TIERS[_k] = _t


def check_rate_limit(
    x_rapidapi_key: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
):
    key   = x_rapidapi_key or "anonymous"
    tier  = API_KEY_TIERS.get(key, "free")
    limit = TIER_LIMITS.get(tier, TIER_LIMITS["free"])

    if limit >= 999_999:
        return {"key": key, "tier": tier}

    rl  = db.get(RateLimit, key)
    now = datetime.utcnow()

    if rl is None:
        rl = RateLimit(api_key=key, tier=tier, req_count=0, window_start=now)
        db.add(rl)

    if rl.window_start.date() < now.date():
        rl.req_count    = 0
        rl.window_start = now

    if rl.req_count >= limit:
        raise HTTPException(
            status_code=429,
            detail={
                "error":     "rate_limit_exceeded",
                "tier":      tier,
                "limit":     limit,
                "resets_at": (rl.window_start + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "upgrade":   "https://rapidapi.com/your-profile/api/ted-it-tenders",
            },
        )

    rl.req_count += 1
    db.commit()
    return {"key": key, "tier": tier, "remaining": limit - rl.req_count}


# ── Serialisierung ────────────────────────────────────────────────────────────

def _cpv_list(raw: str) -> list:
    try:
        codes = json.loads(raw) if raw else []
        # Deduplizieren (Reihenfolge bewahren)
        seen = set()
        result = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                result.append(c)
        return result
    except Exception:
        return []


def buyer_to_dict(b: Buyer) -> dict:
    if not b:
        return {}
    nuts = enrich_nuts(b.nuts_code or "") if b.nuts_code else {}
    return {
        "name":         b.name,
        "nuts_code":    b.nuts_code,
        "nuts_label":   nuts.get("region", ""),
        "country_code": b.country_code,
        "country":      nuts.get("country", "") or _CC3_TO_NAME.get(b.country_code or "", ""),
    }


def lot_to_dict(lot: Lot) -> dict:
    return {
        "lot_number":       lot.lot_number,
        "title":            lot.title,
        "description":      lot.description,
        "cpv_codes":        _cpv_list(lot.cpv_codes),
        "estimated_value":  lot.estimated_value,
        "currency":         lot.estimated_currency,
        "deadline_date":    lot.deadline_date.isoformat() if lot.deadline_date else None,
    }


def award_to_dict(a: Award) -> dict:
    supplier = None
    if a.supplier:
        supplier = {
            "name":         a.supplier.name,
            "country_code": a.supplier.country_code,
            "nuts_code":    a.supplier.nuts_code,
        }
    return {
        "award_notice_id":  a.award_notice_id,
        "award_date":       a.award_date.isoformat()      if a.award_date      else None,
        "published_date":   a.published_date.isoformat()  if a.published_date  else None,
        "contract_value":   a.contract_value,
        "currency":         a.contract_currency,
        "offers_received":  a.offers_received,
        "lot_number":       a.lot.lot_number               if a.lot             else None,
        "supplier":         supplier,
    }


_CC3_TO_NAME = {
    "DEU": "Germany", "FRA": "France", "POL": "Poland", "ITA": "Italy",
    "ESP": "Spain", "NLD": "Netherlands", "BEL": "Belgium", "AUT": "Austria",
    "SWE": "Sweden", "DNK": "Denmark", "FIN": "Finland", "PRT": "Portugal",
    "CZE": "Czech Republic", "SVK": "Slovakia", "HUN": "Hungary", "ROU": "Romania",
    "BGR": "Bulgaria", "HRV": "Croatia", "SVN": "Slovenia", "EST": "Estonia",
    "LVA": "Latvia", "LTU": "Lithuania", "LUX": "Luxembourg", "MLT": "Malta",
    "CYP": "Cyprus", "IRL": "Ireland", "GRC": "Greece", "NOR": "Norway",
    "CHE": "Switzerland", "ISL": "Iceland",
}

_NC_MAP = {
    "1": "Works", "2": "Supplies", "4": "Services", "5": "Mixed",
    "works": "Works", "supplies": "Supplies", "services": "Services",
    "combined": "Mixed",
}
_PR_MAP = {
    "1": "Open procedure", "2": "Restricted procedure",
    "3": "Negotiated (with prior notice)", "4": "Negotiated (without prior notice)",
    "6": "Competitive dialogue", "8": "Innovation partnership",
    "open": "Open procedure", "restricted": "Restricted procedure",
    "neg-w-call": "Negotiated (with prior notice)",
    "neg-wo-call": "Negotiated (without prior notice)",
    "comp-dial": "Competitive dialogue", "innovation": "Innovation partnership",
    "oth-single": "Negotiated (without prior notice)",
    "oth-mult": "Negotiated (with prior notice)",
}

def _clean_label(raw: Optional[str], mapping: dict, fallback: str = "") -> str:
    """Bereinigt DB-Werte die als Listen-Strings gespeichert wurden."""
    if not raw:
        return fallback
    # Altes Format: "['supplies', 'services']" → ersten Wert nehmen
    if raw.startswith("["):
        try:
            import ast
            vals = ast.literal_eval(raw)
            raw = vals[0] if vals else fallback
        except Exception:
            raw = raw.strip("[]'\"").split(",")[0].strip().strip("'\"")
    return mapping.get(str(raw).strip(), str(raw).strip()) if raw else fallback


_TD_LABEL_MAP = {
    # Classic TED codes
    "3": "Contract Notice", "F02": "Contract Notice",
    "cn": "Contract Notice", "cn-desg": "Contract Notice",
    "cn-social": "Contract Notice",
    "7": "Contract Award Notice", "F03": "Contract Award Notice",
    "can": "Contract Award Notice", "can-social": "Contract Award Notice",
    "1": "Prior Information Notice", "F01": "Prior Information Notice",
    "pin": "Prior Information Notice",
    # eForms form-type codes
    "competition": "Contract Notice",
    "result": "Contract Award Notice",
    "dir-awa-pre": "Direct Award Prenotification",
    "cont-modif": "Contract Modification Notice",
    "planning": "Prior Information Notice",
    "veat": "Voluntary Ex Ante Transparency Notice",
    "corr": "Corrigendum",
}


def tender_to_dict(t: Tender, detail: bool = False) -> dict:
    cpv_codes = _cpv_list(t.cpv_codes)
    cpv_enriched = [enrich_cpv(c) for c in cpv_codes[:5]] if cpv_codes else []

    # doc_type / doc_type_label bereinigen
    doc_type = t.doc_type or ""
    doc_label = t.doc_type_label or _TD_LABEL_MAP.get(doc_type, "")
    if not doc_label and doc_type:
        doc_label = _TD_LABEL_MAP.get(doc_type, doc_type)

    base = {
        "id":             t.id,
        "ted_url":        t.ted_url,
        "doc_type":       doc_type,
        "doc_type_label": doc_label,
        "published_date": t.published_date.isoformat() if t.published_date else None,
        "deadline_date":  t.deadline_date.isoformat()  if t.deadline_date  else None,
        "title":          t.title,
        "buyer":          buyer_to_dict(t.buyer) if t.buyer else {"name": None},
        "location": {
            "country_code":  t.country_code,
            "country":       t.country_label or _CC3_TO_NAME.get(t.country_code or "", ""),
            "nuts_code":     t.nuts_code,
            "nuts_label":    t.nuts_label,
        },
        "cpv": {
            "codes":          cpv_codes,
            "main_code":      t.cpv_main,
            "main_label":     t.cpv_main_label,
            "category":       t.cpv_category,
            "category_label": t.cpv_category_label,
            "enriched":       cpv_enriched,
        },
        "procedure": {
            "contract_type":       _clean_label(t.contract_type, _NC_MAP),
            "contract_type_label": _clean_label(t.contract_type, _NC_MAP),
            "procedure":           _clean_label(t.procedure, _PR_MAP),
            "procedure_label":     _clean_label(t.procedure, _PR_MAP),
            "award_criteria":      t.award_criteria or "",
            "award_criteria_label":t.award_criteria_label or "",
        },
        "award_notice_id": t.award_notice_id,
        "has_award":       bool(t.awards),
        "lot_count":       len(t.lots),
        "total_estimated_value": sum(
            l.estimated_value for l in t.lots if l.estimated_value
        ) or None,
        "currency": next(
            (l.estimated_currency for l in t.lots if l.estimated_currency), None
        ),
        "active": (
            t.deadline_date is None or t.deadline_date >= __import__("datetime").date.today()
        ) if t.deadline_date is not None else (
            t.doc_type in ("competition", "3", "cn", "cn-desg", "cn-social", "F02")
        ),
    }

    if detail:
        base["lots"]        = [lot_to_dict(l) for l in t.lots]
        base["awards"]      = [award_to_dict(a) for a in t.awards]
        base["description"] = t.description

    return base


# ── Endpunkte ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"], summary="API health and data coverage stats")
def health(db: Session = Depends(get_db)):
    """Returns API health status with database coverage statistics."""
    from sqlalchemy import func as _f
    total_tenders = db.query(_f.count(Tender.id)).scalar() or 0
    total_awards  = db.query(_f.count(Award.id)).scalar() or 0

    with_desc  = db.query(_f.count(Tender.id)).filter(Tender.description.isnot(None)).scalar() or 0
    with_nuts  = db.query(_f.count(Tender.id)).filter(
        Tender.nuts_code.isnot(None), Tender.nuts_code != ""
    ).scalar() or 0
    with_value = (
        db.query(_f.count(Tender.id.distinct()))
        .join(Tender.lots)
        .filter(Lot.estimated_value.isnot(None))
        .scalar() or 0
    )
    active_count = db.query(_f.count(Tender.id)).filter(
        or_(Tender.deadline_date.is_(None), Tender.deadline_date >= date.today())
    ).scalar() or 0

    countries = db.query(
        Tender.country_code, _f.count(Tender.id)
    ).group_by(Tender.country_code).order_by(_f.count(Tender.id).desc()).limit(10).all()

    last_scraped = db.query(_f.max(Tender.scraped_at)).scalar()

    return {
        "status": "ok",
        "database": {
            "tenders":       total_tenders,
            "awards":        total_awards,
            "active":        active_count,
            "last_scraped":  last_scraped.isoformat() if last_scraped else None,
        },
        "coverage": {
            "with_description": {
                "count":   with_desc,
                "percent": round(with_desc / total_tenders * 100, 1) if total_tenders else 0,
            },
            "with_nuts_code": {
                "count":   with_nuts,
                "percent": round(with_nuts / total_tenders * 100, 1) if total_tenders else 0,
            },
            "with_value": {
                "count":   with_value,
                "percent": round(with_value / total_tenders * 100, 1) if total_tenders else 0,
            },
        },
        "top_countries": [
            {"country_code": cc, "tenders": cnt} for cc, cnt in countries
        ],
    }


@app.get(
    "/tenders",
    tags=["Tenders"],
    summary="Search EU IT tenders",
    response_description="Paginated list of IT procurement notices",
)
def list_tenders(
    request: Request,
    # Geographic
    country:       Optional[str]  = Query(None, description="3-letter ISO code: DEU, FRA, NLD …"),
    nuts:          Optional[str]  = Query(None, description="NUTS region prefix, e.g. DE2 for Bavaria"),
    # Content
    cpv:           Optional[str]  = Query(None, description="CPV prefix: 72=IT Services, 48=Software, 30=Hardware"),
    keyword:       Optional[str]  = Query(None, description="Full-text search in title and description"),
    # Type
    doc_type:      Optional[str]  = Query(None, description="3=Contract Notice, 7=Award Notice, competition, result"),
    has_award:     Optional[bool] = Query(None, description="Only tenders with linked award notice"),
    # Time
    active:        bool           = Query(True,  description="Only active tenders (deadline >= today)"),
    days:          Optional[int]  = Query(None,  description="Published within last N days"),
    date_from:     Optional[date] = Query(None,  description="Published from (YYYY-MM-DD)"),
    date_to:       Optional[date] = Query(None,  description="Published to (YYYY-MM-DD)"),
    deadline_from: Optional[date] = Query(None,  description="Submission deadline from (YYYY-MM-DD)"),
    deadline_to:   Optional[date] = Query(None,  description="Submission deadline to (YYYY-MM-DD)"),
    # Value
    min_value:     Optional[float]= Query(None,  description="Minimum estimated contract value (EUR)"),
    max_value:     Optional[float]= Query(None,  description="Maximum estimated contract value (EUR)"),
    # Procedure
    procedure:     Optional[str]  = Query(None,  description="Procedure code: OPEN, RESTRICTED, NEG-W-CALL …"),
    buyer_name:    Optional[str]  = Query(None,  description="Partial buyer/authority name search"),
    # Sort
    sort_by:       str            = Query("published_date", description="Sort field: published_date, deadline_date, title, relevance"),
    sort_order:    str            = Query("desc",            description="Sort order: asc or desc"),
    # Pagination
    page:          int            = Query(1,  ge=1,      description="Page number"),
    page_size:     int            = Query(20, ge=1, le=100, description="Results per page (max 100)"),
    # Auth
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    q = (
        db.query(Tender)
        .options(
            selectinload(Tender.buyer),
            selectinload(Tender.lots),
            selectinload(Tender.awards).selectinload(Award.supplier),
        )
    )

    q = _apply_tender_filters(
        q,
        country=country, nuts=nuts, cpv=cpv, keyword=keyword, doc_type=doc_type,
        has_award=has_award, active=active, days=days, date_from=date_from, date_to=date_to,
        deadline_from=deadline_from, deadline_to=deadline_to,
        min_value=min_value, max_value=max_value,
        procedure=procedure, buyer_name=buyer_name,
    )

    total   = q.count()
    pages   = (total + page_size - 1) // page_size if total else 0

    q = _apply_sort(q, sort_by, sort_order, keyword=keyword)
    results = q.offset((page - 1) * page_size).limit(page_size).all()

    # Build pagination links
    base_url = str(request.url).split("?")[0]
    params   = dict(request.query_params)

    def page_url(p):
        params["page"] = str(p)
        return base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())

    links = {}
    if page > 1:
        links["prev"] = page_url(page - 1)
    if page < pages:
        links["next"] = page_url(page + 1)

    return {
        "meta": {
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "pages":     pages,
            "links":     links,
        },
        "results": [tender_to_dict(t) for t in results],
    }




class TenderSearchRequest(BaseModel):
    country:       Optional[str]   = None
    nuts:          Optional[str]   = None
    cpv:           Optional[str]   = None
    keyword:       Optional[str]   = None
    doc_type:      Optional[str]   = None
    has_award:     Optional[bool]  = None
    active:        bool            = True
    days:          Optional[int]   = None
    date_from:     Optional[date]  = None
    date_to:       Optional[date]  = None
    deadline_from: Optional[date]  = None
    deadline_to:   Optional[date]  = None
    min_value:     Optional[float] = None
    max_value:     Optional[float] = None
    procedure:     Optional[str]   = None
    buyer_name:    Optional[str]   = None
    sort_by:       str             = "published_date"
    sort_order:    str             = "desc"
    page:          int             = 1
    page_size:     int             = 20


@app.post(
    "/tenders/search",
    tags=["Tenders"],
    summary="Advanced tender search via JSON body",
    response_description="Paginated list of IT procurement notices",
)
def search_tenders(
    body: TenderSearchRequest,
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """Search tenders with a JSON body (same filters as GET /tenders but POST-friendly
    for complex queries or long keyword strings)."""
    page_size = min(max(body.page_size, 1), 100)
    page      = max(body.page, 1)

    q = (
        db.query(Tender)
        .options(
            selectinload(Tender.buyer),
            selectinload(Tender.lots),
            selectinload(Tender.awards).selectinload(Award.supplier),
        )
    )

    q = _apply_tender_filters(
        q,
        country=body.country, nuts=body.nuts, cpv=body.cpv, keyword=body.keyword,
        doc_type=body.doc_type, has_award=body.has_award, active=body.active,
        days=body.days, date_from=body.date_from, date_to=body.date_to,
        deadline_from=body.deadline_from, deadline_to=body.deadline_to,
        min_value=body.min_value, max_value=body.max_value,
        procedure=body.procedure, buyer_name=body.buyer_name,
    )

    total = q.count()
    pages = (total + page_size - 1) // page_size if total else 0

    q       = _apply_sort(q, body.sort_by, body.sort_order, keyword=body.keyword)
    results = q.offset((page - 1) * page_size).limit(page_size).all()

    return {
        "meta": {
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "pages":     pages,
        },
        "results": [tender_to_dict(t) for t in results],
    }

@app.get(
    "/tenders/{tender_id}",
    tags=["Tenders"],
    summary="Get full tender details including lots and awards",
    response_description=(
        "Full tender record with description, lots (title/CPV/estimated value), "
        "and awards (supplier name, contract value, offers received). "
        "XML data is fetched on-demand from TED on first access."
    ),
)
def get_tender(
    tender_id: str,
    enrich: bool = Query(
        True,
        description="Fetch full XML from TED if description/lots are missing (default: true). "
                    "Set to false for faster response when XML detail is not needed."
    ),
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    t = (
        db.query(Tender)
        .options(
            selectinload(Tender.buyer),
            selectinload(Tender.lots),
            selectinload(Tender.awards).selectinload(Award.supplier),
            selectinload(Tender.awards).selectinload(Award.lot),
        )
        .filter(Tender.id == tender_id)
        .first()
    )
    if not t:
        raise HTTPException(status_code=404, detail=f"Tender '{tender_id}' not found.")

    # Lazy XML enrichment: falls Beschreibung/Lose noch fehlen, jetzt nachladen
    if enrich and (not t.description or not t.lots):
        try:
            from xml_parser import enrich_and_save
            enrich_and_save(db, tender_id)
            # Nach Anreicherung: Tender neu laden damit Lots/Awards up-to-date
            db.expire(t)
            t = (
                db.query(Tender)
                .options(
                    selectinload(Tender.buyer),
                    selectinload(Tender.lots),
                    selectinload(Tender.awards).selectinload(Award.supplier),
                    selectinload(Tender.awards).selectinload(Award.lot),
                )
                .filter(Tender.id == tender_id)
                .first()
            )
        except Exception:
            pass  # XML-Fehler darf die Antwort nicht blockieren

    return tender_to_dict(t, detail=True)


@app.get(
    "/awards",
    tags=["Awards"],
    summary="Search contract award notices",
    response_description="Paginated list of contract awards with supplier and value info",
)
def list_awards(
    country:   Optional[str] = Query(None, description="3-letter ISO country code"),
    days:      Optional[int] = Query(None, description="Zuschläge aus den letzten N Tagen"),
    min_value: Optional[float] = Query(None, description="Mindestwert in EUR"),
    page:      int           = Query(1,   ge=1),
    page_size: int           = Query(20,  ge=1, le=100),
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    q = (
        db.query(Award)
        .options(
            selectinload(Award.tender).selectinload(Tender.buyer),
            selectinload(Award.supplier),
            selectinload(Award.lot),
        )
        .join(Award.tender)
    )

    if country:
        q = q.filter(Tender.country_code == country.upper())

    if days:
        q = q.filter(Award.published_date >= date.today() - timedelta(days=days))

    if min_value:
        q = q.filter(Award.contract_value >= min_value)

    total   = q.count()
    results = q.order_by(Award.published_date.desc()).offset((page - 1) * page_size).limit(page_size).all()

    def _fmt(a: Award):
        return {
            "award_notice_id":  a.award_notice_id,
            "award_date":       a.award_date.isoformat()      if a.award_date      else None,
            "published_date":   a.published_date.isoformat()  if a.published_date  else None,
            "contract_value":   a.contract_value,
            "currency":         a.contract_currency,
            "offers_received":  a.offers_received,
            "tender": {
                "id":     a.tender.id          if a.tender else None,
                "title":  a.tender.title[:200] if a.tender and a.tender.title else None,
                "country":a.tender.country_code if a.tender else None,
                "ted_url":a.tender.ted_url      if a.tender else None,
            },
            "supplier": {
                "name":         a.supplier.name         if a.supplier else None,
                "country_code": a.supplier.country_code if a.supplier else None,
            },
        }

    return {
        "meta":    {"total": total, "page": page, "page_size": page_size,
                    "pages": (total + page_size - 1) // page_size if total else 0},
        "results": [_fmt(a) for a in results],
    }


@app.get(
    "/stats",
    tags=["Stats"],
    summary="Aggregated statistics on available tender data",
)
def stats(
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    total          = db.query(func.count(Tender.id)).scalar()
    active_count   = (
        db.query(func.count(Tender.id))
        .filter(or_(Tender.deadline_date.is_(None), Tender.deadline_date >= date.today()))
        .scalar()
    )
    awarded_count  = db.query(func.count(Tender.id)).filter(Tender.award_notice_id.isnot(None)).scalar()
    awards_total   = db.query(func.count(Award.id)).scalar()
    newest         = db.query(func.max(Tender.published_date)).scalar()

    by_country = (
        db.query(Tender.country_code, Tender.country_label, func.count(Tender.id))
        .group_by(Tender.country_code, Tender.country_label)
        .order_by(func.count(Tender.id).desc())
        .limit(30).all()
    )
    by_cpv = (
        db.query(Tender.cpv_category, Tender.cpv_category_label, func.count(Tender.id))
        .group_by(Tender.cpv_category, Tender.cpv_category_label)
        .order_by(func.count(Tender.id).desc())
        .limit(15).all()
    )
    by_type = (
        db.query(Tender.doc_type_label, func.count(Tender.id))
        .group_by(Tender.doc_type_label)
        .order_by(func.count(Tender.id).desc())
        .all()
    )

    return {
        "summary": {
            "total_tenders":   total,
            "active_tenders":  active_count,
            "awarded_tenders": awarded_count,
            "total_awards":    awards_total,
            "latest_published":newest.isoformat() if newest else None,
        },
        "by_country": [
            {"country_code": cc, "country": cl or cc, "count": n}
            for cc, cl, n in by_country if cc
        ],
        "by_cpv_category": [
            {"category": cat, "label": lbl or cat, "count": n}
            for cat, lbl, n in by_cpv if cat
        ],
        "by_doc_type": [
            {"doc_type": dt, "count": n}
            for dt, n in by_type if dt
        ],
    }


# ── Suppliers ─────────────────────────────────────────────────────────────────

@app.get(
    "/suppliers",
    tags=["Suppliers"],
    summary="Search winning suppliers / contractors",
    response_description="Suppliers with win count and total awarded value",
)
def list_suppliers(
    keyword:   Optional[str] = Query(None, description="Name search"),
    country:   Optional[str] = Query(None, description="Supplier country code"),
    page:      int           = Query(1,   ge=1),
    page_size: int           = Query(20,  ge=1, le=100),
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    from sqlalchemy import case

    q = (
        db.query(
            Supplier.id,
            Supplier.name,
            Supplier.country_code,
            func.count(Award.id).label("win_count"),
            func.sum(Award.contract_value).label("total_value"),
            func.max(Award.published_date).label("last_award"),
        )
        .outerjoin(Award, Award.supplier_id == Supplier.id)
        .group_by(Supplier.id, Supplier.name, Supplier.country_code)
    )

    if keyword:
        q = q.filter(Supplier.name.ilike(f"%{keyword}%"))

    if country:
        q = q.filter(Supplier.country_code == country.upper())

    total   = q.count()
    results = (
        q.order_by(func.count(Award.id).desc())
         .offset((page - 1) * page_size)
         .limit(page_size)
         .all()
    )

    return {
        "meta": {
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "pages":     (total + page_size - 1) // page_size if total else 0,
        },
        "results": [
            {
                "id":           row.id,
                "name":         row.name,
                "country_code": row.country_code,
                "country":      _CC3_TO_NAME.get(row.country_code or "", ""),
                "win_count":    row.win_count or 0,
                "total_value":  round(row.total_value, 2) if row.total_value else None,
                "last_award":   row.last_award.isoformat() if row.last_award else None,
            }
            for row in results
        ],
    }


@app.get(
    "/suppliers/{supplier_id}/awards",
    tags=["Suppliers"],
    summary="Get all awards for a specific supplier",
)
def get_supplier_awards(
    supplier_id: int,
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    supplier = db.get(Supplier, supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail=f"Supplier {supplier_id} not found.")

    q = (
        db.query(Award)
        .options(
            selectinload(Award.tender).selectinload(Tender.buyer),
            selectinload(Award.lot),
        )
        .filter(Award.supplier_id == supplier_id)
    )
    total   = q.count()
    results = q.order_by(Award.published_date.desc()).offset((page - 1) * page_size).limit(page_size).all()

    return {
        "supplier": {
            "id":           supplier.id,
            "name":         supplier.name,
            "country_code": supplier.country_code,
            "country":      _CC3_TO_NAME.get(supplier.country_code or "", ""),
        },
        "meta":    {"total": total, "page": page, "page_size": page_size,
                    "pages": (total + page_size - 1) // page_size if total else 0},
        "results": [award_to_dict(a) for a in results],
    }


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.post(
    "/alerts",
    tags=["Alerts"],
    summary="Create a saved search alert",
    response_description=(
        "Create a named search profile. The system will check daily for new tenders "
        "matching the criteria and (if configured) send notifications via webhook or email."
    ),
    status_code=201,
)
def create_alert(
    name:        str            = Query(...,  description="Alert name, e.g. 'IT Services Germany'"),
    keyword:     Optional[str]  = Query(None, description="Keyword to match in title/description"),
    country:     Optional[str]  = Query(None, description="Country filter, e.g. DEU"),
    cpv_prefix:  Optional[str]  = Query(None, description="CPV prefix, e.g. '72' for IT Services"),
    doc_type:    Optional[str]  = Query(None, description="'cn' for notices, 'can' for awards"),
    min_value:   Optional[float]= Query(None, description="Minimum contract value in EUR"),
    email:       Optional[str]  = Query(None, description="E-mail address for notifications (future)"),
    webhook_url: Optional[str]  = Query(None, description="Webhook URL for notifications (future)"),
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    alert = Alert(
        name        = name[:200],
        keyword     = keyword,
        country     = country.upper() if country else None,
        cpv_prefix  = cpv_prefix,
        doc_type    = doc_type,
        min_value   = min_value,
        email       = email,
        webhook_url = webhook_url,
        active      = 1,
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)

    # Sofort prüfen: wie viele aktuelle Tenders passen?
    matches = _run_alert_query(db, alert, days=14)

    return {
        "id":           alert.id,
        "name":         alert.name,
        "criteria": {
            "keyword":    alert.keyword,
            "country":    alert.country,
            "cpv_prefix": alert.cpv_prefix,
            "doc_type":   alert.doc_type,
            "min_value":  alert.min_value,
        },
        "current_matches_14d": len(matches),
        "sample":       [tender_to_dict(t) for t in matches[:3]],
        "message":      "Alert created. Will be checked daily for new matches.",
    }


@app.get(
    "/alerts",
    tags=["Alerts"],
    summary="List all saved alerts",
)
def list_alerts(
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    alerts = db.query(Alert).filter(Alert.active == 1).order_by(Alert.created_at.desc()).all()
    return {
        "total": len(alerts),
        "results": [
            {
                "id":           a.id,
                "name":         a.name,
                "criteria": {
                    "keyword":    a.keyword,
                    "country":    a.country,
                    "cpv_prefix": a.cpv_prefix,
                    "doc_type":   a.doc_type,
                    "min_value":  a.min_value,
                },
                "email":        a.email,
                "webhook_url":  a.webhook_url,
                "last_run":     a.last_run.isoformat() if a.last_run else None,
                "last_matches": a.last_matches,
                "created_at":   a.created_at.isoformat() if a.created_at else None,
            }
            for a in alerts
        ],
    }


@app.get(
    "/alerts/{alert_id}/check",
    tags=["Alerts"],
    summary="Manually trigger an alert check and return current matches",
)
def check_alert(
    alert_id: int,
    days: int = Query(7, description="How many days back to search for matches"),
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    alert = db.get(Alert, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found.")

    matches = _run_alert_query(db, alert, days=days)

    alert.last_run     = datetime.utcnow()
    alert.last_matches = len(matches)
    db.commit()

    return {
        "alert_id":  alert_id,
        "name":      alert.name,
        "days":      days,
        "matches":   len(matches),
        "results":   [tender_to_dict(t) for t in matches],
    }


@app.delete(
    "/alerts/{alert_id}",
    tags=["Alerts"],
    summary="Deactivate an alert",
    status_code=200,
)
def delete_alert(
    alert_id: int,
    _rate = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    alert = db.get(Alert, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found.")
    alert.active = 0
    db.commit()
    return {"message": f"Alert {alert_id} deactivated."}


def _run_alert_query(db: Session, alert: Alert, days: int = 7) -> list:
    """Führt die gespeicherte Alert-Suche aus und gibt passende Tenders zurück."""
    q = (
        db.query(Tender)
        .options(
            selectinload(Tender.buyer),
            selectinload(Tender.lots),
            selectinload(Tender.awards),
        )
        .filter(Tender.published_date >= date.today() - timedelta(days=days))
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

    if alert.min_value:
        # Lots mit Mindestwert
        from sqlalchemy import exists
        q = q.filter(
            exists().where(
                (Lot.tender_id == Tender.id) & (Lot.estimated_value >= alert.min_value)
            )
        )

    return q.order_by(Tender.published_date.desc()).limit(50).all()
