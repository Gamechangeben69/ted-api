# TED API Optimization Design
**Date:** 2026-04-07
**Project:** ted-api (RapidAPI publication)
**Status:** Approved

## Goal

Make the TED IT Tenders API competitive with major procurement data providers
(Tenders.guru, OpenOpps, EU Supply) for publication and sale on RapidAPI.
Focus: data quality, richer API responses, full-text search, and performance.

---

## Section 1: Data Quality Fixes

### 1.1 NUTS Codes (currently 0/1734)

**Problem:** Scraper reads `RP` field from TED API which is empty for eForms notices.
NUTS codes exist in the XML under:
- `cac:ProcurementProjectLot/cac:RealizedLocation/cbc:ID`
- `cac:DeliveryTerms/cac:DeliveryLocation/cbc:ID`

**Fix:** Extend `xml_parser.py` to extract NUTS codes during XML enrichment.
Write back to `tenders.nuts_code` and `lots.nuts_code` (already in schema).

### 1.2 Full XML Enrichment (currently 218/1734 have descriptions)

**Problem:** `--xml-limit` cap means most tenders lack description, lot values, NUTS.

**Fix:**
- Add `--backfill-xml` flag to `scraper.py` that enriches all tenders missing description
- Use `ThreadPoolExecutor(max_workers=4)` for parallel XML fetching (~7 min vs 25 min sequential)
- Increase daily `--xml-limit` from 200 to 500
- Add second daily cron at 06:00: `scraper.py --backfill-xml --limit 200` for stragglers

### 1.3 doc_type Labels

**Problem:** `competition`, `result`, `dir-awa-pre`, `cont-modif`, `planning` not mapped in `main.py`.

**Fix:** Extend `_TD_LABEL_MAP` in `main.py`:
```python
"competition":  "Contract Notice",
"result":       "Contract Award Notice",
"dir-awa-pre":  "Direct Award (Pre-announcement)",
"cont-modif":   "Contract Modification",
"planning":     "Prior Information Notice",
"veat":         "Voluntary ex-ante Transparency Notice",
```

### 1.4 Estimated Contract Value

**Fix:** Aggregate `SUM(lots.estimated_value)` per tender.
Expose as `total_estimated_value` + `currency` in API response.
Data already exists in `lots` table from XML enrichment.

---

## Section 2: API Features

### 2.1 Full-Text Search with Ranking

- Add `search_vector tsvector` column to `tenders` table
- PostgreSQL trigger auto-updates on INSERT/UPDATE from `title + description + cpv_main_label + cpv_category_label`
- Language: German stemming (`pg_catalog.german`) with English fallback
- When `keyword` param is used: rank by `ts_rank(search_vector, query)` instead of date

### 2.2 New Filters

| Parameter | Type | Description |
|-----------|------|-------------|
| `deadline_from` | date | Tenders with deadline >= date |
| `deadline_to` | date | Tenders with deadline <= date |
| `min_value` | float | Min estimated contract value (from lots) |
| `max_value` | float | Max estimated contract value (from lots) |
| `procedure` | str | `open`, `restricted`, `negotiated`, `competitive_dialogue` |
| `buyer_name` | str | Partial match on buyer name |
| `sort_by` | str | `date` (default), `deadline`, `value`, `relevance` |
| `sort_order` | str | `asc` / `desc` (default: `desc`) |

### 2.3 Richer Response Fields

All fields added to both list and detail responses:

| Field | Source |
|-------|--------|
| `total_estimated_value` | SUM(lots.estimated_value) |
| `currency` | lots.estimated_currency |
| `deadline_date` | tenders.deadline_date (already in DB) |
| `procedure_label` | _PR_MAP applied to procedure field |
| `active` | deadline_date >= today OR deadline_date IS NULL |

### 2.4 Pagination with Links

Add to `meta` object:
```json
{
  "total": 1734,
  "page": 2,
  "page_size": 20,
  "pages": 87,
  "next_url": "/tenders?page=3&...",
  "prev_url": "/tenders?page=1&..."
}
```

### 2.5 New Endpoint: POST /tenders/search

Complex search endpoint for programmatic use:
```json
{
  "keywords": ["cybersecurity", "cloud"],
  "cpv_codes": ["72", "48"],
  "countries": ["DEU", "FRA"],
  "deadline_from": "2026-04-01",
  "min_value": 100000,
  "active_only": true
}
```
Returns same structure as GET /tenders. Enables RapidAPI customer alerting workflows.

### 2.6 Enhanced /health Endpoint

Add to response:
```json
{
  "status": "ok",
  "tenders_in_db": 1734,
  "last_scraped_at": "2026-04-07T05:00:00Z",
  "coverage_days": 30,
  "countries_covered": ["DEU", "FRA", "POL", ...],
  "with_description_pct": 85,
  "with_value_pct": 60
}
```

---

## Section 3: Performance & Infrastructure

### 3.1 Parallel XML Enrichment

- `ThreadPoolExecutor(max_workers=4)` in `xml_parser.py`
- `--backfill-xml [--limit N]` flag in `scraper.py`
- Reduces backfill time from ~25 min to ~7 min

### 3.2 PostgreSQL Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_tenders_deadline ON tenders (deadline_date);
CREATE INDEX IF NOT EXISTS idx_tenders_doc_type ON tenders (doc_type);
CREATE INDEX IF NOT EXISTS idx_tenders_procedure ON tenders (procedure);
CREATE INDEX IF NOT EXISTS idx_tenders_search ON tenders USING GIN (search_vector);
CREATE INDEX IF NOT EXISTS idx_lots_tender_value ON lots (tender_id, estimated_value);
```

### 3.3 search_vector Trigger

```sql
CREATE OR REPLACE FUNCTION tenders_search_vector_update() RETURNS trigger AS $$
BEGIN
  NEW.search_vector :=
    setweight(to_tsvector('pg_catalog.german', coalesce(NEW.title, '')), 'A') ||
    setweight(to_tsvector('pg_catalog.german', coalesce(NEW.cpv_main_label, '')), 'B') ||
    setweight(to_tsvector('pg_catalog.german', coalesce(NEW.description, '')), 'C');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tenders_search_vector_trigger
BEFORE INSERT OR UPDATE ON tenders
FOR EACH ROW EXECUTE FUNCTION tenders_search_vector_update();
```

### 3.4 Updated Cron Schedule

```
0 5 * * *  scraper.py --days 3 --land DEU,FRA,POL,... --xml-limit 500
0 6 * * *  scraper.py --backfill-xml --limit 200
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `xml_parser.py` | Extract NUTS codes; parallel enrichment with ThreadPoolExecutor |
| `scraper.py` | Add `--backfill-xml` flag; increase xml-limit default |
| `database.py` | Add `search_vector` column to Tender model |
| `main.py` | Fix label maps; new filters; sort_by; pagination links; POST /search; enhanced /health |

## Files NOT Changed

- `enrichment.py` — CPV/NUTS label lookup unchanged
- `requirements.txt` — no new dependencies

---

## Success Criteria

- [ ] NUTS codes populated for >80% of tenders (after XML backfill)
- [ ] Descriptions populated for >80% of tenders (after XML backfill)
- [ ] All doc_type values return correct human-readable labels
- [ ] `keyword` search returns results ranked by relevance
- [ ] All new filters work correctly
- [ ] `total_estimated_value` populated for tenders that have lots with values
- [ ] POST /search endpoint functional
- [ ] /health shows scraping metadata
- [ ] All existing endpoints still pass basic smoke tests
