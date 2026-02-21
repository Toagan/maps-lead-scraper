-- Maps Lead Scraper v2 — Supabase schema
-- Uses "scraper_leads" to avoid conflict with existing "leads" table (Instantly)

CREATE TABLE IF NOT EXISTS scraper_leads (
    id             BIGSERIAL PRIMARY KEY,
    place_id       TEXT UNIQUE NOT NULL,
    cid            TEXT,
    name           TEXT NOT NULL,
    address        TEXT,
    phone          TEXT,
    website        TEXT,
    email          TEXT,
    email_source   TEXT,
    rating         REAL,
    review_count   INTEGER,
    category       TEXT,
    categories     TEXT,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    thumbnail_url  TEXT,
    operating_hours JSONB,
    price_range    TEXT,
    description    TEXT,
    country        TEXT NOT NULL,
    region         TEXT,
    city           TEXT,
    search_term    TEXT,
    scraped_at     TIMESTAMPTZ DEFAULT NOW(),
    enriched_at    TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scraper_leads_country ON scraper_leads(country);
CREATE INDEX IF NOT EXISTS idx_scraper_leads_region ON scraper_leads(country, region);
CREATE INDEX IF NOT EXISTS idx_scraper_leads_place_id ON scraper_leads(place_id);

CREATE TABLE IF NOT EXISTS scrape_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status              TEXT NOT NULL DEFAULT 'pending',
    search_term         TEXT NOT NULL,
    country             TEXT NOT NULL,
    targeting_mode      TEXT NOT NULL,
    targeting_config    JSONB NOT NULL,
    enrich_emails       BOOLEAN DEFAULT FALSE,
    total_locations     INTEGER DEFAULT 0,
    processed_locations INTEGER DEFAULT 0,
    total_leads         INTEGER DEFAULT 0,
    total_duplicates    INTEGER DEFAULT 0,
    total_enriched      INTEGER DEFAULT 0,
    total_api_calls     INTEGER DEFAULT 0,
    error_message       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);
