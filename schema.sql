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
    category_relevance REAL,
    fit_score      REAL,
    low_confidence BOOLEAN DEFAULT FALSE,
    is_chain       BOOLEAN DEFAULT FALSE,
    chain_confidence REAL,
    street         TEXT,
    postal_code    TEXT,
    city_parsed    TEXT,
    job_id         UUID,
    scraped_at     TIMESTAMPTZ DEFAULT NOW(),
    enriched_at    TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scraper_leads_country ON scraper_leads(country);
CREATE INDEX IF NOT EXISTS idx_scraper_leads_region ON scraper_leads(country, region);
CREATE INDEX IF NOT EXISTS idx_scraper_leads_place_id ON scraper_leads(place_id);
CREATE INDEX IF NOT EXISTS idx_scraper_leads_fit_score ON scraper_leads(fit_score);

CREATE TABLE IF NOT EXISTS scrape_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status              TEXT NOT NULL DEFAULT 'pending',
    job_name            TEXT,
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
    saturated_points    INTEGER DEFAULT 0,
    error_message       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS job_leads (
    job_id          UUID NOT NULL REFERENCES scrape_jobs(id) ON DELETE CASCADE,
    place_id        TEXT NOT NULL REFERENCES scraper_leads(place_id) ON DELETE CASCADE,
    found_by_query  TEXT,
    found_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (job_id, place_id)
);

CREATE INDEX IF NOT EXISTS idx_job_leads_job_id ON job_leads(job_id);
CREATE INDEX IF NOT EXISTS idx_job_leads_place_id ON job_leads(place_id);
