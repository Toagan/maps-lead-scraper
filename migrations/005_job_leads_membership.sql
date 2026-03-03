-- Preserve per-job lead membership independently of canonical lead rows.
CREATE TABLE IF NOT EXISTS job_leads (
    job_id          UUID NOT NULL REFERENCES scrape_jobs(id) ON DELETE CASCADE,
    place_id        TEXT NOT NULL REFERENCES scraper_leads(place_id) ON DELETE CASCADE,
    found_by_query  TEXT,
    found_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (job_id, place_id)
);

CREATE INDEX IF NOT EXISTS idx_job_leads_job_id ON job_leads(job_id);
CREATE INDEX IF NOT EXISTS idx_job_leads_place_id ON job_leads(place_id);
