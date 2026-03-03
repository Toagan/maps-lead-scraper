-- Add lead quality scoring and confidence fields.
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS fit_score REAL;
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS chain_confidence REAL;

CREATE INDEX IF NOT EXISTS idx_scraper_leads_fit_score ON scraper_leads(fit_score);
