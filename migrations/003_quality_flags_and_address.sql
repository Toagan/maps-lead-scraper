-- Quality flags, structured address, job association, saturation tracking
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS low_confidence BOOLEAN DEFAULT FALSE;
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS is_chain BOOLEAN DEFAULT FALSE;
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS street TEXT;
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS postal_code TEXT;
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS city_parsed TEXT;
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS job_id UUID REFERENCES scrape_jobs(id);

ALTER TABLE scrape_jobs ADD COLUMN IF NOT EXISTS saturated_points INTEGER DEFAULT 0;
