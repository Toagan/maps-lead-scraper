-- Index on email to speed up enrichment queries (WHERE email IS NULL)
CREATE INDEX IF NOT EXISTS idx_scraper_leads_email ON scraper_leads(email);
