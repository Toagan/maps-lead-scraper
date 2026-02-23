-- Custom job name
ALTER TABLE scrape_jobs ADD COLUMN IF NOT EXISTS job_name TEXT;
