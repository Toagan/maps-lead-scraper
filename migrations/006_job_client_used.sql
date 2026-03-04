-- Track whether a job's leads have been sent to a client.
ALTER TABLE scrape_jobs ADD COLUMN IF NOT EXISTS client_used BOOLEAN DEFAULT FALSE;
