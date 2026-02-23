-- Add category_relevance column for scoring how well a result matches the search term
ALTER TABLE scraper_leads ADD COLUMN IF NOT EXISTS category_relevance REAL;
