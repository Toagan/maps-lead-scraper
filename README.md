# BlueReach Lead Scraper

Google Maps lead scraper powered by the Serper API. Scrapes business listings across 50+ countries with smart grid-based search, deduplication, email enrichment, and SERP-based website discovery.

Built with FastAPI + Supabase + vanilla JS frontend. Docker-ready for one-click Railway deployment.

## Features

### Scraping
- **Multi-country scraping** — DACH (DE/AT/CH) with region targeting, or 50+ worldwide countries
- **Multi-country jobs** — scrape multiple countries in a single job (e.g. `["de", "at", "ch"]`)
- **Smart grid search** — generates coordinate grids for large cities (100k+ pop) to overcome Google's 120-result cap per query
- **4 scrape modes** — Quick (50k+ pop), Smart (10k+), Thorough (5k+), Max (all locations / PLZ grid)
- **4 targeting modes** — Country-wide, by regions, by city names, or radius from coordinates
- **Adaptive two-pass scraping** — first pass for broad coverage, second pass deepens high-yield saturated grid points automatically
- **Credit management** — estimation before scraping, hard budget caps per job, per-call budget tracking
- **Preview mode** — test a search with 1 API call before committing
- **Job resume** — resume cancelled or budget-reached jobs from where they left off
- **Orphaned job recovery** — automatically marks stale running jobs as cancelled on server restart

### Enrichment
- **Email enrichment** — crawls business websites (homepage + subpages like /kontakt, /impressum, /contact) to extract emails
- **SERP discovery** — for leads with no website, searches Google to find their website, then extracts emails from it
- **Directory filtering** — skips known aggregator/directory domains (Yelp, Gelbe Seiten, Facebook, etc.) during SERP discovery
- **Domain cooldown** — rate-limits crawling per domain to avoid getting blocked

### Data Quality
- **Global deduplication** — dedup by `place_id` within each job, plus cross-job dedup via upsert
- **Closed business detection** — filters permanently closed businesses (DE/EN/FR/IT)
- **Chain detection** — flags businesses appearing 5+ times with confidence scores
- **Category relevance scoring** — 0.0-1.0 score using substring, word, and stem-prefix matching (works across German/English)
- **Lead fit scoring** — per-lead `fit_score` (0-1) combining relevance, website/phone presence, rating, review count, and confidence
- **Low-confidence flagging** — marks leads with 2 or fewer reviews
- **DACH address parsing** — extracts street, postal code, and city from German/Austrian/Swiss address formats

### Export & UI
- **Per-job CSV export** — membership-safe exports via `job_leads` table (no cross-job overwrite)
- **Streaming CSV** — paginated streaming export, no row cap
- **Google Maps links** — computed `google_maps_url` from CID on every lead
- **Real-time job tracking** — progress, cancellation, auto-polling in the web UI
- **34 pre-built category bundles** — 20 German + 14 English niche bundles (dentists, lawyers, construction, etc.)
- **Custom bundles** — save your own search term sets to the database
- **AI-powered term generation** — GPT-4o-mini suggests search terms for any niche in any language

### Configuration
- **Runtime API key override** — swap Serper API key from the UI without restarting
- **Tunable rate limits** — configurable max RPS, concurrency, enricher parallelism, and domain cooldown

## Quick Start

### Prerequisites

- Python 3.12+
- Supabase project (free tier works)
- Serper API key ([serper.dev](https://serper.dev))
- OpenAI API key (optional — for email enrichment & term suggestions)

### Setup

```bash
git clone https://github.com/Toagan/maps-lead-scraper.git
cd maps-lead-scraper
pip install -r requirements.txt
```

Create `.env`:

```
SERPER_API_KEY=your_serper_key
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_key
OPENAI_API_KEY=your_openai_key  # optional
```

Initialize the database — run `schema.sql` in Supabase SQL editor, then run migrations in order:

```
migrations/002_add_category_relevance.sql
migrations/003_quality_flags_and_address.sql
migrations/004_job_name.sql
migrations/005_job_leads_membership.sql
migrations/006_fit_score_and_chain_confidence.sql
```

### Run

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000` for the web UI.

## API Endpoints

### Scraping
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/scrape` | Start a scrape job |
| `POST` | `/scrape/estimate` | Estimate credits without starting |
| `POST` | `/scrape/preview` | 1-page test search (3 credits) |

### Jobs
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/jobs` | List all jobs (paginated) |
| `GET` | `/jobs/{id}` | Get job details + running status |
| `POST` | `/jobs/{id}/cancel` | Cancel a running job |
| `POST` | `/jobs/{id}/resume` | Resume a cancelled or budget-reached job |
| `DELETE` | `/jobs/{id}` | Delete a finished job |

### Leads
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/leads` | Query leads with filters |
| `GET` | `/leads?format=csv` | Export as streaming CSV |
| `GET` | `/leads/categories` | List distinct categories for a job |

### Categories
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/categories` | List all category bundles (built-in + custom) |
| `POST` | `/categories/suggest` | AI-generate search terms for a niche |
| `POST` | `/categories/save` | Save a custom bundle |
| `DELETE` | `/categories/{key}` | Delete a custom bundle |

### Settings
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/settings` | Get current settings (masked API key, status) |
| `PUT` | `/settings/serper-key` | Override the Serper API key at runtime |
| `DELETE` | `/settings/serper-key` | Reset Serper key to `.env` default |

### Other
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/stats` | Dashboard statistics (total leads, by country, email/phone/website counts) |
| `GET` | `/regions` | DACH regions list with city counts |
| `GET` | `/worldwide-countries` | Available worldwide countries |
| `GET` | `/health` | Health check |

## Lead Filters

The `/leads` endpoint supports these query parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `country` | string | Filter by country code (e.g. `de`) |
| `region` | string | Filter by region code (e.g. `BY`) |
| `category` | string | Substring match on category |
| `categories` | string | Comma-separated exact category match |
| `has_email` | bool | Only leads with email |
| `has_phone` | bool | Only leads with phone |
| `has_website` | bool | Only leads with website |
| `search_term` | string | Substring match on search term |
| `min_relevance` | float | Minimum category relevance (0.0-1.0) |
| `min_fit_score` | float | Minimum fit score (0.0-1.0) |
| `min_reviews` | int | Minimum review count |
| `job_id` | string | Filter by job (uses `job_leads` membership) |
| `exclude_chains` | bool | Exclude detected chains |
| `exclude_low_confidence` | bool | Exclude low-confidence leads |
| `limit` | int | Results per page (default 100) |
| `offset` | int | Pagination offset |
| `format` | string | Set to `csv` for CSV export |
| `filename` | string | Custom CSV filename |

## Scrape Request Body

```json
{
  "job_name": "My Custom Job Name",
  "search_term": "Zahnarzt",
  "category_key": "dental_de",
  "country": "de",
  "countries": ["de", "at", "ch"],
  "targeting_mode": "radius",
  "regions": ["BY", "BW"],
  "cities": ["München", "Stuttgart"],
  "center_lat": 48.137,
  "center_lng": 11.576,
  "radius_km": 50,
  "scrape_mode": "smart",
  "enrich_emails": true,
  "serp_discovery": false,
  "credit_limit": 10000
}
```

Use either `search_term` (single term) or `category_key` (bundle of terms). `countries` list overrides `country`. All targeting/mode fields are optional with sensible defaults.

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SERPER_API_KEY` | Yes | — | Serper.dev API key |
| `SUPABASE_URL` | Yes | — | Supabase project URL |
| `SUPABASE_KEY` | Yes | — | Supabase anon/service key |
| `OPENAI_API_KEY` | No | — | For email enrichment & AI term generation |
| `SERPER_MAX_RPS` | No | `50` | Max Serper requests/sec |
| `SERPER_MAX_CONCURRENT` | No | `20` | Max concurrent Serper requests |
| `ENRICHER_MAX_CONCURRENT` | No | `10` | Max concurrent website crawl requests |
| `ENRICHER_DOMAIN_COOLDOWN` | No | `0.5` | Seconds between requests to the same domain |
| `BATCH_UPSERT_SIZE` | No | `50` | Leads per database upsert batch |

## Deployment

Includes `Dockerfile` and `railway.toml` for one-click Railway deployment.

```bash
# Build and run with Docker
docker build -t bluereach .
docker run -p 8000:8000 --env-file .env bluereach
```

Set environment variables in the Railway dashboard for cloud deployment. The health check endpoint (`/health`) is pre-configured.

## Architecture

```
app/
├── api/              # FastAPI route handlers
│   ├── scrape.py     # /scrape, /scrape/estimate, /scrape/preview, /categories
│   ├── jobs.py       # /jobs CRUD + cancel + resume
│   ├── leads.py      # /leads query + streaming CSV export
│   ├── stats.py      # /stats dashboard
│   ├── regions.py    # /regions, /worldwide-countries
│   ├── settings.py   # /settings, runtime API key management
│   └── router.py     # Central router + /health
├── services/
│   ├── scraper.py    # Job orchestrator (grid search, two-pass, dedup, fit scoring)
│   ├── serper.py     # Serper API client (maps + web search), relevance scoring, address parsing
│   ├── database.py   # Supabase CRUD (leads, jobs, job_leads, custom bundles)
│   ├── enricher.py   # Email enrichment (website crawl) + SERP discovery pipeline
│   └── regions.py    # City resolution, PLZ grid, grid point generation
├── geo/
│   ├── __init__.py   # Country registry, region lookup, haversine
│   ├── germany.py    # DE regions, cities, Serper params
│   ├── austria.py    # AT regions, cities, Serper params
│   ├── switzerland.py # CH regions, cities, Serper params
│   └── worldwide.py  # 50+ country support via world_cities data
├── schemas/          # Pydantic request/response models
├── static/           # Frontend (single HTML file)
├── utils/
│   ├── emails.py     # Email extraction + validation (priority sorting)
│   └── rate_limiter.py # Token-bucket rate limiter
├── categories.py     # 34 pre-built category bundles (20 DE + 14 EN)
├── config.py         # Pydantic settings (env vars)
└── main.py           # FastAPI app, lifespan, orphaned job recovery
data/
├── cities_de.txt     # German cities with coordinates + population
├── cities_at.txt     # Austrian cities
├── cities_ch.txt     # Swiss cities
├── plz_germany.csv   # German postal code grid (Max mode)
├── plz_austria.csv   # Austrian postal code grid
├── plz_switzerland.csv # Swiss postal code grid
└── world_cities*.csv # Worldwide city data (5k/15k/full)
migrations/           # Incremental SQL migrations for Supabase
```

## Database Schema

Three core tables in Supabase (PostgreSQL):

- **`scraper_leads`** — all scraped leads with contact info, quality scores, and parsed address fields
- **`scrape_jobs`** — job metadata, status, progress counters, and targeting config
- **`job_leads`** — many-to-many membership linking jobs to leads (enables per-job exports without overwriting)
- **`custom_bundles`** — user-saved category bundles

## Credits

Each Serper `/maps` API call costs **3 credits**. Each `/search` call (SERP discovery) costs **1 credit**.

Typical usage:
- Smart mode, single country, 1 search term: ~500-2,000 credits
- Category bundle (15 terms), single country, Smart mode: ~10,000-30,000 credits
- Category bundle (30 terms), DACH, Max mode: ~2,500,000 credits
- Use the **Estimate** button before starting to check costs
