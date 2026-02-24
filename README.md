# BlueReach Lead Scraper

Google Maps lead scraper powered by the Serper API. Scrapes business listings across 50+ countries with smart grid-based search, deduplication, and email enrichment.

Built with FastAPI + Supabase + vanilla JS frontend.

## Features

- **Multi-country scraping** — DACH (DE/AT/CH) with region targeting, or 50+ worldwide countries
- **Smart grid search** — generates coordinate grids for large cities to overcome Google's 120-result cap
- **Category bundles** — pre-built search term sets (e.g. "Baubranche" = 50 construction terms) or custom terms
- **AI-powered term generation** — GPT-4o-mini suggests search terms for any niche
- **4 scrape modes** — Quick (50k+), Smart (10k+), Thorough (5k+), Max (all locations / PLZ grid)
- **Credit management** — estimation before scraping, hard budget caps per job
- **Preview mode** — test a search with 1 API call before committing
- **Email enrichment** — crawls business websites to extract emails (OpenAI-assisted)
- **Quality flags** — closed business detection, chain detection, low-confidence flagging, category relevance scoring
- **Per-job CSV export** — download leads from individual scrape runs
- **Real-time job tracking** — progress, cancellation, auto-polling

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
migrations/001_custom_bundles.sql
migrations/002_add_category_relevance.sql
migrations/003_quality_flags_and_address.sql
migrations/004_job_name.sql
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
| `GET` | `/jobs` | List all jobs |
| `GET` | `/jobs/{id}` | Get job details |
| `POST` | `/jobs/{id}/cancel` | Cancel a running job |
| `DELETE` | `/jobs/{id}` | Delete a finished job |

### Leads
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/leads` | Query leads with filters |
| `GET` | `/leads?format=csv` | Export as CSV |

### Categories
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/categories` | List category bundles |
| `POST` | `/categories/suggest` | AI-generate search terms |
| `POST` | `/categories/save` | Save custom bundle |
| `DELETE` | `/categories/{key}` | Delete custom bundle |

### Other
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/stats` | Dashboard statistics |
| `GET` | `/regions` | DACH regions list |
| `GET` | `/worldwide-countries` | Available worldwide countries |
| `GET` | `/health` | Health check |

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `SERPER_API_KEY` | Yes | Serper.dev API key |
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_KEY` | Yes | Supabase anon/service key |
| `OPENAI_API_KEY` | No | For email enrichment & AI term generation |
| `SERPER_MAX_RPS` | No | Max Serper requests/sec (default: 50) |
| `SERPER_MAX_CONCURRENT` | No | Max concurrent Serper requests (default: 20) |

## Deployment

Includes `Dockerfile` and `railway.toml` for one-click Railway deployment. Set environment variables in Railway dashboard.

## Architecture

```
app/
├── api/           # FastAPI route handlers
│   ├── scrape.py  # /scrape, /categories endpoints
│   ├── jobs.py    # /jobs endpoints
│   ├── leads.py   # /leads endpoint + CSV export
│   └── ...
├── services/
│   ├── scraper.py   # Job orchestrator (grid search, pagination, dedup)
│   ├── serper.py    # Serper API client + relevance scoring
│   ├── database.py  # Supabase CRUD operations
│   ├── enricher.py  # Email enrichment pipeline
│   └── regions.py   # City resolution + grid point generation
├── geo/             # Country modules (DE, AT, CH, worldwide)
├── schemas/         # Pydantic request/response models
├── static/          # Frontend (single HTML file)
└── main.py          # FastAPI app + lifespan
```

## Credits

Each Serper `/maps` API call costs **3 credits**. A typical scrape:
- Smart mode, single country, 1 search term: ~500–2,000 credits
- Category bundle (40 terms), DACH, Max mode: ~2,500,000 credits
- Use the **Estimate** button before starting to check costs
