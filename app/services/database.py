"""Supabase CRUD — leads upsert, jobs CRUD, stats queries."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from supabase import create_client, Client

from app.config import settings

logger = logging.getLogger(__name__)

_client: Client | None = None

LEADS_TABLE = "scraper_leads"
JOBS_TABLE = "scrape_jobs"
BUNDLES_TABLE = "custom_bundles"


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def init_supabase() -> None:
    global _client
    if settings.supabase_url and settings.supabase_key:
        try:
            _client = create_client(settings.supabase_url, settings.supabase_key)
            logger.info("Supabase connected")
        except Exception as exc:
            logger.error("Supabase connection failed: %s", exc)
    else:
        logger.warning("Supabase credentials not configured — running in local-only mode")


def close_supabase() -> None:
    global _client
    _client = None


def get_client() -> Client | None:
    return _client


# ---------------------------------------------------------------------------
# Leads
# ---------------------------------------------------------------------------

def get_existing_place_ids(country: str) -> set[str]:
    if not _client:
        return set()
    try:
        rows = _client.table(LEADS_TABLE).select("place_id").eq("country", country).execute()
        return {r["place_id"] for r in rows.data if r.get("place_id")}
    except Exception as exc:
        logger.error("Error fetching place_ids: %s", exc)
        return set()


def upsert_leads(records: list[dict]) -> int:
    if not _client or not records:
        return 0
    try:
        # Remove None values from each record
        cleaned = [{k: v for k, v in r.items() if v is not None} for r in records]
        _client.table(LEADS_TABLE).upsert(cleaned, on_conflict="place_id").execute()
        return len(cleaned)
    except Exception as exc:
        logger.error("Error upserting leads: %s", exc)
        return 0


def update_lead_email(place_id: str, email: str, source: str) -> bool:
    if not _client:
        return False
    try:
        _client.table(LEADS_TABLE).update({
            "email": email,
            "email_source": source,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }).eq("place_id", place_id).execute()
        return True
    except Exception as exc:
        logger.error("Error updating lead email: %s", exc)
        return False


def _build_leads_query(
    country=None, region=None, category=None,
    has_email=None, has_phone=None, has_website=None,
    search_term=None,
):
    q = _client.table(LEADS_TABLE).select("*", count="exact")
    if country:
        q = q.eq("country", country)
    if region:
        q = q.eq("region", region)
    if category:
        q = q.ilike("category", f"%{category}%")
    if has_email:
        q = q.neq("email", None).neq("email", "")
    if has_phone:
        q = q.neq("phone", None).neq("phone", "")
    if has_website:
        q = q.neq("website", None).neq("website", "")
    if search_term:
        q = q.ilike("search_term", f"%{search_term}%")
    return q


def query_leads(
    country=None, region=None, category=None,
    has_email=None, has_phone=None, has_website=None,
    search_term=None,
    limit: int = 100, offset: int = 0,
) -> tuple:
    if not _client:
        return [], 0
    try:
        # Supabase caps at 1000 rows per request — paginate if needed
        all_rows = []
        page_size = min(limit, 1000)
        fetched = 0
        total = 0
        while fetched < limit:
            q = _build_leads_query(country, region, category, has_email, has_phone, has_website, search_term)
            q = q.range(offset + fetched, offset + fetched + page_size - 1)
            result = q.execute()
            if total == 0:
                total = result.count if result.count is not None else len(result.data)
            if not result.data:
                break
            all_rows.extend(result.data)
            fetched += len(result.data)
            if len(result.data) < page_size:
                break
        return all_rows, total
    except Exception as exc:
        logger.error("Error querying leads: %s", exc)
        return [], 0


def get_stats() -> dict:
    if not _client:
        return {}
    try:
        total_q = _client.table(LEADS_TABLE).select("*", count="exact").execute()
        total = total_q.count or 0

        with_email_q = _client.table(LEADS_TABLE).select("*", count="exact").neq("email", None).neq("email", "").execute()
        with_email = with_email_q.count or 0

        with_phone_q = _client.table(LEADS_TABLE).select("*", count="exact").neq("phone", None).neq("phone", "").execute()
        with_phone = with_phone_q.count or 0

        with_website_q = _client.table(LEADS_TABLE).select("*", count="exact").neq("website", None).neq("website", "").execute()
        with_website = with_website_q.count or 0

        # Per-country counts — discover countries from jobs table
        by_country = {}
        job_countries = set()
        try:
            jobs = _client.table(JOBS_TABLE).select("country").limit(500).execute()
            job_countries = set(r["country"] for r in (jobs.data or []) if r.get("country"))
        except Exception:
            pass
        for cc in sorted({"de", "at", "ch"} | job_countries):
            cq = _client.table(LEADS_TABLE).select("*", count="exact").eq("country", cc).execute()
            cnt = cq.count or 0
            if cnt > 0:
                by_country[cc] = cnt

        return {
            "total_leads": total,
            "with_email": with_email,
            "with_phone": with_phone,
            "with_website": with_website,
            "by_country": by_country,
        }
    except Exception as exc:
        logger.error("Error getting stats: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Custom Bundles
# ---------------------------------------------------------------------------

def save_custom_bundle(key: str, name: str, queries: list[str]) -> bool:
    if not _client:
        return False
    try:
        _client.table(BUNDLES_TABLE).upsert(
            {"key": key, "name": name, "queries": queries},
            on_conflict="key",
        ).execute()
        return True
    except Exception as exc:
        logger.error("Error saving custom bundle: %s", exc)
        return False


def list_custom_bundles() -> list[dict]:
    if not _client:
        return []
    try:
        result = (
            _client.table(BUNDLES_TABLE)
            .select("key, name, queries")
            .order("created_at", desc=False)
            .execute()
        )
        return result.data
    except Exception as exc:
        logger.error("Error listing custom bundles: %s", exc)
        return []


def delete_custom_bundle(key: str) -> bool:
    if not _client:
        return False
    try:
        _client.table(BUNDLES_TABLE).delete().eq("key", key).execute()
        return True
    except Exception as exc:
        logger.error("Error deleting custom bundle: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def create_job(
    search_term: str,
    country: str,
    targeting_mode: str,
    targeting_config: dict,
    enrich_emails: bool,
    total_locations: int,
) -> str:
    job_id = str(uuid.uuid4())
    if not _client:
        return job_id
    try:
        _client.table(JOBS_TABLE).insert({
            "id": job_id,
            "status": "pending",
            "search_term": search_term,
            "country": country,
            "targeting_mode": targeting_mode,
            "targeting_config": targeting_config,
            "enrich_emails": enrich_emails,
            "total_locations": total_locations,
        }).execute()
    except Exception as exc:
        logger.error("Error creating job: %s", exc)
    return job_id


def update_job(job_id: str, **fields) -> None:
    if not _client:
        return
    try:
        _client.table(JOBS_TABLE).update(fields).eq("id", job_id).execute()
    except Exception as exc:
        logger.error("Error updating job %s: %s", job_id, exc)


def get_job(job_id: str) -> dict | None:
    if not _client:
        return None
    try:
        result = _client.table(JOBS_TABLE).select("*").eq("id", job_id).execute()
        return result.data[0] if result.data else None
    except Exception as exc:
        logger.error("Error getting job %s: %s", job_id, exc)
        return None


def list_jobs(limit: int = 50, offset: int = 0) -> list[dict]:
    if not _client:
        return []
    try:
        result = (
            _client.table(JOBS_TABLE)
            .select("*")
            .order("created_at", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        return result.data
    except Exception as exc:
        logger.error("Error listing jobs: %s", exc)
        return []
