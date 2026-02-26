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
    """Fetch all place_ids for a country, paginating past Supabase's 1000-row default."""
    if not _client:
        return set()
    try:
        ids: set[str] = set()
        page_size = 1000
        offset = 0
        while True:
            rows = (
                _client.table(LEADS_TABLE)
                .select("place_id")
                .eq("country", country)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            for r in rows.data:
                if r.get("place_id"):
                    ids.add(r["place_id"])
            if len(rows.data) < page_size:
                break
            offset += page_size
        return ids
    except Exception as exc:
        logger.error("Error fetching place_ids: %s", exc)
        return set()


def get_job_place_ids(job_id: str) -> set[str]:
    """Fetch all place_ids for a specific job (for resume dedup)."""
    if not _client:
        return set()
    try:
        ids: set[str] = set()
        page_size = 1000
        offset = 0
        while True:
            rows = (
                _client.table(LEADS_TABLE)
                .select("place_id")
                .eq("job_id", job_id)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            for r in rows.data:
                if r.get("place_id"):
                    ids.add(r["place_id"])
            if len(rows.data) < page_size:
                break
            offset += page_size
        return ids
    except Exception as exc:
        logger.error("Error fetching job place_ids: %s", exc)
        return set()


def upsert_leads(records: list[dict]) -> int:
    if not _client or not records:
        return 0
    try:
        # Include job_id in the upsert so leads always belong to the
        # latest job that found them. Each job is independent — its CSV
        # should contain every lead it discovered.
        cleaned = [{k: v for k, v in r.items() if v is not None} for r in records]
        _client.table(LEADS_TABLE).upsert(cleaned, on_conflict="place_id").execute()
        return len(cleaned)
    except Exception as exc:
        logger.error("Error upserting leads: %s", exc)
        return 0


def flag_chains(job_id: str, chain_names: set[str]) -> int:
    """Mark leads from this job as is_chain=true if their name is in chain_names."""
    if not _client or not chain_names:
        return 0
    try:
        names_list = list(chain_names)
        _client.table(LEADS_TABLE).update(
            {"is_chain": True}
        ).eq("job_id", job_id).in_("name", names_list).execute()
        return len(names_list)
    except Exception as exc:
        logger.error("Error flagging chains: %s", exc)
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


def update_lead_serp(
    place_id: str,
    website_serp: str,
    email_serp: str | None = None,
    email_serp_source: str | None = None,
) -> bool:
    if not _client:
        return False
    try:
        fields: dict = {
            "website_serp": website_serp,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }
        if email_serp:
            fields["email_serp"] = email_serp
        if email_serp_source:
            fields["email_serp_source"] = email_serp_source
        _client.table(LEADS_TABLE).update(fields).eq("place_id", place_id).execute()
        return True
    except Exception as exc:
        logger.error("Error updating lead SERP data: %s", exc)
        return False


def get_job_categories(job_id: str) -> list[dict]:
    """Return distinct category values with counts for a given job."""
    if not _client:
        return []
    import time
    for attempt in range(3):
        try:
            all_rows = []
            page_size = 1000
            offset = 0
            while True:
                rows = (
                    _client.table(LEADS_TABLE)
                    .select("category")
                    .eq("job_id", job_id)
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                all_rows.extend(rows.data)
                if len(rows.data) < page_size:
                    break
                offset += page_size
            if all_rows or attempt == 2:
                break
            # Empty result may be transient — retry
            logger.warning("get_job_categories(%s): empty on attempt %d, retrying", job_id, attempt + 1)
            time.sleep(0.5)
        except Exception as exc:
            logger.error("Error getting job categories (attempt %d): %s", attempt + 1, exc)
            if attempt < 2:
                time.sleep(0.5)
                continue
            return []
    counts: dict[str, int] = {}
    for r in all_rows:
        cat = r.get("category") or "Uncategorized"
        counts[cat] = counts.get(cat, 0) + 1
    return sorted(
        [{"category": cat, "count": cnt} for cat, cnt in counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )


def _build_leads_query(
    country=None, region=None, category=None, categories=None,
    has_email=None, has_phone=None, has_website=None,
    search_term=None, min_relevance=None,
    job_id=None, exclude_chains=None, exclude_low_confidence=None,
    min_reviews=None,
):
    q = _client.table(LEADS_TABLE).select("*", count="exact")
    if country:
        q = q.eq("country", country)
    if region:
        q = q.eq("region", region)
    if category:
        q = q.ilike("category", f"%{category}%")
    if categories:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        if cat_list:
            q = q.in_("category", cat_list)
    if has_email:
        q = q.neq("email", None).neq("email", "")
    if has_phone:
        q = q.neq("phone", None).neq("phone", "")
    if has_website:
        q = q.neq("website", None).neq("website", "")
    if search_term:
        q = q.ilike("search_term", f"%{search_term}%")
    if min_relevance is not None:
        q = q.gte("category_relevance", min_relevance)
    if job_id:
        q = q.eq("job_id", job_id)
    if exclude_chains:
        q = q.neq("is_chain", True)
    if exclude_low_confidence:
        q = q.neq("low_confidence", True)
    if min_reviews is not None:
        q = q.gte("review_count", min_reviews)
    return q


def query_leads(
    country=None, region=None, category=None, categories=None,
    has_email=None, has_phone=None, has_website=None,
    search_term=None, min_relevance=None,
    job_id=None, exclude_chains=None, exclude_low_confidence=None,
    min_reviews=None,
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
            q = _build_leads_query(country, region, category, categories, has_email, has_phone, has_website, search_term, min_relevance, job_id, exclude_chains, exclude_low_confidence, min_reviews)
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
        job_countries: set[str] = set()
        try:
            jobs = _client.table(JOBS_TABLE).select("country").limit(500).execute()
            for r in (jobs.data or []):
                raw = r.get("country", "")
                # Multi-country jobs store "DE,AT,CH" — split into individual codes
                for part in raw.split(","):
                    part = part.strip().lower()
                    if part:
                        job_countries.add(part)
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
    job_name: str | None = None,
) -> str:
    job_id = str(uuid.uuid4())
    if not _client:
        return job_id
    try:
        row = {
            "id": job_id,
            "status": "pending",
            "search_term": search_term,
            "country": country,
            "targeting_mode": targeting_mode,
            "targeting_config": targeting_config,
            "enrich_emails": enrich_emails,
            "total_locations": total_locations,
        }
        if job_name:
            row["job_name"] = job_name
        _client.table(JOBS_TABLE).insert(row).execute()
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


def delete_job(job_id: str) -> bool:
    """Delete a job. Detaches leads (sets job_id=NULL) rather than deleting
    them, because upsert may have re-stamped leads from earlier jobs."""
    if not _client:
        return False
    try:
        # Detach leads — don't delete them, they may belong to other jobs logically
        _client.table(LEADS_TABLE).update({"job_id": None}).eq("job_id", job_id).execute()
        _client.table(JOBS_TABLE).delete().eq("id", job_id).execute()
        return True
    except Exception as exc:
        logger.error("Error deleting job %s: %s", job_id, exc)
        return False


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
