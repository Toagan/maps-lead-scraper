"""Async website crawler for email enrichment."""

from __future__ import annotations

import asyncio
import logging
import time
from urllib.parse import urljoin, urlparse

import aiohttp

from app.config import settings
from app.services import database as db
from app.utils.emails import extract_emails

logger = logging.getLogger(__name__)

# Subpages to try if homepage has no email (DE + EN)
SUBPAGES = [
    "/kontakt",
    "/impressum",
    "/ueber-uns",
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/get-in-touch",
    "/team",
]

_semaphore = asyncio.Semaphore(settings.enricher_max_concurrent)
_domain_last: dict[str, float] = {}

# Max age for domain cooldown entries (seconds) — avoids unbounded growth
_DOMAIN_COOLDOWN_MAX_SIZE = 10_000

# Retry settings for website fetching
_FETCH_MAX_RETRIES = 2
_FETCH_RETRY_DELAY = 1.0


async def _fetch_page(session: aiohttp.ClientSession, url: str) -> str | None:
    """Fetch an HTML page with retry logic."""
    for attempt in range(_FETCH_MAX_RETRIES):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=12),
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/2.0)"},
            ) as resp:
                if resp.status != 200:
                    return None
                ct = resp.headers.get("Content-Type", "")
                if "text/html" not in ct:
                    return None
                return await resp.text(errors="replace")
        except asyncio.TimeoutError:
            if attempt < _FETCH_MAX_RETRIES - 1:
                await asyncio.sleep(_FETCH_RETRY_DELAY)
                continue
            logger.debug("Timeout fetching %s", url)
            return None
        except aiohttp.ClientSSLError:
            logger.debug("SSL error fetching %s", url)
            return None
        except aiohttp.ClientError as exc:
            if attempt < _FETCH_MAX_RETRIES - 1:
                await asyncio.sleep(_FETCH_RETRY_DELAY)
                continue
            logger.debug("Client error fetching %s: %s", url, exc)
            return None
        except Exception as exc:
            logger.debug("Unexpected error fetching %s: %s", url, exc)
            return None
    return None


def _domain_cooldown(domain: str) -> float:
    """Return seconds to sleep for domain cooldown. Prunes old entries."""
    # Prune if too large
    if len(_domain_last) > _DOMAIN_COOLDOWN_MAX_SIZE:
        cutoff = time.monotonic() - 60
        stale = [k for k, v in _domain_last.items() if v < cutoff]
        for k in stale:
            del _domain_last[k]

    now = time.monotonic()
    last = _domain_last.get(domain, 0)
    delay = settings.enricher_domain_cooldown - (now - last)
    return max(0, delay)


async def _crawl_for_email(
    session: aiohttp.ClientSession,
    website: str,
) -> tuple[str | None, str | None]:
    """Crawl a website for email. Returns (email, source_url) or (None, None)."""
    url = website if website.startswith("http") else f"https://{website}"
    domain = urlparse(url).netloc

    # Domain cooldown
    delay = _domain_cooldown(domain)
    if delay > 0:
        await asyncio.sleep(delay)
    _domain_last[domain] = time.monotonic()

    # Try homepage first
    html = await _fetch_page(session, url)
    if html:
        emails = extract_emails(html)
        if emails:
            return emails[0], url

    # Try subpages
    for subpage in SUBPAGES:
        sub_url = urljoin(url + "/", subpage.lstrip("/"))
        html = await _fetch_page(session, sub_url)
        if html:
            emails = extract_emails(html)
            if emails:
                return emails[0], sub_url

    return None, None


# ---- Batched database updates ----

def _flush_email_updates(updates: list[dict]) -> None:
    """Batch-update email fields for multiple leads."""
    if not updates:
        return
    client = db.get_client()
    if not client:
        return
    for u in updates:
        try:
            client.table(db.LEADS_TABLE).update({
                "email": u["email"],
                "email_source": u["source"],
                "enriched_at": u["enriched_at"],
            }).eq("place_id", u["place_id"]).execute()
        except Exception as exc:
            logger.error("Error updating lead email for %s: %s", u["place_id"], exc)


def _flush_serp_updates(updates: list[dict]) -> None:
    """Batch-update SERP fields for multiple leads."""
    if not updates:
        return
    client = db.get_client()
    if not client:
        return
    for u in updates:
        try:
            fields: dict = {
                "website_serp": u["website"],
                "enriched_at": u["enriched_at"],
            }
            if u.get("email"):
                fields["email_serp"] = u["email"]
            if u.get("source"):
                fields["email_serp_source"] = u["source"]
            client.table(db.LEADS_TABLE).update(fields).eq("place_id", u["place_id"]).execute()
        except Exception as exc:
            logger.error("Error updating lead SERP for %s: %s", u["place_id"], exc)


# ---- Directories / aggregators to skip ----

_SERP_BLACKLIST_DOMAINS = {
    "yelp.com", "yelp.de", "gelbeseiten.de", "jameda.de", "doctolib.de",
    "doctolib.com", "sanego.de", "11880.com", "dasoertliche.de",
    "meinestadt.de", "golocal.de", "cylex.de", "branchenbuch.meinestadt.de",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "google.com", "google.de",
    "maps.google.com", "trustpilot.com", "kununu.com", "provenexpert.com",
    "wikipedia.org", "wikidata.org", "infobel.com", "hotfrog.de",
    "firmenwissen.de", "northdata.de", "unternehmensregister.de",
}


def _is_directory_url(url: str) -> bool:
    """Return True if the URL belongs to a known directory/aggregator."""
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        # Also check parent domain (e.g. business.yelp.com -> yelp.com)
        parts = domain.split(".")
        if len(parts) > 2:
            parent = ".".join(parts[-2:])
            if parent in _SERP_BLACKLIST_DOMAINS:
                return True
        return domain in _SERP_BLACKLIST_DOMAINS
    except Exception:
        return False


# Language-specific search suffixes
_SEARCH_SUFFIX = {
    "de": "kontakt",
    "at": "kontakt",
    "ch": "kontakt",
    "en": "contact",
    "us": "contact",
    "gb": "contact",
    "uk": "contact",
    "fr": "contact",
    "it": "contatto",
    "es": "contacto",
    "nl": "contact",
}


async def discover_and_enrich(
    country: str,
    job_id: str,
    cancel_event: asyncio.Event,
) -> int:
    """SERP-based discovery: find websites for leads that have none, then extract emails.

    Returns count of leads where an email was found via SERP.
    """
    client = db.get_client()
    if not client:
        return 0

    # Fetch leads with no website AND no email for this job
    try:
        result = (
            client.table(db.LEADS_TABLE)
            .select("place_id, name, city")
            .eq("job_id", job_id)
            .eq("country", country)
            .or_("website.is.null,website.eq.")
            .is_("email", "null")
            .limit(5000)
            .execute()
        )
        candidates = result.data
    except Exception as exc:
        logger.error("Error fetching SERP discovery candidates: %s", exc)
        return 0

    if not candidates:
        return 0

    logger.info("Job %s: SERP discovery for %d leads (country=%s)", job_id, len(candidates), country)

    from app.services.serper import search_web
    from app.geo.worldwide import is_worldwide, get_serper_params

    if is_worldwide(country):
        gl, hl = get_serper_params(country)
    else:
        from app.geo import get_country_module
        mod = get_country_module(country)
        gl, hl = mod.SERPER_GL, mod.SERPER_HL

    suffix = _SEARCH_SUFFIX.get(country.lower(), "contact")
    enriched = 0
    serp_buffer: list[dict] = []
    BUFFER_SIZE = 50

    async with aiohttp.ClientSession() as session:
        for i, row in enumerate(candidates):
            if cancel_event.is_set():
                break

            name = row.get("name", "")
            city = row.get("city", "")
            if not name:
                continue

            query = f"{name} {city} {suffix}"
            results = await search_web(query, gl=gl, hl=hl, num=5)

            # Pick first non-directory result
            website_url = None
            for r in results:
                link = r.get("link", "")
                if link and not _is_directory_url(link):
                    website_url = link
                    break

            if not website_url:
                continue

            async with _semaphore:
                email, source = await _crawl_for_email(session, website_url)

            from datetime import datetime, timezone
            update = {
                "place_id": row["place_id"],
                "website": website_url,
                "email": email,
                "source": source,
                "enriched_at": datetime.now(timezone.utc).isoformat(),
            }
            serp_buffer.append(update)
            if email:
                enriched += 1

            # Flush buffer periodically
            if len(serp_buffer) >= BUFFER_SIZE:
                await asyncio.to_thread(_flush_serp_updates, serp_buffer)
                serp_buffer = []
                logger.info("Job %s: SERP discovery progress %d/%d, found %d emails",
                            job_id, i + 1, len(candidates), enriched)

    # Flush remaining
    if serp_buffer:
        await asyncio.to_thread(_flush_serp_updates, serp_buffer)

    logger.info("Job %s: SERP discovery done — %d/%d emails found (country=%s)",
                job_id, enriched, len(candidates), country)
    return enriched


async def enrich_leads(
    country: str,
    job_id: str,
    cancel_event: asyncio.Event,
) -> int:
    """Enrich all leads that have a website but no email. Returns count enriched."""
    client = db.get_client()
    if not client:
        return 0

    # Fetch leads needing enrichment
    try:
        result = (
            client.table(db.LEADS_TABLE)
            .select("place_id, website")
            .eq("country", country)
            .neq("website", "")
            .is_("email", "null")
            .limit(5000)
            .execute()
        )
        candidates = result.data
    except Exception as exc:
        logger.error("Error fetching enrichment candidates: %s", exc)
        return 0

    if not candidates:
        return 0

    logger.info("Job %s: enriching %d leads (country=%s)", job_id, len(candidates), country)
    enriched = 0
    email_buffer: list[dict] = []
    BUFFER_SIZE = 50

    async with aiohttp.ClientSession() as session:
        for i, row in enumerate(candidates):
            if cancel_event.is_set():
                break

            website = row.get("website", "")
            if not website:
                continue

            async with _semaphore:
                email, source = await _crawl_for_email(session, website)

            if email:
                from datetime import datetime, timezone
                email_buffer.append({
                    "place_id": row["place_id"],
                    "email": email,
                    "source": source,
                    "enriched_at": datetime.now(timezone.utc).isoformat(),
                })
                enriched += 1

            # Flush buffer periodically
            if len(email_buffer) >= BUFFER_SIZE:
                await asyncio.to_thread(_flush_email_updates, email_buffer)
                email_buffer = []
                logger.info("Job %s: enrichment progress %d/%d, found %d emails",
                            job_id, i + 1, len(candidates), enriched)

    # Flush remaining
    if email_buffer:
        await asyncio.to_thread(_flush_email_updates, email_buffer)

    db.update_job(job_id, total_enriched=enriched)
    logger.info("Job %s: enriched %d/%d leads with emails (country=%s)",
                job_id, enriched, len(candidates), country)
    return enriched
