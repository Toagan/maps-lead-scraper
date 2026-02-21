"""Async website crawler for email enrichment."""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urljoin

import aiohttp

from app.config import settings
from app.services import database as db
from app.utils.emails import extract_emails

logger = logging.getLogger(__name__)

# Subpages to try if homepage has no email
SUBPAGES = [
    "/kontakt",
    "/impressum",
    "/contact",
    "/about",
    "/ueber-uns",
    "/about-us",
]

_semaphore = asyncio.Semaphore(settings.enricher_max_concurrent)
_domain_last: dict[str, float] = {}


async def _fetch_page(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=15),
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/2.0)"},
        ) as resp:
            if resp.status != 200:
                return None
            ct = resp.headers.get("Content-Type", "")
            if "text/html" not in ct:
                return None
            return await resp.text(errors="replace")
    except Exception:
        return None


async def _enrich_single(
    session: aiohttp.ClientSession,
    place_id: str,
    website: str,
) -> bool:
    """Crawl a business website and extract email. Returns True if email found."""
    async with _semaphore:
        # Domain cooldown
        from urllib.parse import urlparse
        domain = urlparse(website).netloc
        import time
        now = time.monotonic()
        last = _domain_last.get(domain, 0)
        if now - last < settings.enricher_domain_cooldown:
            await asyncio.sleep(settings.enricher_domain_cooldown - (now - last))
        _domain_last[domain] = time.monotonic()

        # Ensure URL has scheme
        url = website if website.startswith("http") else f"https://{website}"

        # Try homepage first
        html = await _fetch_page(session, url)
        if html:
            emails = extract_emails(html)
            if emails:
                db.update_lead_email(place_id, emails[0], url)
                return True

        # Try subpages
        for subpage in SUBPAGES:
            sub_url = urljoin(url + "/", subpage.lstrip("/"))
            html = await _fetch_page(session, sub_url)
            if html:
                emails = extract_emails(html)
                if emails:
                    db.update_lead_email(place_id, emails[0], sub_url)
                    return True

    return False


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

    logger.info("Job %s: enriching %d leads", job_id, len(candidates))
    enriched = 0

    async with aiohttp.ClientSession() as session:
        tasks = []
        for row in candidates:
            if cancel_event.is_set():
                break
            website = row.get("website", "")
            if not website:
                continue
            tasks.append(_enrich_single(session, row["place_id"], website))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        enriched = sum(1 for r in results if r is True)

    db.update_job(job_id, total_enriched=enriched)
    logger.info("Job %s: enriched %d leads with emails", job_id, enriched)
    return enriched
