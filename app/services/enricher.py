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

# Subpages to try if homepage has no email (DE + EN)
SUBPAGES = [
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/get-in-touch",
    "/team",
    "/kontakt",
    "/impressum",
    "/ueber-uns",
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


# Directories / aggregators to skip when picking SERP results
_SERP_BLACKLIST_DOMAINS = {
    "yelp.com", "yelp.de", "gelbeseiten.de", "jameda.de", "doctolib.de",
    "doctolib.com", "sanego.de", "11880.com", "dasoertliche.de",
    "meinestadt.de", "golocal.de", "cylex.de", "branchenbuch.meinestadt.de",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "google.com", "google.de",
    "maps.google.com", "trustpilot.com", "kununu.com", "provenexpert.com",
    "wikipedia.org", "wikidata.org",
}


def _is_directory_url(url: str) -> bool:
    """Return True if the URL belongs to a known directory/aggregator."""
    from urllib.parse import urlparse
    try:
        domain = urlparse(url).netloc.lower()
        # Strip www.
        if domain.startswith("www."):
            domain = domain[4:]
        return domain in _SERP_BLACKLIST_DOMAINS
    except Exception:
        return False


async def _enrich_single_serp(
    session: aiohttp.ClientSession,
    place_id: str,
    website: str,
) -> bool:
    """Crawl a SERP-discovered website and store email in serp columns. Returns True if email found."""
    async with _semaphore:
        from urllib.parse import urlparse
        import time
        domain = urlparse(website).netloc
        now = time.monotonic()
        last = _domain_last.get(domain, 0)
        if now - last < settings.enricher_domain_cooldown:
            await asyncio.sleep(settings.enricher_domain_cooldown - (now - last))
        _domain_last[domain] = time.monotonic()

        url = website if website.startswith("http") else f"https://{website}"

        # Try homepage first
        html = await _fetch_page(session, url)
        if html:
            emails = extract_emails(html)
            if emails:
                db.update_lead_serp(place_id, website, emails[0], url)
                return True

        # Try subpages
        for subpage in SUBPAGES:
            sub_url = urljoin(url + "/", subpage.lstrip("/"))
            html = await _fetch_page(session, sub_url)
            if html:
                emails = extract_emails(html)
                if emails:
                    db.update_lead_serp(place_id, website, emails[0], sub_url)
                    return True

    # No email found, but still store the discovered website
    db.update_lead_serp(place_id, website)
    return False


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

    enriched = 0

    async with aiohttp.ClientSession() as session:
        for row in candidates:
            if cancel_event.is_set():
                break

            name = row.get("name", "")
            city = row.get("city", "")
            if not name:
                continue

            query = f"{name} {city} kontakt"
            results = await search_web(query, gl=gl, hl=hl, num=3)

            # Pick first non-directory result
            website_url = None
            for r in results:
                link = r.get("link", "")
                if link and not _is_directory_url(link):
                    website_url = link
                    break

            if not website_url:
                continue

            found = await _enrich_single_serp(session, row["place_id"], website_url)
            if found:
                enriched += 1

    logger.info("Job %s: SERP discovery found %d emails (country=%s)", job_id, enriched, country)
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
