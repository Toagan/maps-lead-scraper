"""Async Serper Maps API client."""

from __future__ import annotations

import asyncio
import logging

import aiohttp

from app.config import settings
from app.utils.rate_limiter import TokenBucket

logger = logging.getLogger(__name__)

_rate_limiter = TokenBucket(rate=settings.serper_max_rps)
_semaphore = asyncio.Semaphore(settings.serper_max_concurrent)

SERPER_URL = "https://google.serper.dev/maps"
SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Shared session for connection reuse (lazy init)
_session: aiohttp.ClientSession | None = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None


async def search_maps(
    query: str,
    gl: str,
    hl: str,
    lat: float,
    lon: float,
    zoom: int = 16,
    start: int = 0,
) -> dict | None:
    """
    Call Serper /maps endpoint.

    /maps returns 20 results per page and respects the ll coordinate for
    proximity-based ranking (unlike /places which ignores coordinates).
    Costs 3 credits per call vs 1 for /places, but yields 10-15x more
    unique results when combined with grid search.
    """
    payload = {
        "q": query,
        "gl": gl,
        "hl": hl,
        "ll": f"@{lat},{lon},{zoom}z",
        "start": start,
    }
    headers = {
        "X-API-KEY": settings.serper_api_key,
        "Content-Type": "application/json",
    }

    for attempt in range(3):
        await _rate_limiter.acquire()
        async with _semaphore:
            try:
                session = await _get_session()
                async with session.post(
                    SERPER_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status == 429:
                        wait = 2 ** attempt
                        logger.warning("Serper 429 — retrying in %ss", wait)
                        await asyncio.sleep(wait)
                        continue
                    body = await resp.text()
                    logger.error("Serper %s: %s", resp.status, body[:200])
                    return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("Serper request error (attempt %d): %s", attempt + 1, exc)
                if attempt < 2:
                    await asyncio.sleep(1)
    return None


async def search_web(
    query: str,
    gl: str,
    hl: str,
    num: int = 3,
) -> list[dict]:
    """
    Call Serper /search endpoint for organic web results.

    Returns a list of {title, link, snippet} dicts.
    Costs 1 credit per call.
    """
    payload = {
        "q": query,
        "gl": gl,
        "hl": hl,
        "num": num,
    }
    headers = {
        "X-API-KEY": settings.serper_api_key,
        "Content-Type": "application/json",
    }

    for attempt in range(3):
        await _rate_limiter.acquire()
        async with _semaphore:
            try:
                session = await _get_session()
                async with session.post(
                    SERPER_SEARCH_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("organic", [])
                    if resp.status == 429:
                        wait = 2 ** attempt
                        logger.warning("Serper search 429 — retrying in %ss", wait)
                        await asyncio.sleep(wait)
                        continue
                    body = await resp.text()
                    logger.error("Serper search %s: %s", resp.status, body[:200])
                    return []
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("Serper search error (attempt %d): %s", attempt + 1, exc)
                if attempt < 2:
                    await asyncio.sleep(1)
    return []


def compute_category_relevance(search_term: str, category: str, categories_str: str) -> float:
    """Score 0.0–1.0 how well the result's category matches the search term.

    Uses lightweight substring matching that works across German/English
    without needing a mapping table.  Never used to drop results — only
    to score them so users can filter at export time.
    """
    if not category and not categories_str:
        return 0.5  # no data — keep neutral

    search_lower = search_term.lower().strip()
    cat_lower = (category or "").lower()
    # types list comes as comma-separated string; normalise underscores
    cats_lower = (categories_str or "").lower().replace("_", " ")
    combined = f"{cat_lower} {cats_lower}"

    # Direct substring: "Fitnessstudio" in "Fitnessstudio" or vice-versa
    if search_lower in cat_lower or cat_lower in search_lower:
        return 1.0

    # Any significant search word appears in the category fields
    for word in search_lower.split():
        if len(word) >= 3 and word in combined:
            return 0.9

    # Reverse check: any category word appears inside the search term.
    # Catches German/English overlaps like "fitness" (from types) inside
    # "fitnessstudio" (German search term).
    for word in combined.replace(",", " ").split():
        if len(word) >= 4 and word in search_lower:
            return 0.8

    # Stem prefix match: "Steuerberater" ↔ "Steuerberatung" share prefix "steuerber"
    # Check if the search term and any category word share a common prefix >= 6 chars.
    for word in combined.replace(",", " ").split():
        if len(word) >= 6:
            prefix_len = min(len(search_lower), len(word))
            common = 0
            for i in range(prefix_len):
                if search_lower[i] == word[i]:
                    common += 1
                else:
                    break
            if common >= 6:
                return 0.75

    # Category exists but zero overlap with search term
    if category:
        return 0.3

    return 0.5


_CLOSED_INDICATORS = {
    "permanently closed", "dauerhaft geschlossen", "définitivement fermé",
    "chiuso definitivamente", "permanently_closed",
}


def is_place_closed(place: dict) -> bool:
    """Detect permanently closed businesses from Serper response fields."""
    # Serper may include a businessStatus field
    status = (place.get("businessStatus") or place.get("business_status") or "").lower()
    if "closed" in status:
        return True
    # Check title and description for closure indicators
    title = (place.get("title") or "").lower()
    desc = (place.get("description") or place.get("snippet") or "").lower()
    for indicator in _CLOSED_INDICATORS:
        if indicator in title or indicator in desc:
            return True
    return False


def parse_dach_address(address: str) -> dict:
    """Parse a DACH-format address into structured components.

    Handles patterns like:
      "Hauptstraße 15, 80331 München"
      "Bahnhofstr. 3a, 8001 Zürich, Schweiz"
      "Musterweg 7, 1010 Wien, Österreich"
    """
    import re
    result = {"street": None, "postal_code": None, "city_parsed": None}
    if not address:
        return result

    # Strip trailing country names
    cleaned = re.sub(r',?\s*(Deutschland|Germany|Österreich|Austria|Schweiz|Switzerland|Suisse)$', '', address, flags=re.IGNORECASE).strip()

    # Pattern: Street ..., PLZ City
    m = re.match(r'^(.+?),\s*(\d{4,5})\s+(.+?)$', cleaned)
    if m:
        result["street"] = m.group(1).strip()
        result["postal_code"] = m.group(2).strip()
        # City might have trailing comma+region, take first part
        city_part = m.group(3).split(",")[0].strip()
        result["city_parsed"] = city_part
        return result

    # Fallback: just PLZ somewhere in the string
    m2 = re.search(r'\b(\d{4,5})\s+(\S+(?:\s+\S+)?)', cleaned)
    if m2:
        result["postal_code"] = m2.group(1).strip()
        result["city_parsed"] = m2.group(2).split(",")[0].strip()

    return result


def extract_place_data(place: dict, search_term: str, city_name: str) -> dict:
    """Extract normalised fields from a Serper place result. Ported from v1."""
    categories_list = place.get("types", place.get("categories", []))
    if isinstance(categories_list, list):
        categories = ", ".join(categories_list)
    else:
        categories = str(categories_list) if categories_list else ""

    hours_raw = place.get("openingHours", place.get("hours", None))
    if isinstance(hours_raw, dict):
        operating_hours = hours_raw
    elif isinstance(hours_raw, list):
        operating_hours = {"schedule": hours_raw}
    else:
        operating_hours = None

    return {
        "place_id": place.get("cid") or place.get("place_id") or place.get("placeId", ""),
        "cid": place.get("cid", ""),
        "name": place.get("title", "Unknown"),
        "address": place.get("address", ""),
        "phone": place.get("phoneNumber", place.get("phone", "")),
        "website": place.get("website", ""),
        "rating": place.get("rating"),
        "review_count": place.get("ratingCount", place.get("reviews", place.get("reviewCount"))),
        "category": place.get("category", place.get("type", "")),
        "categories": categories,
        "latitude": place.get("latitude"),
        "longitude": place.get("longitude"),
        "thumbnail_url": place.get("thumbnailUrl", ""),
        "operating_hours": operating_hours,
        "price_range": place.get("price", place.get("priceRange", "")),
        "description": place.get("description", place.get("snippet", "")),
        "search_term": search_term,
        "city": city_name,
    }
