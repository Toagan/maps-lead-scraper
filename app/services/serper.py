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

SERPER_URL = "https://google.serper.dev/places"


async def search_maps(
    query: str,
    gl: str,
    hl: str,
    lat: float,
    lon: float,
    zoom: int = 14,
    start: int = 0,
) -> dict | None:
    """
    Call Serper /places endpoint.

    Returns parsed JSON or None on failure.
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

    async with _semaphore:
        await _rate_limiter.acquire()
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
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


def extract_place_data(place: dict, search_term: str, city_name: str) -> dict:
    """Extract normalised fields from a Serper place result. Ported from v1."""
    categories_list = place.get("categories", [])
    if isinstance(categories_list, list):
        categories = ", ".join(categories_list)
    else:
        categories = str(categories_list) if categories_list else ""

    hours = place.get("openingHours", place.get("hours", ""))
    if isinstance(hours, dict):
        hours = hours.get("status", str(hours))
    elif isinstance(hours, list):
        hours = "; ".join(hours)

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
        "operating_hours": hours if isinstance(hours, dict) else None,
        "price_range": place.get("price", place.get("priceRange", "")),
        "description": place.get("description", place.get("snippet", "")),
        "search_term": search_term,
        "city": city_name,
    }
