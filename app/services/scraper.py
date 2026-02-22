"""Job orchestrator — runs a scrape job asynchronously."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List

from app.geo import get_region, get_country_module, haversine_km
from app.geo.worldwide import is_worldwide, get_serper_params
from app.services import database as db
from app.services.serper import search_maps, extract_place_data
from app.services.regions import (
    City,
    get_city_scrape_config,
    get_worldwide_scrape_config,
    generate_grid_points,
)

logger = logging.getLogger(__name__)

# In-memory registry of running jobs so we can cancel them
_running_jobs: dict[str, asyncio.Event] = {}

# Max distance (km) a result may be from the search point before we discard it.
# Google Maps can return results well outside the visible area.
_MAX_RESULT_DISTANCE_KM = 25.0


def is_job_running(job_id: str) -> bool:
    return job_id in _running_jobs


def cancel_job(job_id: str) -> bool:
    event = _running_jobs.get(job_id)
    if event:
        event.set()
        return True
    return False


def _result_within_bounds(
    result_lat: float | None,
    result_lon: float | None,
    search_lat: float,
    search_lon: float,
    max_km: float = _MAX_RESULT_DISTANCE_KM,
) -> bool:
    """Return True if the result coordinates are within max_km of the search point."""
    if result_lat is None or result_lon is None:
        # No coordinates returned — keep the result rather than discard
        return True
    return haversine_km(search_lat, search_lon, result_lat, result_lon) <= max_km


async def run_job(
    job_id: str,
    search_queries: List[str],
    country: str,
    cities: List[City],
    enrich_emails: bool = False,
    scrape_mode: str = "smart",
) -> None:
    """
    Main scraping loop executed as a background task.

    search_queries: list of search terms (1 for single, 15+ for category bundle).
    All queries share a single seen_ids set for global deduplication.

    For cities with population >= 100k, a grid of coordinate points is generated
    around the city center to overcome Google's proximity bias and 120-result cap.
    """
    cancel_event = asyncio.Event()
    _running_jobs[job_id] = cancel_event

    _worldwide = is_worldwide(country)
    if _worldwide:
        gl, hl = get_serper_params(country)
    else:
        mod = get_country_module(country)
        gl, hl = mod.SERPER_GL, mod.SERPER_HL

    # Mark job as running
    db.update_job(
        job_id,
        status="running",
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    # Load existing place_ids for dedup (shared across ALL queries)
    seen_ids = db.get_existing_place_ids(country)
    logger.info("Job %s: loaded %d existing place_ids, %d queries, %d cities",
                job_id, len(seen_ids), len(search_queries), len(cities))

    total_leads = 0
    total_dupes = 0
    total_api_calls = 0
    leads_buffer: list[dict] = []

    # Pre-compute grid points per city so we can calculate accurate total_steps
    city_grids: list[list[tuple[float, float]]] = []
    for city in cities:
        if _worldwide:
            # Worldwide cities: single center point
            city_grids.append([(city.lat, city.lon)])
        else:
            grid = generate_grid_points(city)
            city_grids.append([(gp.lat, gp.lon) for gp in grid])

    total_steps = sum(len(g) for g in city_grids) * len(search_queries)
    current_step = 0

    try:
        for q_idx, search_term in enumerate(search_queries):
            if cancel_event.is_set():
                break
            logger.info("Job %s: query %d/%d — %s",
                        job_id, q_idx + 1, len(search_queries), search_term)

            for city_idx, city in enumerate(cities):
                if cancel_event.is_set():
                    break

                grid_points = city_grids[city_idx]

                if _worldwide:
                    zoom, max_pages = get_worldwide_scrape_config()
                    region_code = None
                else:
                    zoom, max_pages = get_city_scrape_config(city.population)
                    region_code = get_region(city.lat, city.lon, country)

                for gp_lat, gp_lon in grid_points:
                    if cancel_event.is_set():
                        break

                    current_step += 1
                    query = f"{search_term} in {city.name}"

                    for page in range(max_pages):
                        if cancel_event.is_set():
                            break

                        data = await search_maps(
                            query=query,
                            gl=gl,
                            hl=hl,
                            lat=gp_lat,
                            lon=gp_lon,
                            zoom=zoom,
                            start=page * 20,
                        )
                        total_api_calls += 1

                        if not data or "places" not in data or not data["places"]:
                            break

                        new_on_page = 0
                        for place in data["places"]:
                            pdata = extract_place_data(place, search_term, city.name)
                            pid = pdata["place_id"]
                            if not pid:
                                continue
                            if pid in seen_ids:
                                total_dupes += 1
                                continue

                            # Bounding-box validation: discard results too far
                            # from the grid search point
                            if not _result_within_bounds(
                                pdata.get("latitude"),
                                pdata.get("longitude"),
                                gp_lat,
                                gp_lon,
                            ):
                                total_dupes += 1
                                continue

                            seen_ids.add(pid)
                            new_on_page += 1
                            total_leads += 1

                            # Build DB record
                            record = {
                                "place_id": pid,
                                "cid": pdata.get("cid") or None,
                                "name": pdata["name"],
                                "address": pdata.get("address") or None,
                                "phone": pdata.get("phone") or None,
                                "website": pdata.get("website") or None,
                                "rating": pdata.get("rating"),
                                "review_count": pdata.get("review_count"),
                                "category": pdata.get("category") or None,
                                "categories": pdata.get("categories") or None,
                                "latitude": pdata.get("latitude"),
                                "longitude": pdata.get("longitude"),
                                "thumbnail_url": pdata.get("thumbnail_url") or None,
                                "operating_hours": pdata.get("operating_hours"),
                                "price_range": pdata.get("price_range") or None,
                                "description": pdata.get("description") or None,
                                "country": country,
                                "region": region_code,
                                "city": city.name,
                                "search_term": search_term,
                            }
                            leads_buffer.append(record)

                            # Batch upsert every 50
                            if len(leads_buffer) >= 50:
                                db.upsert_leads(leads_buffer)
                                leads_buffer = []

                        # Early break if no new results on page
                        if new_on_page == 0:
                            break

                # Update job progress periodically (after all grid points for a city)
                db.update_job(
                    job_id,
                    processed_locations=current_step,
                    total_locations=total_steps,
                    total_leads=total_leads,
                    total_duplicates=total_dupes,
                    total_api_calls=total_api_calls,
                )

        # Flush remaining leads
        if leads_buffer:
            db.upsert_leads(leads_buffer)
            leads_buffer = []

        # Email enrichment pass
        enriched = 0
        if enrich_emails and not cancel_event.is_set():
            from app.services.enricher import enrich_leads
            enriched = await enrich_leads(country, job_id, cancel_event)

        # Mark completed
        status = "cancelled" if cancel_event.is_set() else "completed"
        db.update_job(
            job_id,
            status=status,
            total_leads=total_leads,
            total_duplicates=total_dupes,
            total_api_calls=total_api_calls,
            total_enriched=enriched,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Job %s %s: %d leads, %d dupes, %d API calls",
                     job_id, status, total_leads, total_dupes, total_api_calls)

    except Exception as exc:
        logger.exception("Job %s failed: %s", job_id, exc)
        db.update_job(
            job_id,
            status="failed",
            error_message=str(exc),
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
    finally:
        _running_jobs.pop(job_id, None)
