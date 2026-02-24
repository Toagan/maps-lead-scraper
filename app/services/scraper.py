"""Job orchestrator — runs a scrape job asynchronously."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List

from app.geo import get_region, get_country_module, haversine_km
from app.geo.worldwide import is_worldwide, get_serper_params
from app.services import database as db
from app.services.serper import (
    search_maps, extract_place_data, compute_category_relevance,
    is_place_closed, parse_dach_address,
)
from app.services.regions import (
    City,
    get_city_scrape_config,
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


def estimate_credits(cities: List[City], search_queries: List[str]) -> dict:
    """Estimate API credits before starting a run."""
    total_grid_points = 0
    for city in cities:
        grid = generate_grid_points(city)
        total_grid_points += len(grid)

    total_steps = total_grid_points * len(search_queries)
    # Average pages per step: grid cities ~2, single-point ~3
    avg_pages = 2.5
    estimated_calls = int(total_steps * avg_pages)
    credits_per_call = 3  # Serper /maps = 3 credits
    estimated_credits = estimated_calls * credits_per_call

    return {
        "total_grid_points": total_grid_points,
        "total_steps": total_steps,
        "estimated_api_calls": estimated_calls,
        "estimated_credits": estimated_credits,
        "search_queries": len(search_queries),
        "cities": len(cities),
    }


async def preview_search(
    search_term: str,
    country: str,
    city: City,
) -> dict:
    """Run a single-page test search and return relevance analysis."""
    from app.geo.worldwide import is_worldwide as _is_ww, get_serper_params as _get_sp

    if _is_ww(country):
        gl, hl = _get_sp(country)
    else:
        mod = get_country_module(country)
        gl, hl = mod.SERPER_GL, mod.SERPER_HL

    query = f"{search_term} in {city.name}"
    data = await search_maps(
        query=query, gl=gl, hl=hl,
        lat=city.lat, lon=city.lon, zoom=16, start=0,
    )

    if not data or "places" not in data or not data["places"]:
        return {"query": query, "total": 0, "matching": 0, "results": []}

    results = []
    matching = 0
    for place in data["places"]:
        if is_place_closed(place):
            continue
        pdata = extract_place_data(place, search_term, city.name)
        relevance = compute_category_relevance(
            search_term,
            pdata.get("category", ""),
            pdata.get("categories", ""),
        )
        if relevance >= 0.5:
            matching += 1
        results.append({
            "name": pdata["name"],
            "category": pdata.get("category", ""),
            "categories": pdata.get("categories", ""),
            "relevance": relevance,
            "website": pdata.get("website", ""),
            "rating": pdata.get("rating"),
            "review_count": pdata.get("review_count"),
        })

    return {
        "query": query,
        "total": len(results),
        "matching": matching,
        "match_rate": f"{matching}/{len(results)}",
        "results": results,
    }


_CREDITS_PER_CALL = 3  # Serper /maps = 3 credits per API call


async def run_job(
    job_id: str,
    search_queries: List[str],
    country: str,
    cities: List[City],
    enrich_emails: bool = False,
    serp_discovery: bool = False,
    scrape_mode: str = "smart",
    credit_limit: int | None = None,
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

    # Cache gl/hl per country code (multi-country jobs have mixed cities)
    _gl_hl_cache: dict[str, tuple[str, str]] = {}

    def _get_gl_hl(cc: str) -> tuple[str, str]:
        if cc not in _gl_hl_cache:
            if is_worldwide(cc):
                _gl_hl_cache[cc] = get_serper_params(cc)
            else:
                mod = get_country_module(cc)
                _gl_hl_cache[cc] = (mod.SERPER_GL, mod.SERPER_HL)
        return _gl_hl_cache[cc]

    # Mark job as running
    db.update_job(
        job_id,
        status="running",
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    # Job-local seen set: dedup within this job only
    seen_countries = {c.country or country for c in cities}
    seen_ids: set[str] = set()
    logger.info("Job %s: %d queries, %d cities",
                job_id, len(search_queries), len(cities))

    total_leads = 0
    total_dupes = 0
    total_api_calls = 0
    total_closed_skipped = 0
    saturated_points = 0
    leads_buffer: list[dict] = []
    # Track business name frequencies for chain detection
    name_counts: dict[str, int] = {}

    # Pre-compute grid points per city so we can calculate accurate total_steps
    city_grids: list[list[tuple[float, float]]] = []
    for city in cities:
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

                zoom, max_pages = get_city_scrape_config(city.population)
                city_country = city.country or country
                city_ww = is_worldwide(city_country)
                region_code = None if city_ww else get_region(city.lat, city.lon, city_country)
                gl, hl = _get_gl_hl(city_country)

                for gp_lat, gp_lon in grid_points:
                    if cancel_event.is_set():
                        break

                    current_step += 1
                    query = f"{search_term} in {city.name}"
                    last_page_count = 0

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

                        # Credit budget check
                        if credit_limit and total_api_calls * _CREDITS_PER_CALL >= credit_limit:
                            logger.info("Job %s: credit limit reached (%d/%d)",
                                        job_id, total_api_calls * _CREDITS_PER_CALL, credit_limit)
                            cancel_event.set()

                        if not data or "places" not in data or not data["places"]:
                            break

                        places = data["places"]
                        new_on_page = 0
                        last_page_count = len(places)
                        for place in places:
                            # Skip permanently closed businesses
                            if is_place_closed(place):
                                total_closed_skipped += 1
                                continue

                            pdata = extract_place_data(place, search_term, city.name)
                            pid = pdata["place_id"]
                            if not pid:
                                continue
                            # Already processed in THIS job — true duplicate
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

                            # Category relevance scoring
                            relevance = compute_category_relevance(
                                search_term,
                                pdata.get("category", ""),
                                pdata.get("categories", ""),
                            )

                            # Quality flags
                            review_count = pdata.get("review_count") or 0
                            low_confidence = review_count <= 2

                            # Track name frequency for chain detection
                            biz_name = pdata["name"]
                            name_counts[biz_name] = name_counts.get(biz_name, 0) + 1

                            # Parse address into structured components (DACH)
                            addr_parts = parse_dach_address(pdata.get("address") or "")

                            # Build DB record
                            record = {
                                "place_id": pid,
                                "cid": pdata.get("cid") or None,
                                "name": biz_name,
                                "address": pdata.get("address") or None,
                                "street": addr_parts["street"],
                                "postal_code": addr_parts["postal_code"],
                                "city_parsed": addr_parts["city_parsed"],
                                "phone": pdata.get("phone") or None,
                                "website": pdata.get("website") or None,
                                "rating": pdata.get("rating"),
                                "review_count": review_count,
                                "category": pdata.get("category") or None,
                                "categories": pdata.get("categories") or None,
                                "latitude": pdata.get("latitude"),
                                "longitude": pdata.get("longitude"),
                                "thumbnail_url": pdata.get("thumbnail_url") or None,
                                "operating_hours": pdata.get("operating_hours"),
                                "price_range": pdata.get("price_range") or None,
                                "description": pdata.get("description") or None,
                                "country": city_country,
                                "region": region_code,
                                "city": city.name,
                                "search_term": search_term,
                                "category_relevance": relevance,
                                "low_confidence": low_confidence,
                                "job_id": job_id,
                            }
                            leads_buffer.append(record)

                            # Batch upsert every 50
                            if len(leads_buffer) >= 50:
                                db.upsert_leads(leads_buffer)
                                leads_buffer = []

                        # Partial page = last page (Google has no more results)
                        if last_page_count < 20:
                            break

                        # Adaptive duplicate threshold: stop paginating when
                        # <25% of the page is new (>75% dupes/existing).
                        # Saves API credits on diminishing returns.
                        if new_on_page < last_page_count * 0.25:
                            break

                    # Saturation detection: if last page of this grid point
                    # was full AND we hit max_pages, flag it
                    if page + 1 >= max_pages and last_page_count >= 20:
                        saturated_points += 1

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

        # Chain detection: flag businesses whose name appeared 5+ times
        chain_names = {n for n, c in name_counts.items() if c >= 5}
        if chain_names:
            db.flag_chains(job_id, chain_names)
            logger.info("Job %s: flagged %d chain names (%d leads)",
                        job_id, len(chain_names),
                        sum(c for n, c in name_counts.items() if n in chain_names))

        # Log saturation stats
        if saturated_points > 0:
            sat_rate = saturated_points / max(total_steps, 1)
            logger.warning(
                "Job %s: %d/%d grid points saturated (%.0f%%). "
                "Consider tighter grid or max mode for better coverage.",
                job_id, saturated_points, total_steps, sat_rate * 100,
            )

        # Email enrichment pass
        enriched = 0
        if enrich_emails and not cancel_event.is_set():
            from app.services.enricher import enrich_leads
            for cc in seen_countries:
                if cancel_event.is_set():
                    break
                enriched += await enrich_leads(cc, job_id, cancel_event)

        # SERP discovery pass: find websites for leads with no website, then extract emails
        serp_enriched = 0
        if serp_discovery and not cancel_event.is_set():
            from app.services.enricher import discover_and_enrich
            for cc in seen_countries:
                if cancel_event.is_set():
                    break
                serp_enriched += await discover_and_enrich(cc, job_id, cancel_event)

        # Mark completed
        if not cancel_event.is_set():
            status = "completed"
        elif credit_limit and total_api_calls * _CREDITS_PER_CALL >= credit_limit:
            status = "budget_reached"
        else:
            status = "cancelled"
        db.update_job(
            job_id,
            status=status,
            total_leads=total_leads,
            total_duplicates=total_dupes,
            total_api_calls=total_api_calls,
            total_enriched=enriched,
            total_serp_enriched=serp_enriched,
            saturated_points=saturated_points,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Job %s %s: %d leads, %d dupes, %d API calls, %d saturated, %d closed skipped, %d serp enriched",
                     job_id, status, total_leads, total_dupes, total_api_calls,
                     saturated_points, total_closed_skipped, serp_enriched)

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
