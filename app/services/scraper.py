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


async def _scrape_grid_point(
    query: str,
    gl: str,
    hl: str,
    gp_lat: float,
    gp_lon: float,
    zoom: int,
    max_pages: int,
    search_term: str,
    city_name: str,
    cancel_event: asyncio.Event,
) -> tuple[list[dict], int, int, bool]:
    """Process one grid point: paginate and collect raw records.

    Returns (records, api_calls, closed_skipped, saturated).
    Dedup is done centrally after gather to avoid interleaving issues.
    """
    records: list[dict] = []
    local_api_calls = 0
    local_closed_skipped = 0
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
        local_api_calls += 1

        if not data or "places" not in data or not data["places"]:
            break

        places = data["places"]
        last_page_count = len(places)
        new_on_page = 0
        page_pids: set[str] = set()

        for place in places:
            if is_place_closed(place):
                local_closed_skipped += 1
                continue

            pdata = extract_place_data(place, search_term, city_name)
            pid = pdata["place_id"]
            if not pid:
                continue

            if not _result_within_bounds(
                pdata.get("latitude"),
                pdata.get("longitude"),
                gp_lat,
                gp_lon,
            ):
                continue

            # Track new within this page for early-stop (use page-local set)
            if pid not in page_pids:
                new_on_page += 1
                page_pids.add(pid)

            records.append({
                "pdata": pdata,
                "gp_lat": gp_lat,
                "gp_lon": gp_lon,
            })

        # Partial page = last page
        if last_page_count < 20:
            break

        # Adaptive duplicate threshold within this grid point's pages
        if new_on_page < last_page_count * 0.25:
            break

    saturated = (page + 1 >= max_pages and last_page_count >= 20) if max_pages > 0 else False
    return records, local_api_calls, local_closed_skipped, saturated


async def _scrape_grid_point_with_meta(
    city_name: str,
    city_country: str,
    region_code: str | None,
    **gp_kwargs,
) -> tuple[str, str, str | None, list[dict], int, int, bool]:
    """Wrapper that bundles city metadata with grid point results."""
    records, api_calls, closed, saturated = await _scrape_grid_point(
        city_name=city_name, **gp_kwargs,
    )
    return city_name, city_country, region_code, records, api_calls, closed, saturated


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
    await asyncio.to_thread(
        db.update_job,
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

            # Flatten ALL grid points across ALL cities into one task pool.
            # The semaphore (20) limits actual API concurrency; this ensures
            # all slots stay busy even with many single-point small cities.
            all_coros = []

            for city_idx, city in enumerate(cities):
                grid_points = city_grids[city_idx]
                zoom, max_pages = get_city_scrape_config(city.population)
                city_country = city.country or country
                city_ww = is_worldwide(city_country)
                region_code = None if city_ww else get_region(city.lat, city.lon, city_country)
                gl, hl = _get_gl_hl(city_country)
                query = f"{search_term} in {city.name}"

                for gp_lat, gp_lon in grid_points:
                    all_coros.append(
                        _scrape_grid_point_with_meta(
                            city_name=city.name,
                            city_country=city_country,
                            region_code=region_code,
                            query=query,
                            gl=gl,
                            hl=hl,
                            gp_lat=gp_lat,
                            gp_lon=gp_lon,
                            zoom=zoom,
                            max_pages=max_pages,
                            search_term=search_term,
                            cancel_event=cancel_event,
                        )
                    )

            # Process results as they complete — zero idle gaps between tasks.
            # as_completed yields futures in completion order, keeping all
            # semaphore slots busy continuously.
            for future in asyncio.as_completed(all_coros):
                city_name, city_country, region_code, records, api_calls, closed_skipped, saturated = await future

                current_step += 1
                total_api_calls += api_calls
                total_closed_skipped += closed_skipped
                if saturated:
                    saturated_points += 1

                for rec in records:
                    pdata = rec["pdata"]
                    pid = pdata["place_id"]

                    if pid in seen_ids:
                        total_dupes += 1
                        continue

                    seen_ids.add(pid)
                    total_leads += 1

                    relevance = compute_category_relevance(
                        search_term,
                        pdata.get("category", ""),
                        pdata.get("categories", ""),
                    )

                    review_count = pdata.get("review_count") or 0
                    low_confidence = review_count <= 2

                    biz_name = pdata["name"]
                    name_counts[biz_name] = name_counts.get(biz_name, 0) + 1

                    addr_parts = parse_dach_address(pdata.get("address") or "")

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
                        "city": city_name,
                        "search_term": search_term,
                        "category_relevance": relevance,
                        "low_confidence": low_confidence,
                        "job_id": job_id,
                    }
                    leads_buffer.append(record)

                    if len(leads_buffer) >= 50:
                        await asyncio.to_thread(db.upsert_leads, leads_buffer)
                        leads_buffer = []

                # Credit budget check
                if credit_limit and total_api_calls * _CREDITS_PER_CALL >= credit_limit:
                    logger.info("Job %s: credit limit reached (%d/%d)",
                                job_id, total_api_calls * _CREDITS_PER_CALL, credit_limit)
                    cancel_event.set()

                # Progress update every 10 completed grid points
                if current_step % 10 == 0 or current_step == total_steps:
                    await asyncio.to_thread(
                        db.update_job,
                        job_id,
                        processed_locations=current_step,
                        total_locations=total_steps,
                        total_leads=total_leads,
                        total_duplicates=total_dupes,
                        total_api_calls=total_api_calls,
                    )

        # Flush remaining leads (always, even on cancel/budget_reached)
        if leads_buffer:
            await asyncio.to_thread(db.upsert_leads, leads_buffer)
            leads_buffer = []

        # Chain detection: flag businesses whose name appeared 5+ times
        chain_names = {n for n, c in name_counts.items() if c >= 5}
        if chain_names:
            await asyncio.to_thread(db.flag_chains, job_id, chain_names)
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
        await asyncio.to_thread(
            db.update_job,
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
        # Flush any remaining leads even on failure
        if leads_buffer:
            try:
                await asyncio.to_thread(db.upsert_leads, leads_buffer)
            except Exception:
                logger.warning("Job %s: failed to flush leads buffer on error", job_id)
        await asyncio.to_thread(
            db.update_job,
            job_id,
            status="failed",
            error_message=str(exc),
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
    finally:
        _running_jobs.pop(job_id, None)
