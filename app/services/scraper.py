"""Job orchestrator — runs a scrape job asynchronously."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List

from app.geo import get_region, get_country_module, haversine_km
from app.geo.worldwide import is_worldwide, get_serper_params, get_country_name
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

# Strong references to background tasks — prevents GC on Python < 3.12
_background_tasks: set[asyncio.Task] = set()

# Max distance (km) a result may be from the search point before we discard it.
# Google Maps can return results well outside the visible area.
# Tighter for dense cities (fewer irrelevant far-away results), looser for rural.
_MAX_DIST_DEFAULT_KM = 25.0
_MAX_DIST_LARGE_CITY_KM = 8.0   # pop >= 500k
_MAX_DIST_MEDIUM_CITY_KM = 15.0  # pop >= 100k


def _max_distance_km(population: int) -> float:
    if population >= 500_000:
        return _MAX_DIST_LARGE_CITY_KM
    if population >= 100_000:
        return _MAX_DIST_MEDIUM_CITY_KM
    return _MAX_DIST_DEFAULT_KM


class _CallBudget:
    """Global API-call budget shared across concurrent workers."""

    def __init__(self, max_calls: int):
        self.max_calls = max(0, max_calls)
        self.used_calls = 0
        self._lock = asyncio.Lock()

    async def try_acquire(self) -> bool:
        async with self._lock:
            if self.used_calls >= self.max_calls:
                return False
            self.used_calls += 1
            return True

    async def remaining(self) -> int:
        async with self._lock:
            return max(0, self.max_calls - self.used_calls)


def launch_job_task(coro) -> asyncio.Task:
    """Create a background task with a strong reference to prevent GC."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


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
    max_km: float = _MAX_DIST_DEFAULT_KM,
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


def _compute_fit_score(
    relevance: float,
    rating: float | None,
    review_count: int,
    has_website: bool,
    has_phone: bool,
    low_confidence: bool,
) -> float:
    """Compute a 0.0-1.0 outbound-fit score without dropping any lead."""
    score = relevance * 0.55
    if has_website:
        score += 0.15
    if has_phone:
        score += 0.10

    if review_count >= 50:
        score += 0.12
    elif review_count >= 10:
        score += 0.08
    elif review_count >= 3:
        score += 0.04

    if rating is not None:
        if rating >= 4.5:
            score += 0.08
        elif rating >= 4.0:
            score += 0.05
        elif rating >= 3.5:
            score += 0.02

    if low_confidence:
        score -= 0.08
    return max(0.0, min(1.0, round(score, 4)))


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
    call_budget: _CallBudget | None = None,
    phase_stop_event: asyncio.Event | None = None,
    max_distance_km: float = _MAX_DIST_DEFAULT_KM,
    location: str | None = None,
) -> tuple[list[dict], int, int, bool]:
    """Process one grid point: paginate and collect raw records.

    Returns (records, api_calls, closed_skipped, saturated).
    Dedup is done centrally after gather to avoid interleaving issues.
    """
    records: list[dict] = []
    local_api_calls = 0
    local_closed_skipped = 0
    last_page_count = 0
    gp_pids: set[str] = set()  # Dedup within this grid point across all pages

    for page in range(max_pages):
        if cancel_event.is_set() or (phase_stop_event and phase_stop_event.is_set()):
            break

        if call_budget and not await call_budget.try_acquire():
            # Hard stop when global credit budget is exhausted.
            cancel_event.set()
            break

        data = await search_maps(
            query=query,
            gl=gl,
            hl=hl,
            lat=gp_lat,
            lon=gp_lon,
            zoom=zoom,
            start=page * 20,
            location=location,
        )
        local_api_calls += 1

        if not data or "places" not in data or not data["places"]:
            break

        places = data["places"]
        last_page_count = len(places)

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
                max_km=max_distance_km,
            ):
                continue

            # Dedup within this grid point (across pages)
            if pid in gp_pids:
                continue
            gp_pids.add(pid)

            records.append({
                "pdata": pdata,
                "gp_lat": gp_lat,
                "gp_lon": gp_lon,
            })

        # Partial page = last page
        if last_page_count < 20:
            break

    saturated = (page + 1 >= max_pages and last_page_count >= 20) if max_pages > 0 else False
    return records, local_api_calls, local_closed_skipped, saturated


async def _scrape_grid_point_with_meta(
    city_name: str,
    city_country: str,
    region_code: str | None,
    search_term: str,
    query: str,
    gp_lat: float,
    gp_lon: float,
    max_pages: int,
    **gp_kwargs,
) -> tuple[str, str, str | None, str, str, float, float, int, list[dict], int, int, bool]:
    """Wrapper that bundles city metadata with grid point results."""
    records, api_calls, closed, saturated = await _scrape_grid_point(
        city_name=city_name,
        query=query,
        gp_lat=gp_lat,
        gp_lon=gp_lon,
        max_pages=max_pages,
        search_term=search_term,
        **gp_kwargs,
    )
    return (
        city_name,
        city_country,
        region_code,
        search_term,
        query,
        gp_lat,
        gp_lon,
        max_pages,
        records,
        api_calls,
        closed,
        saturated,
    )


async def run_job(
    job_id: str,
    search_queries: List[str],
    country: str,
    cities: List[City],
    enrich_emails: bool = False,
    serp_discovery: bool = False,
    scrape_mode: str = "smart",
    credit_limit: int | None = None,
    resume_offset: int = 0,
) -> None:
    """
    Main scraping loop executed as a background task.

    search_queries: list of search terms (1 for single, 15+ for category bundle).
    All queries share a single seen_ids set for global deduplication.

    For cities with population >= 100k, a grid of coordinate points is generated
    around the city center to overcome Google's proximity bias and 120-result cap.

    resume_offset: number of grid-point tasks to skip (for resuming cancelled jobs).
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

    # On resume, preload place_ids already scraped so dedup works correctly
    total_leads = 0
    total_dupes = 0
    total_api_calls = 0
    total_closed_skipped = 0
    total_irrelevant_skipped = 0
    saturated_points = 0
    if resume_offset > 0:
        existing = await asyncio.to_thread(db.get_job_place_ids, job_id)
        seen_ids = existing
        # Reload counters from saved job state
        job_state = await asyncio.to_thread(db.get_job, job_id)
        if job_state:
            total_leads = job_state.get("total_leads", 0)
            total_dupes = job_state.get("total_duplicates", 0)
            total_api_calls = job_state.get("total_api_calls", 0)
        logger.info("Job %s: resuming from offset %d with %d existing place_ids",
                     job_id, resume_offset, len(seen_ids))
    else:
        logger.info("Job %s: %d queries, %d cities",
                    job_id, len(search_queries), len(cities))

    # Hard credit gate shared by all concurrent requests.
    call_budget: _CallBudget | None = None
    if credit_limit is not None:
        max_calls = credit_limit // _CREDITS_PER_CALL
        remaining_calls = max(0, max_calls - total_api_calls)
        call_budget = _CallBudget(remaining_calls)
        if remaining_calls == 0:
            cancel_event.set()

    leads_buffer: list[dict] = []
    # Track business name frequencies for chain detection
    name_counts: dict[str, int] = {}

    # Pre-compute grid points per city so we can calculate accurate total_steps
    city_grids: list[list[tuple[float, float]]] = []
    for city in cities:
        grid = generate_grid_points(city)
        city_grids.append([(gp.lat, gp.lon) for gp in grid])

    total_steps = sum(len(g) for g in city_grids) * len(search_queries)
    current_step = resume_offset  # start counter from where we left off

    # Build full flat list of all grid-point tasks across all queries
    all_task_descs = []
    for q_idx, search_term in enumerate(search_queries):
        for city_idx, city in enumerate(cities):
            grid_points = city_grids[city_idx]
            zoom, max_pages = get_city_scrape_config(city.population)
            city_country = city.country or country
            city_ww = is_worldwide(city_country)
            region_code = None if city_ww else get_region(city.lat, city.lon, city_country)
            gl, hl = _get_gl_hl(city_country)
            query = f"{search_term} in {city.name}"

            max_dist = _max_distance_km(city.population)
            loc_str = f"{city.name}, {get_country_name(city_country)}"
            for gp_lat, gp_lon in grid_points:
                all_task_descs.append({
                    "city_name": city.name,
                    "city_country": city_country,
                    "region_code": region_code,
                    "query": query,
                    "gl": gl, "hl": hl,
                    "gp_lat": gp_lat, "gp_lon": gp_lon,
                    "zoom": zoom, "max_pages": max_pages,
                    "search_term": search_term,
                    "max_distance_km": max_dist,
                    "location": loc_str,
                })

    # Skip already-processed tasks on resume
    if resume_offset > 0:
        all_task_descs = all_task_descs[resume_offset:]
        logger.info("Job %s: skipped %d/%d tasks, %d remaining",
                     job_id, resume_offset, total_steps, len(all_task_descs))

    # Pass-1 metrics used to decide adaptive second-pass deepening.
    pass1_candidates: dict[tuple[str, str, float, float], dict] = {}
    pass1_query_new_leads: dict[str, int] = {}
    pass1_query_api_calls: dict[str, int] = {}

    try:
        async def _flush_buffer() -> None:
            nonlocal leads_buffer
            if leads_buffer:
                await asyncio.to_thread(db.upsert_leads, leads_buffer)
                leads_buffer = []

        async def _checkpoint() -> None:
            await asyncio.to_thread(
                db.update_job,
                job_id,
                processed_locations=current_step,
                total_locations=total_steps,
                total_leads=total_leads,
                total_duplicates=total_dupes,
                total_api_calls=total_api_calls,
                saturated_points=saturated_points,
            )

        async def _process_task_descs(
            task_descs: list[dict],
            phase_label: str,
            count_progress: bool,
            collect_candidates: bool = False,
            phase_stop_event: asyncio.Event | None = None,
        ) -> tuple[int, int]:
            nonlocal current_step, total_api_calls, total_closed_skipped, total_irrelevant_skipped
            nonlocal total_leads, total_dupes, saturated_points

            DEFAULT_BATCH_SIZE = 30
            desc_idx = 0
            phase_new_leads = 0
            phase_api_calls = 0

            # Per-query early stopping for multi-term jobs
            skipped_queries: set[str] = set()
            query_new: dict[str, int] = {}
            query_calls: dict[str, int] = {}
            multi_query = len(search_queries) > 1

            while (
                desc_idx < len(task_descs)
                and not cancel_event.is_set()
                and not (phase_stop_event and phase_stop_event.is_set())
            ):
                batch_size = DEFAULT_BATCH_SIZE
                if call_budget:
                    remaining_calls = await call_budget.remaining()
                    if remaining_calls <= 0:
                        cancel_event.set()
                        break
                    batch_size = min(batch_size, max(1, remaining_calls))

                batch = task_descs[desc_idx:desc_idx + batch_size]
                desc_idx += batch_size
                if not batch:
                    break

                coros = []
                for desc in batch:
                    if desc["search_term"] in skipped_queries:
                        if count_progress:
                            current_step += 1
                        continue
                    coros.append(
                        _scrape_grid_point_with_meta(
                            cancel_event=cancel_event,
                            call_budget=call_budget,
                            phase_stop_event=phase_stop_event,
                            **desc,
                        )
                    )
                if not coros:
                    await _checkpoint()
                    continue

                for future in asyncio.as_completed(coros):
                    (
                        city_name,
                        city_country,
                        region_code,
                        search_term,
                        query,
                        gp_lat,
                        gp_lon,
                        max_pages,
                        records,
                        api_calls,
                        closed_skipped,
                        saturated,
                    ) = await future

                    if count_progress:
                        current_step += 1
                    total_api_calls += api_calls
                    total_closed_skipped += closed_skipped
                    phase_api_calls += api_calls
                    if saturated:
                        saturated_points += 1

                    new_leads_this_task = 0
                    for rec in records:
                        pdata = rec["pdata"]
                        pid = pdata["place_id"]

                        if pid in seen_ids:
                            total_dupes += 1
                            continue

                        seen_ids.add(pid)

                        relevance = compute_category_relevance(
                            pdata.get("search_term", ""),
                            pdata.get("category", ""),
                            pdata.get("categories", ""),
                        )
                        if relevance <= 0.3:
                            total_irrelevant_skipped += 1
                            continue

                        total_leads += 1
                        new_leads_this_task += 1

                        review_count = pdata.get("review_count") or 0
                        low_confidence = review_count <= 2
                        fit_score = _compute_fit_score(
                            relevance=relevance,
                            rating=pdata.get("rating"),
                            review_count=review_count,
                            has_website=bool(pdata.get("website")),
                            has_phone=bool(pdata.get("phone")),
                            low_confidence=low_confidence,
                        )

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
                            "search_term": pdata.get("search_term", ""),
                            "category_relevance": relevance,
                            "fit_score": fit_score,
                            "low_confidence": low_confidence,
                            "job_id": job_id,
                        }
                        leads_buffer.append(record)
                        if len(leads_buffer) >= 50:
                            await _flush_buffer()

                    phase_new_leads += new_leads_this_task

                    # Per-query efficiency tracking
                    if multi_query:
                        q = search_term
                        query_new[q] = query_new.get(q, 0) + new_leads_this_task
                        query_calls[q] = query_calls.get(q, 0) + api_calls
                        qc = query_calls[q]
                        if qc >= 50 and q not in skipped_queries and query_new[q] / qc < 0.20:
                            skipped_queries.add(q)
                            logger.info(
                                "Job %s: skipping query '%s' — depleted (%.2f leads/call after %d calls)",
                                job_id, q, query_new[q] / qc, qc,
                            )

                    if collect_candidates:
                        pass1_query_new_leads[search_term] = (
                            pass1_query_new_leads.get(search_term, 0) + new_leads_this_task
                        )
                        pass1_query_api_calls[search_term] = (
                            pass1_query_api_calls.get(search_term, 0) + api_calls
                        )
                        if max_pages < 6 and (saturated or new_leads_this_task >= max(12, max_pages * 8)):
                            key = (search_term, city_name, round(gp_lat, 6), round(gp_lon, 6))
                            pass1_candidates[key] = {
                                "city_name": city_name,
                                "city_country": city_country,
                                "region_code": region_code,
                                "query": query,
                                "gl": _get_gl_hl(city_country)[0],
                                "hl": _get_gl_hl(city_country)[1],
                                "gp_lat": gp_lat,
                                "gp_lon": gp_lon,
                                "zoom": 16,
                                "max_pages": 6,
                                "search_term": search_term,
                            }

                    # Extra guard for legacy jobs where total_api_calls is preloaded.
                    if credit_limit and total_api_calls * _CREDITS_PER_CALL >= credit_limit:
                        logger.info(
                            "Job %s: credit limit reached (%d/%d)",
                            job_id, total_api_calls * _CREDITS_PER_CALL, credit_limit,
                        )
                        cancel_event.set()

                    await _checkpoint()

                    if phase_stop_event and phase_api_calls >= 10:
                        marginal = phase_new_leads / max(phase_api_calls, 1)
                        if marginal < 0.35 and not phase_stop_event.is_set():
                            logger.info(
                                "Job %s: stopping %s early due low marginal yield (%.2f leads/call)",
                                job_id, phase_label, marginal,
                            )
                            phase_stop_event.set()

            return phase_new_leads, phase_api_calls

        # First pass (broad coverage)
        pass1_new, pass1_calls = await _process_task_descs(
            all_task_descs,
            phase_label="pass1",
            count_progress=True,
            collect_candidates=True,
        )

        # Adaptive second pass: deepen only high-value saturated points.
        pass2_new = 0
        pass2_calls = 0
        if not cancel_event.is_set() and pass1_candidates:
            efficiencies = {
                q: (pass1_query_new_leads.get(q, 0) / max(1, calls))
                for q, calls in pass1_query_api_calls.items()
            }
            eff_values = sorted(efficiencies.values())
            median_eff = eff_values[len(eff_values) // 2] if eff_values else 0.0
            min_eff = max(0.35, median_eff * 0.7)
            high_value_queries = {q for q, eff in efficiencies.items() if eff >= min_eff}
            pass2_descs = [
                d for d in pass1_candidates.values()
                if d["search_term"] in high_value_queries
            ]
            if pass2_descs:
                logger.info(
                    "Job %s: pass2 deepening %d points across %d queries",
                    job_id, len(pass2_descs), len(high_value_queries),
                )
                pass2_stop_event = asyncio.Event()
                pass2_new, pass2_calls = await _process_task_descs(
                    pass2_descs,
                    phase_label="pass2",
                    count_progress=False,
                    collect_candidates=False,
                    phase_stop_event=pass2_stop_event,
                )
                logger.info(
                    "Job %s: pass2 complete — %d new leads from %d API calls",
                    job_id, pass2_new, pass2_calls,
                )

        await _flush_buffer()

        # Flush remaining leads (always, even on cancel/budget_reached)
        if leads_buffer:
            await asyncio.to_thread(db.upsert_leads, leads_buffer)
            leads_buffer = []

        # Chain detection with confidence.
        chain_scores = {
            n: max(0.55, min(1.0, 0.55 + (c - 5) * 0.08))
            for n, c in name_counts.items()
            if c >= 5
        }
        if chain_scores:
            await asyncio.to_thread(db.flag_chains, job_id, chain_scores)
            logger.info(
                "Job %s: flagged %d chain names (%d leads)",
                job_id, len(chain_scores),
                sum(c for n, c in name_counts.items() if n in chain_scores),
            )

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
            processed_locations=current_step,
            total_locations=total_steps,
            total_leads=total_leads,
            total_duplicates=total_dupes,
            total_api_calls=total_api_calls,
            total_enriched=enriched + serp_enriched,
            saturated_points=saturated_points,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info(
            "Job %s %s: %d leads, %d dupes, %d irrelevant skipped, %d API calls, %d saturated, %d closed skipped, %d serp enriched (pass1=%d/%d, pass2=%d/%d)",
            job_id,
            status,
            total_leads,
            total_dupes,
            total_irrelevant_skipped,
            total_api_calls,
            saturated_points,
            total_closed_skipped,
            serp_enriched,
            pass1_new,
            pass1_calls,
            pass2_new,
            pass2_calls,
        )

    except BaseException as exc:
        # BaseException catches Exception, CancelledError, KeyboardInterrupt, SystemExit
        logger.exception("Job %s failed: %s", job_id, exc)
        # Flush any remaining leads even on failure
        if leads_buffer:
            try:
                await asyncio.to_thread(db.upsert_leads, leads_buffer)
            except Exception:
                logger.warning("Job %s: failed to flush leads buffer on error", job_id)
        try:
            await asyncio.to_thread(
                db.update_job,
                job_id,
                status="failed",
                processed_locations=current_step,
                total_locations=total_steps,
                total_leads=total_leads,
                total_duplicates=total_dupes,
                total_api_calls=total_api_calls,
                error_message=str(exc)[:500],
                completed_at=datetime.now(timezone.utc).isoformat(),
            )
        except Exception:
            logger.error("Job %s: could not update status to failed", job_id)
    finally:
        _running_jobs.pop(job_id, None)
        # Safety net: if job is still marked "running" in DB, force it to "failed"
        try:
            job = await asyncio.to_thread(db.get_job, job_id)
            if job and job.get("status") in ("running", "pending"):
                logger.warning("Job %s: safety net — forcing status to failed", job_id)
                await asyncio.to_thread(
                    db.update_job,
                    job_id,
                    status="failed",
                    error_message="Job terminated unexpectedly",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                )
        except Exception:
            logger.error("Job %s: safety net DB update failed", job_id)
