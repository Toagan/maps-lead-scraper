import asyncio
import json
import logging
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.categories import CATEGORY_BUNDLES
from app.config import settings
from app.geo.worldwide import is_worldwide
from app.schemas.scrape import ScrapeRequest, ScrapeResponse
from app.services import database as db
from app.services.regions import resolve_cities
from app.services.scraper import run_job, estimate_credits, preview_search, launch_job_task

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_bundles() -> dict:
    """Merge hardcoded + custom bundles from DB."""
    merged = {
        key: {"name": val["name"], "queries": val["queries"], "custom": False}
        for key, val in CATEGORY_BUNDLES.items()
    }
    for cb in db.list_custom_bundles():
        merged[cb["key"]] = {
            "name": cb["name"],
            "queries": cb["queries"],
            "custom": True,
        }
    return merged


def _slugify(text: str) -> str:
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "_", slug)
    return slug[:60] or "custom"


# ---------------------------------------------------------------------------
# Category endpoints
# ---------------------------------------------------------------------------

@router.get("/categories")
async def list_categories():
    """List all available category bundles (hardcoded + custom)."""
    return {
        key: {
            "name": val["name"],
            "query_count": len(val["queries"]),
            "queries": val["queries"],
            "custom": val.get("custom", False),
        }
        for key, val in _all_bundles().items()
    }


class SuggestRequest(BaseModel):
    niche: str
    language: str = "English"


@router.post("/categories/suggest")
async def suggest_terms(req: SuggestRequest):
    """Use OpenAI to generate search terms for a niche."""
    if not settings.openai_api_key:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured")

    from openai import OpenAI

    client = OpenAI(api_key=settings.openai_api_key)
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.7,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You generate Google Maps search terms for lead generation. "
                        "Return ONLY a JSON array of strings. No explanation, no markdown."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Generate 12 Google Maps search terms to find businesses in this exact niche: \"{req.niche}\"\n"
                        f"The terms MUST be relevant to \"{req.niche}\" — do NOT include terms from other industries.\n"
                        f"Language for the search terms: {req.language}.\n"
                        f"Return a JSON array of 12 strings."
                    ),
                },
            ],
        )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```\w*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
        terms = json.loads(raw)
        if not isinstance(terms, list):
            raise ValueError("Expected a JSON array")
        return {"terms": [str(t) for t in terms]}
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail="AI returned invalid JSON")
    except Exception as exc:
        logger.error("OpenAI suggest error: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc))


class SaveBundleRequest(BaseModel):
    name: str
    queries: list[str]


@router.post("/categories/save")
async def save_bundle(req: SaveBundleRequest):
    """Save a custom bundle to the database."""
    if not req.name or not req.queries:
        raise HTTPException(status_code=400, detail="Name and queries are required")
    key = "custom_" + _slugify(req.name)
    if key in CATEGORY_BUNDLES:
        raise HTTPException(status_code=400, detail="Cannot overwrite a built-in bundle")
    ok = db.save_custom_bundle(key, req.name, req.queries)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save bundle")
    return {"key": key, "name": req.name, "query_count": len(req.queries)}


@router.delete("/categories/{key}")
async def delete_bundle(key: str):
    """Delete a custom bundle."""
    if key in CATEGORY_BUNDLES:
        raise HTTPException(status_code=400, detail="Cannot delete a built-in bundle")
    ok = db.delete_custom_bundle(key)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to delete bundle")
    return {"deleted": key}


# ---------------------------------------------------------------------------
# Scrape endpoint
# ---------------------------------------------------------------------------

@router.post("/scrape/preview")
async def scrape_preview(req: ScrapeRequest):
    """Run a 1-page test search (1 API call) and return relevance analysis."""
    # Resolve search terms
    all_bundles = _all_bundles()
    if req.category_key:
        bundle = all_bundles.get(req.category_key)
        if not bundle:
            raise HTTPException(status_code=400, detail=f"Unknown category: {req.category_key}")
        search_queries = bundle["queries"]
    elif req.search_term:
        search_queries = [req.search_term]
    else:
        raise HTTPException(status_code=400, detail="Provide search_term or category_key")

    # Resolve cities — just take the first country's first city for preview
    preview_country = (req.countries[0] if req.countries else req.country)
    try:
        cities = resolve_cities(
            country=preview_country,
            targeting_mode=req.targeting_mode,
            regions=req.regions,
            cities=req.cities,
            center_lat=req.center_lat,
            center_lng=req.center_lng,
            radius_km=req.radius_km,
            scrape_mode=req.scrape_mode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not cities:
        raise HTTPException(status_code=400, detail="No cities matched")

    # Preview uses first query + first (largest) city
    result = await preview_search(
        search_term=search_queries[0],
        country=preview_country,
        city=cities[0],
    )
    return result


@router.post("/scrape/estimate")
async def scrape_estimate(req: ScrapeRequest):
    """Estimate credits needed for a scrape run WITHOUT starting it."""
    all_bundles = _all_bundles()
    if req.category_key:
        bundle = all_bundles.get(req.category_key)
        if not bundle:
            raise HTTPException(status_code=400, detail=f"Unknown category: {req.category_key}")
        search_queries = bundle["queries"]
    elif req.search_term:
        search_queries = [req.search_term]
    else:
        raise HTTPException(status_code=400, detail="Provide search_term or category_key")

    country_list = req.countries if req.countries else [req.country]
    all_cities = []
    try:
        for c in country_list:
            all_cities.extend(resolve_cities(
                country=c,
                targeting_mode=req.targeting_mode,
                regions=req.regions,
                cities=req.cities,
                center_lat=req.center_lat,
                center_lng=req.center_lng,
                radius_km=req.radius_km,
                scrape_mode=req.scrape_mode,
            ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not all_cities:
        raise HTTPException(status_code=400, detail="No cities matched")

    return estimate_credits(all_cities, search_queries)


@router.post("/scrape", response_model=ScrapeResponse)
async def start_scrape(req: ScrapeRequest):
    # Resolve search terms: category bundle or single term
    all_bundles = _all_bundles()
    if req.category_key:
        bundle = all_bundles.get(req.category_key)
        if not bundle:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown category: {req.category_key}. Available: {list(all_bundles.keys())}",
            )
        search_queries = bundle["queries"]
        display_name = bundle["name"]
    elif req.search_term:
        search_queries = [req.search_term]
        display_name = req.search_term
    else:
        raise HTTPException(status_code=400, detail="Provide search_term or category_key")

    # Multi-country support: countries list overrides single country
    country_list = req.countries if req.countries else [req.country]

    # Worldwide: allow country, radius, and cities targeting
    allowed_ww_modes = {"country", "radius", "cities"}
    for c in country_list:
        if is_worldwide(c) and req.targeting_mode not in allowed_ww_modes:
            raise HTTPException(
                status_code=400,
                detail=f"Worldwide mode supports 'country', 'radius', and 'cities' targeting (got '{req.targeting_mode}')",
            )

    # Resolve cities for all countries
    all_cities = []
    try:
        for c in country_list:
            all_cities.extend(resolve_cities(
                country=c,
                targeting_mode=req.targeting_mode,
                regions=req.regions,
                cities=req.cities,
                center_lat=req.center_lat,
                center_lng=req.center_lng,
                radius_km=req.radius_km,
                scrape_mode=req.scrape_mode,
            ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not all_cities:
        raise HTTPException(status_code=400, detail="No cities matched the targeting config")

    # Persist job
    country_label = ",".join(c.upper() for c in country_list)
    targeting_config = {
        "regions": req.regions,
        "cities": req.cities,
        "center_lat": req.center_lat,
        "center_lng": req.center_lng,
        "radius_km": req.radius_km,
        "scrape_mode": req.scrape_mode,
        "category_key": req.category_key,
        "countries": country_list,
        "search_queries": search_queries,
        "credit_limit": req.credit_limit,
        "serp_discovery": req.serp_discovery,
    }
    job_id = db.create_job(
        search_term=display_name,
        country=country_label,
        targeting_mode=req.targeting_mode,
        targeting_config=targeting_config,
        enrich_emails=req.enrich_emails,
        total_locations=len(all_cities),
        job_name=req.job_name or None,
    )

    # Credit estimate
    credit_info = estimate_credits(all_cities, search_queries)

    # Launch background task (strong ref prevents GC on Python < 3.12)
    launch_job_task(
        run_job(
            job_id=job_id,
            search_queries=search_queries,
            country=country_list[0],
            cities=all_cities,
            enrich_emails=req.enrich_emails,
            serp_discovery=req.serp_discovery,
            scrape_mode=req.scrape_mode,
            credit_limit=req.credit_limit,
        )
    )

    est = credit_info["estimated_credits"]
    limit_msg = f", limit: {req.credit_limit}" if req.credit_limit else ", no limit"
    return ScrapeResponse(
        job_id=job_id,
        status="pending",
        total_locations=len(all_cities),
        estimated_credits=est,
        message=f"Scraping '{display_name}' ({len(search_queries)} terms) across {len(all_cities)} locations in {country_label} (~{est} credits{limit_msg})",
    )
