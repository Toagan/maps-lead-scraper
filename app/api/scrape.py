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
from app.services.scraper import run_job

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
                    "content": "You generate Google Maps search terms for lead generation.",
                },
                {
                    "role": "user",
                    "content": (
                        f"Generate 12 Google Maps search terms to find {req.niche} businesses. "
                        f"Language: {req.language}. Return a JSON array of strings only."
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

    # Worldwide: force country targeting only
    if is_worldwide(req.country):
        if req.targeting_mode != "country":
            raise HTTPException(
                status_code=400,
                detail="Worldwide mode only supports 'country' targeting",
            )

    # Resolve cities
    try:
        cities = resolve_cities(
            country=req.country,
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
        raise HTTPException(status_code=400, detail="No cities matched the targeting config")

    # Persist job
    targeting_config = {
        "regions": req.regions,
        "cities": req.cities,
        "center_lat": req.center_lat,
        "center_lng": req.center_lng,
        "radius_km": req.radius_km,
        "scrape_mode": req.scrape_mode,
        "category_key": req.category_key,
    }
    job_id = db.create_job(
        search_term=display_name,
        country=req.country,
        targeting_mode=req.targeting_mode,
        targeting_config=targeting_config,
        enrich_emails=req.enrich_emails,
        total_locations=len(cities),
    )

    # Launch background task
    asyncio.create_task(
        run_job(
            job_id=job_id,
            search_queries=search_queries,
            country=req.country,
            cities=cities,
            enrich_emails=req.enrich_emails,
            scrape_mode=req.scrape_mode,
        )
    )

    return ScrapeResponse(
        job_id=job_id,
        status="pending",
        total_locations=len(cities),
        message=f"Scraping '{display_name}' ({len(search_queries)} terms) across {len(cities)} locations",
    )
