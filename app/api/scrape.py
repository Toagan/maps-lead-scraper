import asyncio

from fastapi import APIRouter, HTTPException

from app.categories import CATEGORY_BUNDLES
from app.schemas.scrape import ScrapeRequest, ScrapeResponse
from app.services import database as db
from app.services.regions import resolve_cities
from app.services.scraper import run_job

router = APIRouter()


@router.get("/categories")
async def list_categories():
    """List all available category bundles."""
    return {
        key: {"name": val["name"], "query_count": len(val["queries"]), "queries": val["queries"]}
        for key, val in CATEGORY_BUNDLES.items()
    }


@router.post("/scrape", response_model=ScrapeResponse)
async def start_scrape(req: ScrapeRequest):
    # Resolve search terms: category bundle or single term
    if req.category_key:
        bundle = CATEGORY_BUNDLES.get(req.category_key)
        if not bundle:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown category: {req.category_key}. Available: {list(CATEGORY_BUNDLES.keys())}",
            )
        search_queries = bundle["queries"]
        display_name = bundle["name"]
    elif req.search_term:
        search_queries = [req.search_term]
        display_name = req.search_term
    else:
        raise HTTPException(status_code=400, detail="Provide search_term or category_key")

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
