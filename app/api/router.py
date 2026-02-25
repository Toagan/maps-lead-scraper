from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.api import scrape, jobs, stats, regions, leads, settings

api_router = APIRouter()

api_router.include_router(scrape.router, tags=["scrape"])
api_router.include_router(jobs.router, tags=["jobs"])
api_router.include_router(stats.router, tags=["stats"])
api_router.include_router(regions.router, tags=["regions"])
api_router.include_router(leads.router, tags=["leads"])
api_router.include_router(settings.router, tags=["settings"])


@api_router.get("/health")
async def health():
    return JSONResponse({"status": "ok"})
