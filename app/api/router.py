from fastapi import APIRouter, Depends

from app.api import scrape, jobs, stats, regions, leads, settings
from app.api.auth import get_current_user

api_router = APIRouter(dependencies=[Depends(get_current_user)])

api_router.include_router(scrape.router, tags=["scrape"])
api_router.include_router(jobs.router, tags=["jobs"])
api_router.include_router(stats.router, tags=["stats"])
api_router.include_router(regions.router, tags=["regions"])
api_router.include_router(leads.router, tags=["leads"])
api_router.include_router(settings.router, tags=["settings"])
