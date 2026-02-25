import asyncio

from fastapi import APIRouter, HTTPException

from app.services import database as db
from app.services.regions import resolve_cities
from app.services.scraper import cancel_job, is_job_running, run_job, launch_job_task

router = APIRouter()


@router.get("/jobs")
async def list_jobs(limit: int = 50, offset: int = 0):
    return db.list_jobs(limit=limit, offset=offset)


@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job["is_running"] = is_job_running(job_id)
    return job


@router.post("/jobs/{job_id}/cancel")
async def cancel(job_id: str):
    # If the task is alive in memory, signal it to stop
    if cancel_job(job_id):
        db.update_job(job_id, status="cancelling")
        return {"status": "cancelling", "job_id": job_id}
    # Orphaned job: DB says running but process is gone (e.g. after deploy)
    job = db.get_job(job_id)
    if job and job["status"] in ("running", "pending", "cancelling"):
        db.update_job(job_id, status="cancelled")
        return {"status": "cancelled", "job_id": job_id}
    raise HTTPException(status_code=404, detail="Job not found or already finished")


@router.post("/jobs/{job_id}/resume")
async def resume(job_id: str):
    """Resume a cancelled or budget_reached job from where it left off."""
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] not in ("cancelled", "budget_reached"):
        raise HTTPException(status_code=400, detail=f"Can only resume cancelled or budget-reached jobs (current: {job['status']})")
    if is_job_running(job_id):
        raise HTTPException(status_code=400, detail="Job is already running")

    tc = job.get("targeting_config") or {}
    search_queries = tc.get("search_queries", [])
    if not search_queries:
        raise HTTPException(status_code=400, detail="Job has no search queries in targeting_config")

    # Reconstruct city list from targeting_config
    countries = tc.get("countries", [job.get("country", "de")])
    all_cities = []
    for c in countries:
        all_cities.extend(resolve_cities(
            country=c,
            targeting_mode=job.get("targeting_mode", "country"),
            regions=tc.get("regions"),
            cities=tc.get("cities"),
            center_lat=tc.get("center_lat"),
            center_lng=tc.get("center_lng"),
            radius_km=tc.get("radius_km"),
            scrape_mode=tc.get("scrape_mode", "smart"),
        ))

    if not all_cities:
        raise HTTPException(status_code=400, detail="Could not reconstruct city list")

    resume_offset = job.get("processed_locations", 0)
    credit_limit = tc.get("credit_limit")

    launch_job_task(
        run_job(
            job_id=job_id,
            search_queries=search_queries,
            country=countries[0],
            cities=all_cities,
            enrich_emails=job.get("enrich_emails", False),
            serp_discovery=tc.get("serp_discovery", False),
            scrape_mode=tc.get("scrape_mode", "smart"),
            credit_limit=credit_limit,
            resume_offset=resume_offset,
        )
    )

    return {"status": "resuming", "job_id": job_id, "resume_from": resume_offset}


@router.delete("/jobs/{job_id}")
async def delete(job_id: str):
    if is_job_running(job_id):
        raise HTTPException(status_code=400, detail="Cannot delete a running job — cancel it first")
    # Allow deleting orphaned "running" jobs that aren't actually in memory
    if not db.delete_job(job_id):
        raise HTTPException(status_code=500, detail="Failed to delete job")
    return {"deleted": job_id}
