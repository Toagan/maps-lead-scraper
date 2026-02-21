from fastapi import APIRouter, HTTPException

from app.services import database as db
from app.services.scraper import cancel_job, is_job_running

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
    if cancel_job(job_id):
        db.update_job(job_id, status="cancelling")
        return {"status": "cancelling", "job_id": job_id}
    raise HTTPException(status_code=404, detail="Job not running")
