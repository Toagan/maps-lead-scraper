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


@router.delete("/jobs/{job_id}")
async def delete(job_id: str):
    if is_job_running(job_id):
        raise HTTPException(status_code=400, detail="Cannot delete a running job — cancel it first")
    # Allow deleting orphaned "running" jobs that aren't actually in memory
    if not db.delete_job(job_id):
        raise HTTPException(status_code=500, detail="Failed to delete job")
    return {"deleted": job_id}
