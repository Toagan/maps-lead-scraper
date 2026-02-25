import logging
import os
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.services.database import init_supabase, close_supabase
from app.services.serper import close_session
from app.api.router import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_supabase()
    _recover_orphaned_jobs()
    yield
    await close_session()
    close_supabase()


def _recover_orphaned_jobs():
    """On startup, mark any 'running'/'pending'/'cancelling' jobs as cancelled.
    These are leftovers from a previous server instance that died."""
    from app.services import database as db
    from datetime import datetime, timezone
    try:
        jobs = db.list_jobs(limit=100)
        for j in jobs:
            if j.get("status") in ("running", "pending", "cancelling"):
                logging.getLogger(__name__).warning(
                    "Recovering orphaned job %s (was %s)", j["id"], j["status"])
                db.update_job(
                    j["id"],
                    status="cancelled",
                    error_message="Server restarted while job was running",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                )
    except Exception as exc:
        logging.getLogger(__name__).error("Failed to recover orphaned jobs: %s", exc)


app = FastAPI(title="Maps Lead Scraper", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
async def root():
    return FileResponse(os.path.join(_static_dir, "index.html"))
