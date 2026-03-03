from fastapi import APIRouter

from app.services import database as db
from app.services.serper import get_account_info

router = APIRouter()


@router.get("/stats")
async def stats():
    return db.get_stats()


@router.get("/stats/serper-account")
async def serper_account():
    info = await get_account_info()
    if info is None:
        return {"available": False}
    return {"available": True, **info}
