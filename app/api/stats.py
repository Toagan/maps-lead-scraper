from fastapi import APIRouter

from app.services import database as db

router = APIRouter()


@router.get("/stats")
async def stats():
    return db.get_stats()
