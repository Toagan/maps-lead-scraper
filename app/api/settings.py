from fastapi import APIRouter
from pydantic import BaseModel

from app.config import settings
from app.services.serper import get_serper_api_key, set_serper_api_key

router = APIRouter()


class SerperKeyRequest(BaseModel):
    api_key: str


def _mask(key: str) -> str:
    """Show first 4 and last 4 chars, mask the rest."""
    if len(key) <= 10:
        return "****"
    return key[:4] + "*" * (len(key) - 8) + key[-4:]


@router.get("/settings")
async def get_settings():
    key = get_serper_api_key()
    return {
        "serper_api_key_masked": _mask(key) if key else "",
        "serper_api_key_source": "custom" if get_serper_api_key() != settings.serper_api_key else "env",
        "has_serper_key": bool(key),
        "openai_configured": bool(settings.openai_api_key),
    }


@router.put("/settings/serper-key")
async def update_serper_key(req: SerperKeyRequest):
    key = req.api_key.strip()
    if not key:
        # Reset to env default
        set_serper_api_key(None)
        return {"status": "reset", "source": "env"}
    set_serper_api_key(key)
    return {"status": "updated", "masked": _mask(key), "source": "custom"}


@router.delete("/settings/serper-key")
async def reset_serper_key():
    """Reset to the environment default key."""
    set_serper_api_key(None)
    key = get_serper_api_key()
    return {
        "status": "reset",
        "source": "env",
        "has_key": bool(key),
        "masked": _mask(key) if key else "",
    }
