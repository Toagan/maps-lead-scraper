import csv
import io
from typing import List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.services import database as db

router = APIRouter()


def _add_google_maps_url(lead: dict) -> dict:
    """Add computed google_maps_url from CID (no schema change needed)."""
    cid = lead.get("cid")
    if cid:
        lead["google_maps_url"] = f"https://www.google.com/maps?cid={cid}"
    else:
        lead["google_maps_url"] = ""
    return lead


@router.get("/leads/categories")
async def get_lead_categories(job_id: str):
    return db.get_job_categories(job_id)


@router.get("/leads")
async def get_leads(
    country: Optional[str] = None,
    region: Optional[str] = None,
    category: Optional[str] = None,
    categories: Optional[str] = None,
    has_email: Optional[bool] = None,
    has_phone: Optional[bool] = None,
    has_website: Optional[bool] = None,
    search_term: Optional[str] = None,
    min_relevance: Optional[float] = None,
    job_id: Optional[str] = None,
    exclude_chains: Optional[bool] = None,
    exclude_low_confidence: Optional[bool] = None,
    min_reviews: Optional[int] = None,
    limit: int = Query(default=100, le=50000),
    offset: int = 0,
    format: Optional[str] = None,
    filename: Optional[str] = None,
):
    leads, total = db.query_leads(
        country=country,
        region=region,
        category=category,
        categories=categories,
        has_email=has_email,
        has_phone=has_phone,
        has_website=has_website,
        search_term=search_term,
        min_relevance=min_relevance,
        job_id=job_id,
        exclude_chains=exclude_chains,
        exclude_low_confidence=exclude_low_confidence,
        min_reviews=min_reviews,
        limit=limit,
        offset=offset,
    )

    leads = [_add_google_maps_url(l) for l in leads]

    if format == "csv":
        return _csv_response(leads, filename=filename)

    return {"leads": leads, "total": total, "limit": limit, "offset": offset}


def _csv_response(leads: List[dict], filename: Optional[str] = None) -> StreamingResponse:
    output = io.StringIO()
    fname = (filename.strip() if filename else "leads") or "leads"
    # Sanitize: keep alphanumeric, hyphens, underscores, dots
    fname = "".join(c for c in fname if c.isalnum() or c in "-_.")
    if not fname:
        fname = "leads"
    fname += ".csv"

    if not leads:
        output.write("")
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={fname}"},
        )

    writer = csv.DictWriter(output, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )
