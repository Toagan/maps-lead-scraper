import csv
import io
from typing import List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.services import database as db

router = APIRouter()


@router.get("/leads")
async def get_leads(
    country: Optional[str] = None,
    region: Optional[str] = None,
    category: Optional[str] = None,
    has_email: Optional[bool] = None,
    has_phone: Optional[bool] = None,
    search_term: Optional[str] = None,
    limit: int = Query(default=100, le=50000),
    offset: int = 0,
    format: Optional[str] = None,
):
    leads, total = db.query_leads(
        country=country,
        region=region,
        category=category,
        has_email=has_email,
        has_phone=has_phone,
        search_term=search_term,
        limit=limit,
        offset=offset,
    )

    if format == "csv":
        return _csv_response(leads)

    return {"leads": leads, "total": total, "limit": limit, "offset": offset}


def _csv_response(leads: List[dict]) -> StreamingResponse:
    output = io.StringIO()
    if not leads:
        output.write("")
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=leads.csv"},
        )

    writer = csv.DictWriter(output, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )
