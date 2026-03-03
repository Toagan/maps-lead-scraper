from __future__ import annotations

import csv
import io
from typing import Iterator, Optional

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
    min_fit_score: Optional[float] = None,
    job_id: Optional[str] = None,
    exclude_chains: Optional[bool] = None,
    exclude_low_confidence: Optional[bool] = None,
    min_reviews: Optional[int] = None,
    limit: Optional[int] = Query(default=None, ge=1),
    offset: int = 0,
    format: Optional[str] = None,
    filename: Optional[str] = None,
):
    filters = {
        "country": country,
        "region": region,
        "category": category,
        "categories": categories,
        "has_email": has_email,
        "has_phone": has_phone,
        "has_website": has_website,
        "search_term": search_term,
        "min_relevance": min_relevance,
        "min_fit_score": min_fit_score,
        "job_id": job_id,
        "exclude_chains": exclude_chains,
        "exclude_low_confidence": exclude_low_confidence,
        "min_reviews": min_reviews,
    }

    if format == "csv":
        return _csv_stream_response(
            filename=filename,
            filters=filters,
            offset=offset,
            max_rows=limit,
        )

    limit_value = limit or 100
    leads, total = db.query_leads(
        **filters,
        limit=limit_value,
        offset=offset,
    )
    leads = [_add_google_maps_url(l) for l in leads]

    return {"leads": leads, "total": total, "limit": limit_value, "offset": offset}


def _sanitize_filename(filename: Optional[str]) -> str:
    fname = (filename.strip() if filename else "leads") or "leads"
    # Sanitize: keep alphanumeric, hyphens, underscores, dots
    fname = "".join(c for c in fname if c.isalnum() or c in "-_.")
    if not fname:
        fname = "leads"
    return fname + ".csv"


def _iter_csv_chunks(
    *,
    filters: dict,
    offset: int,
    max_rows: int | None,
) -> Iterator[str]:
    # Stream in pages so CSV export is no longer capped to 50k rows.
    page_size = 1000
    fetched = 0
    current_offset = offset
    writer: csv.DictWriter | None = None
    output = io.StringIO()

    while True:
        chunk_size = page_size if max_rows is None else min(page_size, max_rows - fetched)
        if chunk_size <= 0:
            break

        leads, _ = db.query_leads(
            **filters,
            limit=chunk_size,
            offset=current_offset,
        )
        leads = [_add_google_maps_url(l) for l in leads]
        if not leads:
            break

        if writer is None:
            writer = csv.DictWriter(output, fieldnames=leads[0].keys())
            writer.writeheader()
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

        writer.writerows(leads)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        fetched += len(leads)
        current_offset += len(leads)
        if len(leads) < chunk_size:
            break

    if writer is None:
        yield ""


def _csv_stream_response(
    *,
    filename: Optional[str],
    filters: dict,
    offset: int,
    max_rows: int | None,
) -> StreamingResponse:
    fname = _sanitize_filename(filename)
    return StreamingResponse(
        _iter_csv_chunks(filters=filters, offset=offset, max_rows=max_rows),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )
