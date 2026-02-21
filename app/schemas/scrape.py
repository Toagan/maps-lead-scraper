from typing import List, Optional

from pydantic import BaseModel


class ScrapeRequest(BaseModel):
    search_term: str = ""  # single term, OR use category_key
    category_key: Optional[str] = None  # e.g. "baubranche" — runs all queries in bundle
    country: str = "de"
    targeting_mode: str = "country"  # country | regions | cities | radius
    regions: Optional[List[str]] = None
    cities: Optional[List[str]] = None
    center_lat: Optional[float] = None
    center_lng: Optional[float] = None
    radius_km: Optional[float] = None
    enrich_emails: bool = False
    scrape_mode: str = "smart"  # quick | smart | thorough | max


class ScrapeResponse(BaseModel):
    job_id: str
    status: str
    total_locations: int
    message: str
