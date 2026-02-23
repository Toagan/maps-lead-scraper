from typing import List, Optional

from pydantic import BaseModel


class ScrapeRequest(BaseModel):
    job_name: Optional[str] = None  # custom label for this job; defaults to search term / category name
    search_term: str = ""  # single term, OR use category_key
    category_key: Optional[str] = None  # e.g. "baubranche" — runs all queries in bundle
    country: str = "de"  # single country OR first of countries list
    countries: Optional[List[str]] = None  # e.g. ["de","at","ch"] — overrides country
    targeting_mode: str = "country"  # country | regions | cities | radius
    regions: Optional[List[str]] = None
    cities: Optional[List[str]] = None
    center_lat: Optional[float] = None
    center_lng: Optional[float] = None
    radius_km: Optional[float] = None
    enrich_emails: bool = False
    scrape_mode: str = "smart"  # quick | smart | thorough | max
    credit_limit: Optional[int] = None  # max credits to spend; None = unlimited


class ScrapeResponse(BaseModel):
    job_id: str
    status: str
    total_locations: int
    estimated_credits: int = 0
    message: str
