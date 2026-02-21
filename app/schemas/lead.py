from typing import Dict, Optional

from pydantic import BaseModel


class Lead(BaseModel):
    place_id: str
    cid: Optional[str] = None
    name: str
    address: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    email_source: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    category: Optional[str] = None
    categories: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    thumbnail_url: Optional[str] = None
    operating_hours: Optional[Dict] = None
    price_range: Optional[str] = None
    description: Optional[str] = None
    country: str
    region: Optional[str] = None
    city: Optional[str] = None
    search_term: Optional[str] = None
