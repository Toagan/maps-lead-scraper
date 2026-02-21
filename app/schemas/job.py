from datetime import datetime
from typing import Dict, Optional

from pydantic import BaseModel


class JobProgress(BaseModel):
    id: str
    status: str
    search_term: str
    country: str
    total_locations: int = 0
    processed_locations: int = 0
    total_leads: int = 0
    total_duplicates: int = 0
    total_enriched: int = 0
    total_api_calls: int = 0
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobDetail(JobProgress):
    targeting_mode: str = ""
    targeting_config: Dict = {}
    enrich_emails: bool = False
