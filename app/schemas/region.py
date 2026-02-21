from typing import List

from pydantic import BaseModel


class RegionInfo(BaseModel):
    code: str
    name: str


class CountryInfo(BaseModel):
    code: str
    name: str
    regions: List[RegionInfo]
    city_count: int
