from typing import List

from fastapi import APIRouter

from app.geo import COUNTRIES
from app.geo.worldwide import list_available_countries
from app.schemas.region import CountryInfo, RegionInfo
from app.services.regions import load_cities

router = APIRouter()

COUNTRY_NAMES = {"de": "Germany", "at": "Austria", "ch": "Switzerland"}


@router.get("/regions", response_model=List[CountryInfo])
async def list_regions():
    result = []
    for code, mod in COUNTRIES.items():
        regions = [
            RegionInfo(code=k, name=v["name"])
            for k, v in mod.REGIONS.items()
        ]
        try:
            city_count = len(load_cities(code))
        except Exception:
            city_count = 0
        result.append(CountryInfo(
            code=code,
            name=COUNTRY_NAMES.get(code, code.upper()),
            regions=regions,
            city_count=city_count,
        ))
    return result


@router.get("/worldwide-countries")
async def list_worldwide_countries():
    return list_available_countries()
