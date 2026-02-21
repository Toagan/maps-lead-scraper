from __future__ import annotations

from app.geo import germany, austria, switzerland
from math import radians, cos, sin, asin, sqrt

COUNTRIES = {
    "de": germany,
    "at": austria,
    "ch": switzerland,
}


def get_country_module(country_code: str):
    mod = COUNTRIES.get(country_code.lower())
    if not mod:
        raise ValueError(f"Unsupported country: {country_code}")
    return mod


def get_region(lat: float, lon: float, country_code: str) -> str | None:
    """Determine which region a coordinate belongs to."""
    mod = get_country_module(country_code)

    # Check border overrides first
    for o_lat, o_lon, code, tol in mod.BORDER_OVERRIDES:
        if abs(lat - o_lat) < tol and abs(lon - o_lon) < tol:
            return code

    matches: list[tuple[str, float]] = []
    for code, data in mod.REGIONS.items():
        min_lat, max_lat, min_lon, max_lon = data["bounds"]
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            center_lat = (min_lat + max_lat) / 2
            center_lon = (min_lon + max_lon) / 2
            dist = ((lat - center_lat) ** 2 + (lon - center_lon) ** 2) ** 0.5
            matches.append((code, dist))

    if not matches:
        return None

    # Prioritise small city-states (DE only)
    if country_code == "de":
        codes = {m[0] for m in matches}
        for cs in ("BE", "HH", "HB"):
            if cs in codes:
                return cs

    matches.sort(key=lambda x: x[1])
    return matches[0][0]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in km."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 6371 * 2 * asin(sqrt(a))
