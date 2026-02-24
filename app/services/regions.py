"""Resolve targeting config into a list of cities to scrape."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass

from app.geo import get_country_module, get_region, haversine_km


@dataclass
class City:
    name: str
    lat: float
    lon: float
    population: int
    country: str = ""  # ISO code, e.g. "de", "at", "ch"


# Population thresholds per scrape mode
MIN_POP = {
    "quick": 50_000,
    "smart": 10_000,
    "thorough": 5_000,
    "max": 0,
}


def load_cities(country_code: str) -> list[City]:
    """Load all cities from the country's city file."""
    mod = get_country_module(country_code)
    path = os.path.join(os.path.dirname(__file__), "..", "..", mod.CITY_FILE)
    path = os.path.normpath(path)
    cities: list[City] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.lower().startswith("name,"):
                continue
            parts = line.split(",")
            if len(parts) < 3:
                continue
            pop = 0
            if len(parts) >= 4:
                try:
                    pop = int(parts[3].strip())
                except ValueError:
                    pop = 50_000
            cities.append(City(
                name=parts[0].strip(),
                lat=float(parts[1].strip()),
                lon=float(parts[2].strip()),
                population=pop,
                country=country_code,
            ))
    return cities


PLZ_FILES = {
    "de": "data/plz_germany.csv",
    "at": "data/plz_austria.csv",
    "ch": "data/plz_switzerland.csv",
}


def load_plz_grid(country_code: str = "de") -> list[City]:
    """Load PLZ grid as pseudo-cities for a given country."""
    filename = PLZ_FILES.get(country_code)
    if not filename:
        return []
    path = os.path.join(os.path.dirname(__file__), "..", "..", filename)
    path = os.path.normpath(path)
    entries: list[City] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith(",lat"):
                continue
            parts = line.split(",")
            if len(parts) >= 3:
                entries.append(City(
                    name=parts[0].strip(),
                    lat=float(parts[1].strip()),
                    lon=float(parts[2].strip()),
                    population=0,
                    country=country_code,
                ))
    return entries


def resolve_cities(
    country: str,
    targeting_mode: str,
    regions: list[str] | None = None,
    cities: list[str] | None = None,
    center_lat: float | None = None,
    center_lng: float | None = None,
    radius_km: float | None = None,
    scrape_mode: str = "smart",
) -> list[City]:
    """
    Resolve targeting config into a concrete list of cities.

    targeting_mode: "country" | "regions" | "cities" | "radius"
    scrape_mode: "quick" | "smart" | "thorough" | "max"
    """
    # Worldwide countries: load cities, then apply targeting filters
    from app.geo.worldwide import is_worldwide, load_worldwide_cities
    if is_worldwide(country):
        ww_cities = load_worldwide_cities(country, scrape_mode)
        if targeting_mode == "radius":
            if center_lat is None or center_lng is None or radius_km is None:
                raise ValueError("Radius mode requires center_lat, center_lng, radius_km")
            ww_cities = [
                c for c in ww_cities
                if haversine_km(center_lat, center_lng, c.lat, c.lon) <= radius_km
            ]
        elif targeting_mode == "cities" and cities:
            city_names = set(c.lower() for c in cities)
            ww_cities = [c for c in ww_cities if c.name.lower() in city_names]
        ww_cities.sort(key=lambda c: c.population, reverse=True)
        return ww_cities

    min_pop = MIN_POP.get(scrape_mode, 10_000)

    # Max mode uses PLZ grid for countries that have one
    if scrape_mode == "max" and country in PLZ_FILES:
        all_locations = load_plz_grid(country)
    else:
        all_locations = load_cities(country)

    # Apply population filter
    if min_pop > 0:
        all_locations = [c for c in all_locations if c.population >= min_pop]

    if targeting_mode == "country":
        result = all_locations

    elif targeting_mode == "regions":
        if not regions:
            result = all_locations
        else:
            region_set = set(r.upper() for r in regions)
            result = [
                c for c in all_locations
                if get_region(c.lat, c.lon, country) in region_set
            ]

    elif targeting_mode == "cities":
        if not cities:
            result = all_locations
        else:
            city_names = set(c.lower() for c in cities)
            result = [
                c for c in all_locations
                if c.name.lower() in city_names
            ]

    elif targeting_mode == "radius":
        if center_lat is None or center_lng is None or radius_km is None:
            raise ValueError("Radius mode requires center_lat, center_lng, radius_km")
        result = [
            c for c in all_locations
            if haversine_km(center_lat, center_lng, c.lat, c.lon) <= radius_km
        ]

    else:
        raise ValueError(f"Unknown targeting_mode: {targeting_mode}")

    # Sort by population desc (biggest cities first)
    result.sort(key=lambda c: c.population, reverse=True)
    return result


def get_city_scrape_config(population: int) -> tuple[int, int]:
    """Returns (zoom_level, max_pages) based on city population.

    Uses the /maps endpoint which returns 20 results per page.  Google
    caps at ~120 results (6 pages) per query+location combo.

    Large grid cities (500k+) get 6 pages per point to avoid saturation
    in dense urban areas.  Smaller grid cities (100k+) get 3 pages since
    multiple grid points provide coverage.  Single-point cities also get
    6 pages since there's only one search origin.
    """
    if population >= 400_000:
        # Large grid cities: deep pagination to avoid saturation in dense areas
        return (16, 6)
    elif population >= 100_000:
        # Grid cities: multiple points compensate for shallow pagination
        return (16, 3)
    elif population >= 20_000:
        # Medium cities: single point, go deeper
        return (17, 6)
    else:
        # Small cities: single point, go deeper
        return (17, 6)


# ---------------------------------------------------------------------------
# Grid search for large cities
# ---------------------------------------------------------------------------

def _km_to_deg_lat(km: float) -> float:
    """Approximate km to degrees latitude."""
    return km / 111.0


def _km_to_deg_lon(km: float, lat: float) -> float:
    """Approximate km to degrees longitude at a given latitude."""
    import math
    return km / (111.0 * math.cos(math.radians(lat)))


@dataclass
class GridPoint:
    """A single coordinate point in a city grid."""
    lat: float
    lon: float


def generate_grid_points(city: City, spacing_km: float = 2.0) -> list[GridPoint]:
    """Generate a grid of search points around a city center.

    For cities over 100k population, creates a grid of coordinate points
    so we don't miss businesses far from the city center due to Google's
    proximity bias.  Smaller cities get a single center point.

    Returns a list of GridPoint; for small cities this is just the center.
    """
    pop = city.population

    if pop >= 1_000_000:
        # Megacity (Vienna): 10km radius grid → roughly 8×8 = ~80 points
        radius_km = 10.0
    elif pop >= 500_000:
        # Large city: ~6km radius grid → roughly 5×5 = ~25 points
        radius_km = 6.0
    elif pop >= 200_000:
        # Medium-large city: ~4km radius → roughly 4×4 = ~16 points
        radius_km = 4.0
    elif pop >= 100_000:
        # Medium city: ~3km radius → roughly 3×3 = ~9 points
        radius_km = 3.0
    else:
        # Small city: single center point is sufficient
        return [GridPoint(lat=city.lat, lon=city.lon)]

    dlat = _km_to_deg_lat(spacing_km)
    dlon = _km_to_deg_lon(spacing_km, city.lat)
    steps_lat = int(radius_km / spacing_km)
    steps_lon = int(radius_km / spacing_km)

    points: list[GridPoint] = []
    for i in range(-steps_lat, steps_lat + 1):
        for j in range(-steps_lon, steps_lon + 1):
            plat = city.lat + i * dlat
            plon = city.lon + j * dlon
            if haversine_km(city.lat, city.lon, plat, plon) <= radius_km:
                points.append(GridPoint(lat=plat, lon=plon))

    return points if points else [GridPoint(lat=city.lat, lon=city.lon)]
