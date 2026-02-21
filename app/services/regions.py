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
            ))
    return cities


def load_plz_grid() -> list[City]:
    """Load German PLZ grid as pseudo-cities."""
    path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "plz_germany.csv")
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
    min_pop = MIN_POP.get(scrape_mode, 10_000)

    # Max mode for Germany uses PLZ grid
    if scrape_mode == "max" and country == "de":
        all_locations = load_plz_grid()
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
    """Returns (zoom_level, max_pages) based on city population. Ported from v1."""
    if population >= 500_000:
        return (12, 6)
    elif population >= 200_000:
        return (13, 5)
    elif population >= 100_000:
        return (14, 4)
    elif population >= 50_000:
        return (14, 3)
    elif population >= 20_000:
        return (15, 2)
    else:
        return (15, 1)
