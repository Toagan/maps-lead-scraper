"""Worldwide city support using the joelacus/world-cities dataset."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass

from app.services.regions import City

DACH_CODES = {"de", "at", "ch"}

# Map scrape_mode to the appropriate CSV file (by population threshold)
WORLD_CITY_FILES = {
    "quick": "data/world_cities_15000.csv",      # 15k+ population
    "smart": "data/world_cities_5000.csv",        # 5k+ population
    "thorough": "data/world_cities.csv",          # 1k+ population
    "max": "data/world_cities.csv",               # 1k+ population (same file)
}

# Major countries with Serper hl (language) codes
COUNTRY_INFO: dict[str, dict] = {
    "us": {"name": "United States", "hl": "en"},
    "gb": {"name": "United Kingdom", "hl": "en"},
    "ca": {"name": "Canada", "hl": "en"},
    "au": {"name": "Australia", "hl": "en"},
    "nz": {"name": "New Zealand", "hl": "en"},
    "ie": {"name": "Ireland", "hl": "en"},
    "za": {"name": "South Africa", "hl": "en"},
    "in": {"name": "India", "hl": "en"},
    "sg": {"name": "Singapore", "hl": "en"},
    "ph": {"name": "Philippines", "hl": "en"},
    "ng": {"name": "Nigeria", "hl": "en"},
    "ke": {"name": "Kenya", "hl": "en"},
    "gh": {"name": "Ghana", "hl": "en"},
    "fr": {"name": "France", "hl": "fr"},
    "be": {"name": "Belgium", "hl": "fr"},
    "lu": {"name": "Luxembourg", "hl": "fr"},
    "es": {"name": "Spain", "hl": "es"},
    "mx": {"name": "Mexico", "hl": "es"},
    "ar": {"name": "Argentina", "hl": "es"},
    "co": {"name": "Colombia", "hl": "es"},
    "cl": {"name": "Chile", "hl": "es"},
    "pe": {"name": "Peru", "hl": "es"},
    "it": {"name": "Italy", "hl": "it"},
    "pt": {"name": "Portugal", "hl": "pt"},
    "br": {"name": "Brazil", "hl": "pt"},
    "nl": {"name": "Netherlands", "hl": "nl"},
    "se": {"name": "Sweden", "hl": "sv"},
    "no": {"name": "Norway", "hl": "no"},
    "dk": {"name": "Denmark", "hl": "da"},
    "fi": {"name": "Finland", "hl": "fi"},
    "pl": {"name": "Poland", "hl": "pl"},
    "cz": {"name": "Czech Republic", "hl": "cs"},
    "sk": {"name": "Slovakia", "hl": "sk"},
    "hu": {"name": "Hungary", "hl": "hu"},
    "ro": {"name": "Romania", "hl": "ro"},
    "bg": {"name": "Bulgaria", "hl": "bg"},
    "hr": {"name": "Croatia", "hl": "hr"},
    "rs": {"name": "Serbia", "hl": "sr"},
    "gr": {"name": "Greece", "hl": "el"},
    "tr": {"name": "Turkey", "hl": "tr"},
    "ru": {"name": "Russia", "hl": "ru"},
    "ua": {"name": "Ukraine", "hl": "uk"},
    "il": {"name": "Israel", "hl": "he"},
    "ae": {"name": "United Arab Emirates", "hl": "ar"},
    "sa": {"name": "Saudi Arabia", "hl": "ar"},
    "eg": {"name": "Egypt", "hl": "ar"},
    "jp": {"name": "Japan", "hl": "ja"},
    "kr": {"name": "South Korea", "hl": "ko"},
    "cn": {"name": "China", "hl": "zh"},
    "tw": {"name": "Taiwan", "hl": "zh"},
    "th": {"name": "Thailand", "hl": "th"},
    "vn": {"name": "Vietnam", "hl": "vi"},
    "id": {"name": "Indonesia", "hl": "id"},
    "my": {"name": "Malaysia", "hl": "ms"},
}


def is_worldwide(country_code: str) -> bool:
    return country_code.lower() not in DACH_CODES


def get_serper_params(country_code: str) -> tuple[str, str]:
    """Return (gl, hl) for Serper API."""
    code = country_code.lower()
    info = COUNTRY_INFO.get(code)
    hl = info["hl"] if info else "en"
    return (code, hl)


def get_country_name(country_code: str) -> str:
    code = country_code.lower()
    info = COUNTRY_INFO.get(code)
    return info["name"] if info else code.upper()


def load_worldwide_cities(country_code: str, scrape_mode: str) -> list[City]:
    """Load cities for a country from the appropriate world-cities CSV."""
    code = country_code.upper()
    csv_file = WORLD_CITY_FILES.get(scrape_mode, WORLD_CITY_FILES["smart"])
    path = os.path.join(os.path.dirname(__file__), "..", "..", csv_file)
    path = os.path.normpath(path)

    cities: list[City] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["country"] != code:
                continue
            cities.append(City(
                name=row["name"],
                lat=float(row["lat"]),
                lon=float(row["lng"]),
                population=0,
            ))
    return cities


def list_available_countries() -> list[dict]:
    """Return list of available worldwide countries for the frontend."""
    return [
        {"code": code, "name": info["name"]}
        for code, info in sorted(COUNTRY_INFO.items(), key=lambda x: x[1]["name"])
    ]
