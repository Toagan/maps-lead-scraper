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


# Known major cities keyed by (country_code_upper, lowercase_city_name) → population.
# Only cities >= ~100k that should get grid search.  Avoids false matches
# (e.g. "Portland" UK village matching "Portland" US city).
_MAJOR_CITIES: dict[tuple[str, str], int] = {
    # UK
    ("GB", "london"): 9_000_000, ("GB", "birmingham"): 1_150_000,
    ("GB", "glasgow"): 635_000, ("GB", "liverpool"): 580_000,
    ("GB", "sheffield"): 590_000, ("GB", "manchester"): 550_000,
    ("GB", "edinburgh"): 530_000, ("GB", "leeds"): 520_000,
    ("GB", "bristol"): 470_000, ("GB", "leicester"): 370_000,
    ("GB", "coventry"): 370_000, ("GB", "nottingham"): 330_000,
    ("GB", "newcastle upon tyne"): 300_000, ("GB", "stoke-on-trent"): 270_000,
    ("GB", "cardiff"): 360_000, ("GB", "belfast"): 340_000,
    ("GB", "bradford"): 540_000, ("GB", "wolverhampton"): 260_000,
    ("GB", "plymouth"): 260_000, ("GB", "derby"): 255_000,
    ("GB", "southampton"): 250_000, ("GB", "sunderland"): 175_000,
    ("GB", "aberdeen"): 200_000, ("GB", "portsmouth"): 210_000,
    ("GB", "swansea"): 240_000, ("GB", "reading"): 230_000,
    ("GB", "luton"): 215_000, ("GB", "brighton"): 230_000,
    ("GB", "middlesbrough"): 140_000,
    # US
    ("US", "new york"): 8_300_000, ("US", "los angeles"): 3_900_000,
    ("US", "chicago"): 2_700_000, ("US", "houston"): 2_300_000,
    ("US", "phoenix"): 1_600_000, ("US", "philadelphia"): 1_600_000,
    ("US", "san antonio"): 1_500_000, ("US", "san diego"): 1_400_000,
    ("US", "dallas"): 1_300_000, ("US", "san jose"): 1_000_000,
    ("US", "austin"): 1_000_000, ("US", "jacksonville"): 950_000,
    ("US", "fort worth"): 950_000, ("US", "columbus"): 900_000,
    ("US", "indianapolis"): 880_000, ("US", "charlotte"): 880_000,
    ("US", "san francisco"): 870_000, ("US", "seattle"): 750_000,
    ("US", "denver"): 715_000, ("US", "washington"): 690_000,
    ("US", "nashville"): 680_000, ("US", "oklahoma city"): 680_000,
    ("US", "el paso"): 680_000, ("US", "boston"): 675_000,
    ("US", "portland"): 650_000, ("US", "las vegas"): 640_000,
    ("US", "memphis"): 630_000, ("US", "louisville"): 620_000,
    ("US", "baltimore"): 580_000, ("US", "milwaukee"): 575_000,
    ("US", "albuquerque"): 560_000, ("US", "tucson"): 545_000,
    ("US", "fresno"): 540_000, ("US", "sacramento"): 525_000,
    ("US", "mesa"): 510_000, ("US", "kansas city"): 510_000,
    ("US", "atlanta"): 500_000, ("US", "miami"): 450_000,
    ("US", "minneapolis"): 430_000, ("US", "new orleans"): 390_000,
    ("US", "tampa"): 385_000, ("US", "pittsburgh"): 300_000,
    ("US", "detroit"): 640_000, ("US", "honolulu"): 350_000,
    # France
    ("FR", "paris"): 2_100_000, ("FR", "marseille"): 870_000,
    ("FR", "lyon"): 520_000, ("FR", "toulouse"): 490_000,
    ("FR", "nice"): 340_000, ("FR", "nantes"): 320_000,
    ("FR", "strasbourg"): 280_000, ("FR", "montpellier"): 290_000,
    ("FR", "bordeaux"): 260_000, ("FR", "lille"): 230_000,
    ("FR", "rennes"): 220_000,
    # Spain
    ("ES", "madrid"): 3_300_000, ("ES", "barcelona"): 1_600_000,
    ("ES", "valencia"): 790_000, ("ES", "seville"): 690_000,
    ("ES", "zaragoza"): 670_000, ("ES", "málaga"): 580_000,
    ("ES", "bilbao"): 350_000,
    # Italy
    ("IT", "rome"): 2_870_000, ("IT", "milan"): 1_370_000,
    ("IT", "naples"): 960_000, ("IT", "turin"): 870_000,
    ("IT", "palermo"): 670_000, ("IT", "genoa"): 580_000,
    ("IT", "bologna"): 390_000, ("IT", "florence"): 380_000,
    # Brazil
    ("BR", "são paulo"): 12_300_000, ("BR", "rio de janeiro"): 6_750_000,
    ("BR", "brasília"): 3_000_000, ("BR", "salvador"): 2_900_000,
    ("BR", "fortaleza"): 2_700_000, ("BR", "belo horizonte"): 2_500_000,
    ("BR", "manaus"): 2_200_000, ("BR", "curitiba"): 1_960_000,
    # India
    ("IN", "mumbai"): 12_400_000, ("IN", "delhi"): 11_000_000,
    ("IN", "bangalore"): 8_400_000, ("IN", "hyderabad"): 6_800_000,
    ("IN", "ahmedabad"): 5_600_000, ("IN", "chennai"): 4_600_000,
    ("IN", "kolkata"): 4_500_000, ("IN", "pune"): 3_100_000,
    ("IN", "jaipur"): 3_000_000,
    # Japan
    ("JP", "tokyo"): 13_900_000, ("JP", "yokohama"): 3_750_000,
    ("JP", "osaka"): 2_750_000, ("JP", "nagoya"): 2_300_000,
    ("JP", "sapporo"): 1_970_000, ("JP", "fukuoka"): 1_600_000,
    ("JP", "kobe"): 1_520_000, ("JP", "kyoto"): 1_460_000,
    # Australia
    ("AU", "sydney"): 5_300_000, ("AU", "melbourne"): 5_000_000,
    ("AU", "brisbane"): 2_500_000, ("AU", "perth"): 2_100_000,
    ("AU", "adelaide"): 1_400_000,
    # Canada
    ("CA", "toronto"): 2_930_000, ("CA", "montreal"): 1_780_000,
    ("CA", "calgary"): 1_340_000, ("CA", "ottawa"): 1_000_000,
    ("CA", "edmonton"): 1_000_000, ("CA", "vancouver"): 680_000,
    # Other major
    ("TR", "istanbul"): 15_500_000, ("TR", "ankara"): 5_700_000,
    ("TR", "izmir"): 2_900_000,
    ("RU", "moscow"): 12_600_000, ("RU", "saint petersburg"): 5_400_000,
    ("EG", "cairo"): 10_000_000, ("EG", "alexandria"): 5_200_000,
    ("KR", "seoul"): 9_700_000, ("KR", "busan"): 3_400_000,
    ("ID", "jakarta"): 10_500_000, ("ID", "surabaya"): 2_900_000,
    ("TH", "bangkok"): 10_500_000,
    ("MX", "mexico city"): 9_200_000, ("MX", "guadalajara"): 1_500_000,
    ("MX", "monterrey"): 1_100_000,
    ("CO", "bogotá"): 7_400_000, ("CO", "medellín"): 2_500_000,
    ("PE", "lima"): 9_750_000,
    ("AR", "buenos aires"): 3_000_000, ("AR", "córdoba"): 1_500_000,
    ("ZA", "johannesburg"): 5_600_000, ("ZA", "cape town"): 4_600_000,
    ("ZA", "durban"): 3_100_000,
    ("KE", "nairobi"): 4_400_000,
    ("NG", "lagos"): 15_400_000, ("NG", "abuja"): 3_300_000,
    ("SG", "singapore"): 5_700_000,
    ("MY", "kuala lumpur"): 1_800_000,
    ("NL", "amsterdam"): 870_000, ("NL", "rotterdam"): 650_000,
    ("NL", "the hague"): 545_000,
    ("PL", "warsaw"): 1_790_000, ("PL", "kraków"): 780_000,
    ("PL", "łódź"): 680_000, ("PL", "wrocław"): 640_000,
    ("RO", "bucharest"): 1_800_000,
    ("CZ", "prague"): 1_300_000, ("CZ", "brno"): 380_000,
    ("HU", "budapest"): 1_750_000,
    ("GR", "athens"): 660_000, ("GR", "thessaloniki"): 320_000,
    ("SE", "stockholm"): 980_000, ("SE", "gothenburg"): 580_000,
    ("SE", "malmö"): 320_000,
    ("DK", "copenhagen"): 630_000,
    ("FI", "helsinki"): 650_000,
    ("NO", "oslo"): 700_000,
    ("PT", "lisbon"): 550_000, ("PT", "porto"): 240_000,
    ("IE", "dublin"): 550_000,
    ("IL", "tel aviv"): 460_000, ("IL", "jerusalem"): 940_000,
    ("AE", "dubai"): 3_400_000, ("AE", "abu dhabi"): 1_500_000,
    ("SA", "riyadh"): 7_600_000, ("SA", "jeddah"): 4_600_000,
    ("TW", "taipei"): 2_600_000, ("TW", "kaohsiung"): 2_770_000,
    ("CN", "shanghai"): 24_800_000, ("CN", "beijing"): 21_500_000,
    ("CN", "guangzhou"): 13_500_000, ("CN", "shenzhen"): 12_400_000,
    ("VN", "ho chi minh city"): 8_900_000, ("VN", "hanoi"): 8_000_000,
    ("PH", "manila"): 1_800_000, ("PH", "quezon city"): 2_900_000,
    ("GH", "accra"): 2_500_000,
    ("CL", "santiago"): 6_200_000,
}


def _read_city_names_from_csv(csv_file: str, country_upper: str) -> set[str]:
    """Read all city names for a country from a CSV, keyed by 'name|lat' for uniqueness."""
    path = os.path.join(os.path.dirname(__file__), "..", "..", csv_file)
    path = os.path.normpath(path)
    keys: set[str] = set()
    if not os.path.exists(path):
        return keys
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["country"] == country_upper:
                keys.add(f"{row['name']}|{row['lat']}")
    return keys


def _estimate_population(country_upper: str, name: str, in_15k: bool, in_5k: bool) -> int:
    """Estimate city population from known majors list + file tier."""
    known = _MAJOR_CITIES.get((country_upper, name.lower()))
    if known:
        return known
    # File-tier heuristic: 15k file = at least 15k, 5k file = at least 5k
    if in_15k:
        return 30_000  # Conservative — won't trigger grid (needs 100k+)
    if in_5k:
        return 10_000
    return 2_000


def load_worldwide_cities(country_code: str, scrape_mode: str) -> list[City]:
    """Load cities for a country with estimated population.

    Population comes from the _MAJOR_CITIES lookup for large cities (which
    enables grid search), and from file-tier heuristics for the rest.
    """
    code = country_code.upper()
    csv_file = WORLD_CITY_FILES.get(scrape_mode, WORLD_CITY_FILES["smart"])
    path = os.path.join(os.path.dirname(__file__), "..", "..", csv_file)
    path = os.path.normpath(path)

    # Build tier lookup sets
    cities_15k = _read_city_names_from_csv(WORLD_CITY_FILES["quick"], code)
    cities_5k = _read_city_names_from_csv(WORLD_CITY_FILES["smart"], code)

    cities: list[City] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["country"] != code:
                continue
            key = f"{row['name']}|{row['lat']}"
            pop = _estimate_population(code, row["name"], key in cities_15k, key in cities_5k)
            cities.append(City(
                name=row["name"],
                lat=float(row["lat"]),
                lon=float(row["lng"]),
                population=pop,
            ))
    return cities


def list_available_countries() -> list[dict]:
    """Return list of available worldwide countries for the frontend."""
    return [
        {"code": code, "name": info["name"]}
        for code, info in sorted(COUNTRY_INFO.items(), key=lambda x: x[1]["name"])
    ]
