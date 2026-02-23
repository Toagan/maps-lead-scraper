#!/usr/bin/env python3
"""Generate PLZ grid CSVs and clean city files for Austria and Switzerland.

Downloads GeoNames postal code data (CC-BY licensed), deduplicates by postal
code, and outputs CSV files matching the format of data/plz_germany.csv.

Also regenerates city files with:
- Real population data for top cities (from census/official sources)
- Removal of district/subdivision entries
- Heuristic for unknown cities: keep existing non-placeholder pop, else 5000

Usage:
    python scripts/generate_plz_data.py
"""

from __future__ import annotations

import csv
import io
import os
import zipfile
from collections import defaultdict
from urllib.request import urlopen

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(BASE_DIR, "data")

GEONAMES_URL = "https://download.geonames.org/export/zip/{code}.zip"

# ---------------------------------------------------------------------------
# Real population data (census / official statistics)
# ---------------------------------------------------------------------------

AT_POPULATIONS: dict[str, int] = {
    "Vienna": 1_982_097,
    "Graz": 291_072,
    "Linz": 205_726,
    "Salzburg": 155_021,
    "Innsbruck": 131_961,
    "Klagenfurt am Wörthersee": 101_765,
    "Villach": 63_269,
    "Wels": 62_470,
    "Sankt Pölten": 55_878,
    "Dornbirn": 50_363,
    "Wiener Neustadt": 46_291,
    "Steyr": 38_603,
    "Feldkirch": 34_710,
    "Bregenz": 29_806,
    "Klosterneuburg": 27_759,
    "Baden": 26_274,
    "Wolfsberg": 25_105,
    "Leoben": 24_912,
    "Krems an der Donau": 24_610,
    "Traun": 24_417,
    "Amstetten": 23_816,
    "Lustenau": 23_581,
    "Hallein": 21_246,
    "Mödling": 20_840,
    "Traiskirchen": 19_513,
    "Schwechat": 19_033,
    "Stockerau": 16_944,
    "Telfs": 16_022,
    "Ternitz": 15_130,
    "Perchtoldsdorf": 14_867,
    "Eisenstadt": 14_816,
    "Bludenz": 14_808,
    "Hall in Tirol": 14_018,
    "Lienz": 11_864,
    "Knittelfeld": 11_272,
    "Tulln": 16_000,
    "Spittal an der Drau": 16_000,
    "Kufstein": 19_000,
    "Bruck an der Mur": 16_000,
    "Neunkirchen": 13_000,
    "Ried im Innkreis": 12_000,
    "Mistelbach": 11_800,
    "Schwaz": 13_800,
    "Gmunden": 13_200,
    "Hollabrunn": 11_800,
    "Korneuburg": 13_500,
    "Leibnitz": 12_200,
    "Judenburg": 9_200,
    "Voitsberg": 9_800,
    "Wörgl": 14_200,
    "Imst": 10_500,
    "Zell am See": 10_000,
    "Rankweil": 12_000,
    "Saalfelden am Steinernen Meer": 16_800,
    "Hohenems": 16_500,
    "Enns": 12_000,
    "Weiz": 12_200,
    "Liezen": 8_000,
    "Freistadt": 8_000,
    "Oberwart": 8_000,
}

CH_POPULATIONS: dict[str, int] = {
    "Zürich": 421_878,
    "Genève": 203_856,
    "Basel": 177_654,
    "Lausanne": 139_111,
    "Bern": 134_794,
    "Winterthur": 115_104,
    "Luzern": 82_620,
    "Sankt Gallen": 75_833,
    "Lugano": 63_932,
    "Biel/Bienne": 55_206,
    "Thun": 45_356,
    "Köniz": 43_332,
    "La Chaux-de-Fonds": 38_965,
    "Fribourg": 38_829,
    "Chur": 38_156,
    "Schaffhausen": 37_035,
    "Uster": 35_563,
    "Neuchâtel": 34_063,
    "Emmen": 31_291,
    "Zug": 30_934,
    "Yverdon-les-Bains": 30_451,
    "Dübendorf": 29_248,
    "Dietikon": 28_499,
    "Kriens": 28_194,
    "Montreux": 26_574,
    "Frauenfeld": 25_931,
    "Wetzikon": 25_167,
    "Baar": 25_052,
    "Wädenswil": 24_281,
    "Wil": 24_139,
    "Kreuzlingen": 22_982,
    "Horgen": 22_700,
    "Carouge": 22_600,
    "Bülach": 22_143,
    "Aarau": 21_712,
    "Allschwil": 21_485,
    "Wettingen": 21_000,
    "Vernier": 35_000,
    "Sitten": 35_000,
    "Renens": 22_000,
    "Nyon": 21_000,
    "Lancy": 33_000,
    "Onex": 19_000,
    "Meyrin": 25_000,
    "Baden": 19_548,
    "Reinach": 19_213,
    "Olten": 19_056,
    "Gossau": 18_987,
    "Bellinzona": 18_601,
    "Thalwil": 18_221,
    "Muttenz": 17_979,
    "Grenchen": 17_278,
    "Wohlen": 16_931,
    "Solothurn": 16_777,
    "Langenthal": 16_456,
    "Steffisburg": 16_202,
    "Locarno": 16_033,
    "Herisau": 15_718,
    "Arbon": 14_932,
    "Vevey": 20_000,
    "Pully": 18_000,
    "Morges": 16_000,
    "Monthey": 18_000,
    "Martigny-Ville": 18_000,
    "Kloten": 20_000,
    "Burgdorf": 16_500,
    "Bulle": 24_000,
    "Adliswil": 19_000,
    "Riehen": 21_000,
    "Sierre": 17_000,
    "Schwyz": 15_500,
    "Rapperswil": 27_000,
    "Jona": 19_000,
    "Arth": 12_407,
    "Davos": 10_861,
    "Schlieren": 20_000,
    "Einsiedeln": 15_500,
    "Cham": 17_000,
}

# ---------------------------------------------------------------------------
# District patterns to remove
# ---------------------------------------------------------------------------

AT_DISTRICT_PATTERNS = [
    # Vienna districts
    "Döbling", "Simmering", "Ottakring", "Meidling", "Penzing", "Hietzing",
    "Hernals", "Fünfhaus", "Floridsdorf", "Essling", "Aspern", "Favoriten",
    "Donaustadt", "Währing", "Landstraße", "Margareten", "Mariahilf",
    "Neubau", "Josefstadt", "Alsergrund", "Brigittenau", "Innere Stadt",
    # Graz districts
    "Wetzelsdorf", "Straßgang", "Sankt Peter", "Jakomini", "Lend",
    "Geidorf", "Sankt Leonhard", "Andritz", "Gries", "Eggenberg",
    "Waltendorf", "Puntigam", "Ries", "Liebenau", "Mariatrost",
    "Gösting", "Kirchenviertel",
    # Innsbruck districts
    "Wilten", "Pradl", "Hötting", "Mühlau", "Arzl", "Amras",
    # Klagenfurt districts
    "Sankt Martin", "Wölfnitz", "Viktring", "Annabichl", "Sankt Ruprecht",
    "Völkendorf", "Villacher Vorstadt", "Villach-Innere Stadt", "Auen", "Lind",
    # Other duplicates / subdivisions
    "Neu-Guntramsdorf", "Spratzern", "Lichtenegg", "Hafendorf", "Deuchendorf",
    "Oberhaid", "Hart", "Haid",
]

CH_DISTRICT_PATTERNS = [
    # Zürich Kreis entries
    "Zürich (Kreis",
    # Winterthur Kreis entries
    "Stadt Winterthur (Kreis",
    "Seen (Kreis",
    "Oberwinterthur (Kreis",
    "Wülflingen (Kreis",
    "Mattenbach (Kreis",
    "Veltheim (Kreis",
    "Töss (Kreis",
    # Subdivision entries with /
    " / ",
    # Other known subdivisions
    "Littau",
    "Viganello",
    "Pregassona",
    "Massagno",
    "Blécherette",
    "Les Avanchets",
    "Hegnau",
]


def _is_at_district(name: str) -> bool:
    return name in AT_DISTRICT_PATTERNS


def _is_ch_district(name: str) -> bool:
    for pat in CH_DISTRICT_PATTERNS:
        if pat in name:
            return True
    return False


# ---------------------------------------------------------------------------
# PLZ grid generation
# ---------------------------------------------------------------------------

def download_and_extract_plz(country_code: str) -> list[tuple[str, float, float]]:
    """Download GeoNames zip, extract postal code + lat/lng."""
    url = GEONAMES_URL.format(code=country_code)
    print(f"  Downloading {url} ...")
    resp = urlopen(url)
    data = resp.read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        txt_name = f"{country_code}.txt"
        with zf.open(txt_name) as f:
            lines = f.read().decode("utf-8").splitlines()

    entries: list[tuple[str, float, float]] = []
    for line in lines:
        parts = line.split("\t")
        if len(parts) < 10:
            continue
        postal_code = parts[1].strip()
        try:
            lat = float(parts[9])
            lng = float(parts[10])
        except (ValueError, IndexError):
            continue
        entries.append((postal_code, lat, lng))

    return entries


def deduplicate_plz(entries: list[tuple[str, float, float]]) -> list[tuple[str, float, float]]:
    """Average coordinates per postal code."""
    sums: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0])
    for code, lat, lng in entries:
        sums[code][0] += lat
        sums[code][1] += lng
        sums[code][2] += 1

    result = []
    for code in sorted(sums.keys()):
        s = sums[code]
        n = s[2]
        result.append((code, round(s[0] / n, 7), round(s[1] / n, 7)))
    return result


def write_plz_csv(entries: list[tuple[str, float, float]], filename: str) -> None:
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(",lat,lng\n")
        for code, lat, lng in entries:
            f.write(f"{code},{lat},{lng}\n")
    print(f"  Written {len(entries)} entries to {path}")


# ---------------------------------------------------------------------------
# City file regeneration
# ---------------------------------------------------------------------------

def load_existing_cities(filename: str) -> list[dict]:
    """Load existing city file entries."""
    path = os.path.join(DATA_DIR, filename)
    cities = []
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
                    pop = 0
            cities.append({
                "name": parts[0].strip(),
                "lat": parts[1].strip(),
                "lon": parts[2].strip(),
                "population": pop,
            })
    return cities


def regenerate_city_file(
    filename: str,
    pop_lookup: dict[str, int],
    is_district_fn,
    placeholder_pops: set[int] = frozenset({8000, 20000}),
) -> None:
    """Regenerate a city file with real populations and districts removed."""
    cities = load_existing_cities(filename)
    clean = []
    removed = []

    for c in cities:
        if is_district_fn(c["name"]):
            removed.append(c["name"])
            continue

        # Fix population
        if c["name"] in pop_lookup:
            c["population"] = pop_lookup[c["name"]]
        elif c["population"] in placeholder_pops:
            c["population"] = 5000

        clean.append(c)

    # Sort by population descending
    clean.sort(key=lambda c: c["population"], reverse=True)

    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("name,latitude,longitude,population\n")
        for c in clean:
            f.write(f"{c['name']},{c['lat']},{c['lon']},{c['population']}\n")

    print(f"  Written {len(clean)} cities to {path} (removed {len(removed)} districts)")
    if removed[:10]:
        print(f"    Sample removed: {removed[:10]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # --- PLZ grids ---
    print("Generating PLZ grids...")

    for code, filename in [("AT", "plz_austria.csv"), ("CH", "plz_switzerland.csv")]:
        raw = download_and_extract_plz(code)
        deduped = deduplicate_plz(raw)
        write_plz_csv(deduped, filename)

    # --- City files ---
    print("\nRegenerating city files...")

    print("\n  Austria:")
    regenerate_city_file("cities_at.txt", AT_POPULATIONS, _is_at_district)

    print("\n  Switzerland:")
    regenerate_city_file("cities_ch.txt", CH_POPULATIONS, _is_ch_district)

    print("\nDone!")


if __name__ == "__main__":
    main()
