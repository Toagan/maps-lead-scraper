# German Bundesländer with bounding boxes
# Format: (min_lat, max_lat, min_lon, max_lon)
# Ported from v1, with refined boxes to minimize overlaps at state borders

REGIONS = {
    "BY": {"name": "Bavaria (Bayern)", "bounds": (47.27, 50.57, 9.87, 13.84)},
    "BW": {"name": "Baden-Württemberg", "bounds": (47.53, 49.79, 7.51, 10.50)},
    "BE": {"name": "Berlin", "bounds": (52.33, 52.68, 13.08, 13.77)},
    "BB": {"name": "Brandenburg", "bounds": (51.36, 53.56, 11.26, 14.77)},
    "HB": {"name": "Bremen", "bounds": (53.01, 53.61, 8.48, 8.99)},
    "HH": {"name": "Hamburg", "bounds": (53.39, 53.74, 9.73, 10.33)},
    "HE": {"name": "Hesse (Hessen)", "bounds": (49.39, 51.66, 8.20, 10.24)},
    "MV": {"name": "Mecklenburg-Vorpommern", "bounds": (53.11, 54.69, 10.59, 14.41)},
    "NI": {"name": "Lower Saxony (Niedersachsen)", "bounds": (51.29, 53.89, 6.65, 11.60)},
    "NW": {"name": "North Rhine-Westphalia (NRW)", "bounds": (50.32, 52.53, 5.87, 9.46)},
    "RP": {"name": "Rhineland-Palatinate", "bounds": (48.97, 50.94, 6.11, 8.50)},
    "SL": {"name": "Saarland", "bounds": (49.11, 49.64, 6.36, 7.41)},
    "SN": {"name": "Saxony (Sachsen)", "bounds": (50.17, 51.69, 11.87, 15.04)},
    "ST": {"name": "Saxony-Anhalt", "bounds": (50.94, 53.04, 10.56, 12.10)},
    "SH": {"name": "Schleswig-Holstein", "bounds": (53.36, 55.06, 8.31, 11.31)},
    "TH": {"name": "Thuringia (Thüringen)", "bounds": (50.20, 51.65, 9.87, 12.65)},
}

# Border city coordinate overrides for ambiguous locations
BORDER_OVERRIDES = [
    (50.08, 8.24, "HE", 0.03),   # Wiesbaden
    (49.99, 8.25, "RP", 0.03),   # Mainz
    (49.79, 9.95, "BY", 0.05),   # Würzburg
    (49.87, 10.88, "BY", 0.05),  # Schweinfurt
]

COUNTRY_CODE = "de"
SERPER_GL = "de"
SERPER_HL = "de"
CITY_FILE = "data/cities_de.txt"
