# Swiss Kantone with bounding boxes
# Format: (min_lat, max_lat, min_lon, max_lon)

REGIONS = {
    "ZH": {"name": "Zürich", "bounds": (47.15, 47.70, 8.35, 8.99)},
    "BE": {"name": "Bern", "bounds": (46.33, 47.35, 6.86, 8.46)},
    "LU": {"name": "Luzern", "bounds": (46.77, 47.27, 7.83, 8.52)},
    "UR": {"name": "Uri", "bounds": (46.41, 46.93, 8.39, 8.93)},
    "SZ": {"name": "Schwyz", "bounds": (46.90, 47.22, 8.50, 8.99)},
    "OW": {"name": "Obwalden", "bounds": (46.70, 46.98, 8.06, 8.44)},
    "NW": {"name": "Nidwalden", "bounds": (46.77, 47.01, 8.23, 8.58)},
    "GL": {"name": "Glarus", "bounds": (46.79, 47.15, 8.81, 9.23)},
    "ZG": {"name": "Zug", "bounds": (47.06, 47.25, 8.41, 8.62)},
    "FR": {"name": "Fribourg", "bounds": (46.49, 46.98, 6.74, 7.36)},
    "SO": {"name": "Solothurn", "bounds": (47.08, 47.39, 7.33, 7.93)},
    "BS": {"name": "Basel-Stadt", "bounds": (47.52, 47.59, 7.55, 7.68)},
    "BL": {"name": "Basel-Landschaft", "bounds": (47.34, 47.56, 7.32, 7.79)},
    "SH": {"name": "Schaffhausen", "bounds": (47.64, 47.81, 8.40, 8.86)},
    "AR": {"name": "Appenzell Ausserrhoden", "bounds": (47.31, 47.47, 9.18, 9.58)},
    "AI": {"name": "Appenzell Innerrhoden", "bounds": (47.25, 47.40, 9.34, 9.58)},
    "SG": {"name": "St. Gallen", "bounds": (46.87, 47.53, 8.80, 9.67)},
    "GR": {"name": "Graubünden", "bounds": (46.17, 47.07, 8.65, 10.49)},
    "AG": {"name": "Aargau", "bounds": (47.13, 47.62, 7.71, 8.46)},
    "TG": {"name": "Thurgau", "bounds": (47.38, 47.70, 8.75, 9.48)},
    "TI": {"name": "Ticino", "bounds": (45.82, 46.64, 8.38, 9.18)},
    "VD": {"name": "Vaud", "bounds": (46.21, 46.88, 6.07, 7.12)},
    "VS": {"name": "Valais", "bounds": (45.85, 46.66, 6.77, 8.47)},
    "NE": {"name": "Neuchâtel", "bounds": (46.83, 47.10, 6.45, 6.99)},
    "GE": {"name": "Genève", "bounds": (46.13, 46.37, 5.95, 6.31)},
    "JU": {"name": "Jura", "bounds": (47.15, 47.50, 6.85, 7.35)},
}

# (lat, lon, canton_code, tolerance)
# Fixes for SG cities that fall inside AR/AI bounding boxes
BORDER_OVERRIDES = [
    (47.4239, 9.3748, "SG", 0.02),   # Sankt Gallen
    (47.4155, 9.2548, "SG", 0.02),   # Gossau
    (47.4611, 9.3860, "SG", 0.02),   # Wittenbach
    (47.3209, 9.5681, "SG", 0.02),   # Oberriet
    (47.3777, 9.5475, "SG", 0.02),   # Altstätten
    (47.4668, 9.5664, "SG", 0.02),   # Thal
    (47.3649, 7.3445, "JU", 0.02),   # Delémont
]

COUNTRY_CODE = "ch"
SERPER_GL = "ch"
SERPER_HL = "de"
CITY_FILE = "data/cities_ch.txt"
