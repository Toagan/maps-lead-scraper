# Austrian Bundesländer with bounding boxes
# Format: (min_lat, max_lat, min_lon, max_lon)

REGIONS = {
    "W":  {"name": "Wien", "bounds": (48.12, 48.32, 16.18, 16.58)},
    "NÖ": {"name": "Niederösterreich", "bounds": (47.40, 48.97, 14.45, 17.07)},
    "OÖ": {"name": "Oberösterreich", "bounds": (47.46, 48.77, 13.14, 15.05)},
    "S":  {"name": "Salzburg", "bounds": (46.95, 48.00, 12.30, 13.76)},
    "T":  {"name": "Tirol", "bounds": (46.65, 47.75, 10.10, 12.97)},
    "V":  {"name": "Vorarlberg", "bounds": (46.84, 47.59, 9.53, 10.24)},
    "K":  {"name": "Kärnten", "bounds": (46.37, 47.13, 12.65, 15.05)},
    "ST": {"name": "Steiermark", "bounds": (46.61, 47.83, 13.56, 16.17)},
    "B":  {"name": "Burgenland", "bounds": (46.85, 48.12, 16.00, 17.17)},
}

BORDER_OVERRIDES = []

COUNTRY_CODE = "at"
SERPER_GL = "at"
SERPER_HL = "de"
CITY_FILE = "data/cities_at.txt"
