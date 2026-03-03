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
    # US — all 280+ cities with population >= 100k (2024 Census estimates)
    # For duplicate city names, largest population is used so all instances get grid search
    ("US", "new york"): 8_350_000, ("US", "los angeles"): 3_870_000,
    ("US", "chicago"): 2_710_000, ("US", "houston"): 2_435_000,
    ("US", "phoenix"): 1_700_000, ("US", "san antonio"): 1_570_000,
    ("US", "philadelphia"): 1_560_000, ("US", "san diego"): 1_415_000,
    ("US", "dallas"): 1_340_000, ("US", "fort worth"): 1_050_000,
    ("US", "jacksonville"): 1_040_000, ("US", "austin"): 1_010_000,
    ("US", "san jose"): 990_000, ("US", "charlotte"): 978_000,
    ("US", "columbus"): 947_000, ("US", "indianapolis"): 894_000,
    ("US", "san francisco"): 804_000, ("US", "seattle"): 801_000,
    ("US", "denver"): 735_000, ("US", "oklahoma city"): 728_000,
    ("US", "washington"): 718_000, ("US", "nashville"): 713_000,
    ("US", "las vegas"): 695_000, ("US", "boston"): 691_000,
    ("US", "el paso"): 683_000, ("US", "detroit"): 649_000,
    ("US", "louisville"): 645_000, ("US", "portland"): 627_000,
    ("US", "memphis"): 602_000, ("US", "baltimore"): 561_000,
    ("US", "tucson"): 560_000, ("US", "albuquerque"): 558_000,
    ("US", "milwaukee"): 557_000, ("US", "fresno"): 554_000,
    ("US", "sacramento"): 541_000, ("US", "atlanta"): 530_000,
    ("US", "mesa"): 523_000, ("US", "kansas city"): 520_000,
    ("US", "raleigh"): 517_000, ("US", "miami"): 509_000,
    ("US", "colorado springs"): 499_000, ("US", "omaha"): 488_000,
    ("US", "virginia beach"): 452_000, ("US", "oakland"): 445_000,
    ("US", "long beach"): 444_000, ("US", "tampa"): 428_000,
    ("US", "minneapolis"): 427_000, ("US", "bakersfield"): 424_000,
    ("US", "tulsa"): 416_000, ("US", "aurora"): 411_000,
    ("US", "arlington"): 408_000, ("US", "wichita"): 402_000,
    ("US", "henderson"): 366_000, ("US", "cleveland"): 362_000,
    ("US", "new orleans"): 352_000, ("US", "orlando"): 348_000,
    ("US", "anaheim"): 344_000, ("US", "honolulu"): 343_000,
    ("US", "newark"): 333_000, ("US", "lexington"): 333_000,
    ("US", "riverside"): 328_000, ("US", "stockton"): 327_000,
    ("US", "irvine"): 324_000, ("US", "santa ana"): 319_000,
    ("US", "cincinnati"): 318_000, ("US", "corpus christi"): 317_000,
    ("US", "greensboro"): 313_000, ("US", "jersey city"): 312_000,
    ("US", "durham"): 312_000, ("US", "north las vegas"): 310_000,
    ("US", "pittsburgh"): 310_000, ("US", "saint paul"): 306_000,
    ("US", "lincoln"): 305_000, ("US", "gilbert"): 298_000,
    ("US", "plano"): 297_000, ("US", "madison"): 291_000,
    ("US", "reno"): 290_000, ("US", "anchorage"): 289_000,
    ("US", "port saint lucie"): 284_000, ("US", "chandler"): 283_000,
    ("US", "chula vista"): 280_000, ("US", "lubbock"): 279_000,
    ("US", "fort wayne"): 278_000, ("US", "buffalo"): 276_000,
    ("US", "st. petersburg"): 271_000, ("US", "st. louis"): 269_000,
    ("US", "laredo"): 264_000, ("US", "toledo"): 263_000,
    ("US", "glendale"): 263_000, ("US", "enterprise"): 261_000,
    ("US", "winston-salem"): 259_000, ("US", "irving"): 259_000,
    ("US", "chesapeake"): 258_000, ("US", "garland"): 252_000,
    ("US", "cape coral"): 252_000, ("US", "frisco"): 251_000,
    ("US", "scottsdale"): 248_000, ("US", "mckinney"): 243_000,
    ("US", "hialeah"): 242_000, ("US", "boise"): 239_000,
    ("US", "huntsville"): 237_000, ("US", "richmond"): 237_000,
    ("US", "tacoma"): 232_000, ("US", "spokane"): 231_000,
    ("US", "spring valley"): 229_000, ("US", "norfolk"): 228_000,
    ("US", "santa clarita"): 228_000, ("US", "fremont"): 227_000,
    ("US", "salt lake city"): 226_000, ("US", "san bernardino"): 226_000,
    ("US", "fontana"): 223_000, ("US", "modesto"): 222_000,
    ("US", "baton rouge"): 218_000, ("US", "sioux falls"): 217_000,
    ("US", "moreno valley"): 216_000, ("US", "worcester"): 215_000,
    ("US", "grand prairie"): 213_000, ("US", "des moines"): 213_000,
    ("US", "yonkers"): 211_000, ("US", "fayetteville"): 209_000,
    ("US", "tallahassee"): 209_000, ("US", "little rock"): 206_000,
    ("US", "rochester"): 205_000, ("US", "overland park"): 205_000,
    ("US", "amarillo"): 205_000, ("US", "peoria"): 204_000,
    ("US", "vancouver"): 203_000, ("US", "knoxville"): 202_000,
    ("US", "augusta"): 202_000, ("US", "sunrise manor"): 201_000,
    ("US", "grand rapids"): 201_000, ("US", "oxnard"): 200_000,
    ("US", "mobile"): 199_000, ("US", "providence"): 197_000,
    ("US", "chattanooga"): 196_000, ("US", "clarksville"): 195_000,
    ("US", "brownsville"): 195_000, ("US", "fort lauderdale"): 195_000,
    ("US", "birmingham"): 194_000, ("US", "montgomery"): 194_000,
    ("US", "tempe"): 193_000, ("US", "huntington beach"): 191_000,
    ("US", "ontario"): 190_000, ("US", "akron"): 189_000,
    ("US", "cary"): 187_000, ("US", "elk grove"): 186_000,
    ("US", "pembroke pines"): 183_000, ("US", "salem"): 183_000,
    ("US", "newport news"): 182_000, ("US", "surprise"): 179_000,
    ("US", "eugene"): 179_000, ("US", "denton"): 178_000,
    ("US", "rancho cucamonga"): 178_000, ("US", "paradise"): 178_000,
    ("US", "santa rosa"): 177_000, ("US", "murfreesboro"): 176_000,
    ("US", "garden grove"): 173_000, ("US", "shreveport"): 171_000,
    ("US", "fort collins"): 171_000, ("US", "springfield"): 171_000,
    ("US", "roseville"): 171_000, ("US", "oceanside"): 169_000,
    ("US", "lancaster"): 165_000, ("US", "paterson"): 164_000,
    ("US", "killeen"): 164_000, ("US", "corona"): 164_000,
    ("US", "hollywood"): 162_000, ("US", "charleston"): 161_000,
    ("US", "salinas"): 159_000, ("US", "palmdale"): 159_000,
    ("US", "alexandria"): 159_000, ("US", "sunnyvale"): 158_000,
    ("US", "lakewood"): 157_000, ("US", "macon"): 157_000,
    ("US", "hayward"): 156_000, ("US", "bellevue"): 156_000,
    ("US", "naperville"): 155_000, ("US", "bridgeport"): 154_000,
    ("US", "palm bay"): 153_000, ("US", "olathe"): 153_000,
    ("US", "joliet"): 153_000, ("US", "mcallen"): 152_000,
    ("US", "gainesville"): 152_000, ("US", "mesquite"): 150_000,
    ("US", "meridian"): 150_000, ("US", "waco"): 150_000,
    ("US", "savannah"): 149_000, ("US", "columbia"): 149_000,
    ("US", "thornton"): 149_000, ("US", "midland"): 149_000,
    ("US", "pasadena"): 149_000, ("US", "visalia"): 149_000,
    ("US", "escondido"): 148_000, ("US", "miramar"): 148_000,
    ("US", "rockford"): 147_000, ("US", "pomona"): 146_000,
    ("US", "syracuse"): 146_000, ("US", "elizabeth"): 145_000,
    ("US", "coral springs"): 144_000, ("US", "victorville"): 144_000,
    ("US", "round rock"): 143_000, ("US", "new haven"): 142_000,
    ("US", "fargo"): 141_000, ("US", "stamford"): 141_000,
    ("US", "lewisville"): 141_000, ("US", "fullerton"): 138_000,
    ("US", "cedar rapids"): 138_000, ("US", "hampton"): 138_000,
    ("US", "west valley city"): 137_000, ("US", "orange"): 137_000,
    ("US", "warren"): 137_000, ("US", "lehigh acres"): 137_000,
    ("US", "kent"): 137_000, ("US", "carrollton"): 136_000,
    ("US", "torrance"): 136_000, ("US", "dayton"): 136_000,
    ("US", "jackson"): 136_000, ("US", "santa clara"): 135_000,
    ("US", "sterling heights"): 134_000, ("US", "abilene"): 133_000,
    ("US", "west palm beach"): 133_000, ("US", "norman"): 132_000,
    ("US", "college station"): 132_000, ("US", "clovis"): 132_000,
    ("US", "pearland"): 131_000, ("US", "north charleston"): 131_000,
    ("US", "lakeland"): 131_000, ("US", "wilmington"): 130_000,
    ("US", "athens"): 129_000, ("US", "new braunfels"): 129_000,
    ("US", "goodyear"): 128_000, ("US", "allentown"): 128_000,
    ("US", "broken arrow"): 127_000, ("US", "conroe"): 126_000,
    ("US", "simi valley"): 125_000, ("US", "nampa"): 125_000,
    ("US", "topeka"): 125_000, ("US", "cambridge"): 125_000,
    ("US", "buckeye"): 125_000, ("US", "fairfield"): 124_000,
    ("US", "menifee"): 124_000, ("US", "billings"): 124_000,
    ("US", "spring hill"): 123_000, ("US", "concord"): 123_000,
    ("US", "hartford"): 123_000, ("US", "lowell"): 123_000,
    ("US", "thousand oaks"): 123_000, ("US", "lafayette"): 123_000,
    ("US", "ann arbor"): 123_000, ("US", "vallejo"): 122_000,
    ("US", "the woodlands"): 122_000, ("US", "odessa"): 122_000,
    ("US", "pompano beach"): 121_000, ("US", "independence"): 121_000,
    ("US", "arvada"): 121_000, ("US", "high point"): 121_000,
    ("US", "berkeley"): 121_000, ("US", "league city"): 120_000,
    ("US", "antioch"): 120_000, ("US", "las cruces"): 120_000,
    ("US", "brandon"): 119_000, ("US", "tuscaloosa"): 119_000,
    ("US", "miami gardens"): 119_000, ("US", "allen"): 118_000,
    ("US", "manchester"): 118_000, ("US", "richardson"): 118_000,
    ("US", "georgetown"): 118_000, ("US", "waterbury"): 117_000,
    ("US", "greeley"): 117_000, ("US", "rio rancho"): 117_000,
    ("US", "clearwater"): 117_000, ("US", "west jordan"): 117_000,
    ("US", "riverview"): 116_000, ("US", "provo"): 116_000,
    ("US", "palm coast"): 115_000, ("US", "lansing"): 115_000,
    ("US", "tyler"): 115_000, ("US", "davie"): 115_000,
    ("US", "elgin"): 115_000, ("US", "westminster"): 115_000,
    ("US", "evansville"): 114_000, ("US", "everett"): 114_000,
    ("US", "south fulton"): 114_000, ("US", "temecula"): 114_000,
    ("US", "murrieta"): 113_000, ("US", "edison"): 113_000,
    ("US", "carlsbad"): 113_000, ("US", "sparks"): 113_000,
    ("US", "edinburg"): 113_000, ("US", "santa maria"): 112_000,
    ("US", "hillsboro"): 112_000, ("US", "beaumont"): 112_000,
    ("US", "saint george"): 111_000, ("US", "pueblo"): 111_000,
    ("US", "spokane valley"): 111_000, ("US", "bend"): 111_000,
    ("US", "gresham"): 110_000, ("US", "jurupa valley"): 110_000,
    ("US", "ventura"): 109_000, ("US", "sugar land"): 109_000,
    ("US", "centennial"): 109_000, ("US", "lee's summit"): 109_000,
    ("US", "costa mesa"): 108_000, ("US", "suffolk"): 107_000,
    ("US", "downey"): 107_000, ("US", "brockton"): 107_000,
    ("US", "yuma"): 107_000, ("US", "fort myers"): 106_000,
    ("US", "fishers"): 106_000, ("US", "boulder"): 106_000,
    ("US", "green bay"): 106_000, ("US", "west covina"): 106_000,
    ("US", "quincy"): 106_000, ("US", "carmel"): 105_000,
    ("US", "rialto"): 105_000, ("US", "plantation"): 105_000,
    ("US", "renton"): 105_000, ("US", "vacaville"): 105_000,
    ("US", "dearborn"): 105_000, ("US", "boca raton"): 104_000,
    ("US", "sandy springs"): 104_000, ("US", "south bend"): 104_000,
    ("US", "deltona"): 104_000, ("US", "tracy"): 103_000,
    ("US", "temple"): 103_000, ("US", "hesperia"): 103_000,
    ("US", "chico"): 103_000, ("US", "albany"): 103_000,
    ("US", "highlands ranch"): 103_000, ("US", "el monte"): 102_000,
    ("US", "wichita falls"): 102_000, ("US", "new bedford"): 102_000,
    ("US", "el cajon"): 102_000, ("US", "burbank"): 102_000,
    ("US", "lehi"): 102_000, ("US", "san mateo"): 102_000,
    ("US", "sunrise"): 102_000, ("US", "north port"): 102_000,
    ("US", "edmond"): 101_000, ("US", "toms river"): 101_000,
    ("US", "canton"): 101_000, ("US", "leander"): 101_000,
    ("US", "merced"): 101_000, ("US", "davenport"): 101_000,
    ("US", "inglewood"): 100_000, ("US", "san angelo"): 100_000,
    ("US", "avondale"): 100_000, ("US", "longmont"): 100_000,
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
