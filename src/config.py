CATALOG = "brewery_prod" 

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

BRONZE_VOLUME = "bronze"
SILVER_VOLUME = "silver"
GOLD_VOLUME = "gold"

BRONZE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}"
SILVER_PATH = f"/Volumes/{CATALOG}/{SILVER_SCHEMA}/{SILVER_VOLUME}"
GOLD_PATH   = f"/Volumes/{CATALOG}/{GOLD_SCHEMA}/{GOLD_VOLUME}"

OPENBREWERYDB_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200