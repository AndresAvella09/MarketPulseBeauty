from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data" / "local" / "silver"

PRODUCTS_PATH = DATA_DIR / "products.parquet"
REVIEWS_PATH = DATA_DIR / "reviews.parquet"