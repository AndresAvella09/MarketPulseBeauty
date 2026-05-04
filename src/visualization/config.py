from pathlib import Path
import os

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data" / "local" / "gold"

PRODUCTS_PATH = Path(os.getenv("MPB_PRODUCTS_PATH", DATA_DIR / "products.parquet"))
REVIEWS_PATH = Path(os.getenv("MPB_REVIEWS_PATH", DATA_DIR / "reviews.parquet"))
TRENDS_PATH = Path(os.getenv("MPB_TRENDS_PATH", DATA_DIR / "trends.parquet"))