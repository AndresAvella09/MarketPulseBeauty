import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file (if needed)
# ── Input ──────────────────────────────────────────────────────────────────────
SITEMAP_LOCAL_PATH = "data/raw/xml/products-sitemap.xml"   # path to your local sitemap file
 
# ── Bazaarvoice API ────────────────────────────────────────────────────────────
BAZAARVOICE_URL    = "https://api.bazaarvoice.com/data/reviews.json"
BAZAARVOICE_PASSKEY = os.getenv("BAZAARVOICE_PASSKEY")
 
# ── Filtering ──────────────────────────────────────────────────────────────────
# Only scrape product URLs containing at least one of these keywords (case-insensitive).
# Set to [] to scrape ALL products in the sitemap.
TARGET_KEYWORDS = []   # e.g. ["hair", "shampoo", "conditioner"]
 
# ── Pagination / Limits ────────────────────────────────────────────────────────
PAGE_SIZE              = 100   # max allowed by BV API
MAX_REVIEWS_PER_PRODUCT = 5000 # safety cap per product (set to a huge number to get all)
 
# ── Performance ────────────────────────────────────────────────────────────────
MAX_WORKERS  = 8      # parallel threads — be polite, keep ≤ 10
DELAY_SECS   = 0.3    # sleep between paginated calls for the same product
 
# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_FILE = "data/raw/csv/sephora.csv"
 