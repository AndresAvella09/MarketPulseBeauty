import re
import time
import json
import requests
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=None,
                    help="Limit number of products to scrape reviews (dev mode)")
args = parser.parse_args()

from dotenv import load_dotenv
load_dotenv()

# ── Load product IDs ──────────────────────────────────────────────────────────
with open('data/raw/links/product_links.txt', 'r') as f:
    all_links = [line.strip() for line in f if line.strip()]

unique_pids = list(dict.fromkeys(
    re.findall(r'P[0-9]{4,9}', link)[0]
    for link in all_links
    if re.findall(r'P[0-9]{4,9}', link)
))
print(f"Total unique products to scrape: {len(unique_pids)}")

if args.limit:
    print(f"Development mode: limiting to first {args.limit} products\n")
    unique_pids = unique_pids[:args.limit]


# ── Bazaarvoice API ───────────────────────────────────────────────────────────
BV_URL = 'https://api.bazaarvoice.com/data/reviews.json'  # ✅ Fixed: removed /scraping/
BV_PASSKEY = os.getenv("BV_PASSKEY")  # Must be set in .env file

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

def scrape_reviews(p_id, proxy=None):
    """Fetch all reviews for a product from Bazaarvoice. Returns (product_dict, reviews_list)."""
    proxies = {"http": proxy, "https": proxy} if proxy else None
    params = {
        'Filter':     f'ProductId:{p_id.lower()}',
        'Sort':       'Helpfulness:desc',
        'Limit':      100,
        'Offset':     0,
        'Include':    'Products,Comments',
        'Stats':      'Reviews',
        'passkey':    BV_PASSKEY,
        'apiversion': '5.4',
    }

    reviews = []
    product = {}

    while True:
        params['Offset'] = len(reviews)
        try:
            r = requests.get(BV_URL, params=params, headers=HEADERS, proxies=proxies, timeout=15)
            if r.status_code != 200:
                print(f"  HTTP {r.status_code} for {p_id}")
                return None, None
            data = r.json()
        except Exception as e:
            print(f"  Error fetching {p_id}: {e}")
            return None, None

        if data.get('HasErrors'):
            errors = data.get('Errors', [])
            for e in errors:
                print(f"  BV API Error: {e.get('Message')} ({e.get('Code')})")
                if 'passkey' in e.get('Message', '').lower() or 'API_KEY' in e.get('Code', ''):
                    print("  !! Invalid passkey — update BV_PASSKEY in the script")
            return None, None

        if not product:
            product = data.get('Includes', {}).get('Products', {})

        total = data.get('TotalResults') or 0
        batch = data.get('Results') or []
        reviews.extend(batch)

        if not batch or len(reviews) >= total:
            break

        time.sleep(0.2)

    print(f"  {p_id}: {len(reviews)} / {total} reviews")
    time.sleep(0.5)
    return product, reviews


# ── Main scrape loop ──────────────────────────────────────────────────────────
result = {}

for i, pid in enumerate(unique_pids):
    print(f"\n{i+1:04d}/{len(unique_pids)} || {pid}")

    product_data, reviews_data = scrape_reviews(pid)

    if product_data is None:
        print(f"  Skipping {pid} — request failed")
        result[pid] = [None, None]
        continue

    result[pid] = [product_data, reviews_data]

    # Auto-save every 20 products
    if (i + 1) % 20 == 0:
        with open("data/raw/json/scraper_result.json", "w") as f:
            json.dump(result, f)
        print(f"  Progress saved ({i+1} products)")

# ── Final save ────────────────────────────────────────────────────────────────
with open("data/raw/json/scraper_result.json", "w") as f:
    json.dump(result, f)

total_reviews = sum(len(v[1]) for v in result.values() if v[1])
print(f"\nDone! {len(result)} products, {total_reviews} total reviews → data/raw/json/scraper_result.json")