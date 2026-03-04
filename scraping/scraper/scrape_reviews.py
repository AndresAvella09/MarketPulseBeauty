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
load_dotenv()  # Load environment variables from .env file

# ── Load product IDs ──────────────────────────────────────────────────────────
with open('scraping/data/txt/product_links.txt', 'r') as f:
    all_links = [line.strip() for line in f if line.strip()]

unique_pids = list(dict.fromkeys(
    re.findall(r'P[0-9]{4,9}', link)[0]
    for link in all_links
    if re.findall(r'P[0-9]{4,9}', link)
))
print(f"Total unique products to scrape: {len(unique_pids)}")
# 🔥 LIMITADOR SEGURO
if args.limit:
    print(f"Development mode: limiting to first {args.limit} products\n")
    unique_pids = unique_pids[:args.limit]


# ── Bazaarvoice API ───────────────────────────────────────────────────────────
BV_URL = 'https://api.bazaarvoice.com/scraping/data/reviews.json'
# Updated passkey — if this stops working, open DevTools on any Sephora product
# page, filter Network requests by "bazaarvoice", and grab the passkey param
BV_PASSKEY = os.getenv('BV_PASSKEY')

def scrape_reviews(p_id, proxy=None):
    """Fetch all reviews for a product from Bazaarvoice. Returns (product_dict, reviews_list)."""
    proxies = {"http": proxy, "https": proxy} if proxy else None
    params = {
        'Filter':     f'ProductId:{p_id.lower()}',  # BV uses lowercase IDs
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
            r = requests.get(BV_URL, params=params, proxies=proxies, timeout=15)
            data = r.json()
        except Exception as e:
            print(f"  Error fetching {p_id}: {e}")
            return None, None

        if r.status_code != 200:
            print(f"  HTTP {r.status_code} for {p_id}")
            return None, None

        if data.get('HasErrors'):
            errors = data.get('Errors', [])
            for e in errors:
                print(f"  BV API Error: {e.get('Message')} ({e.get('Code')})")
                if 'passkey' in e.get('Message', '').lower() or 'API_KEY' in e.get('Code', ''):
                    print("  !! Invalid passkey — update BV_PASSKEY in the script (see instructions above)")
            return None, None

        # Grab product info on first page
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


# ── Proxy rotation ────────────────────────────────────────────────────────────
proxies = [
    '140.227.174.216:1000', '140.227.175.225:1000',
    '140.227.224.177:1000', '140.227.237.154:1000',
    '140.227.173.230:1000', '140.227.238.18:1000',
    '165.22.211.212:3128',  '140.227.225.38:1000',
    '52.191.103.11:3128',
]
px_idx = 0

def next_proxy():
    global px_idx
    px_idx = (px_idx + 1) % len(proxies)
    return proxies[px_idx]


# ── Main scrape loop ──────────────────────────────────────────────────────────
result = {}

for i, pid in enumerate(unique_pids):
    print(f"\n{i+1:04d}/{len(unique_pids)} || {pid}")

    # Try without proxy first, fall back to proxies on failure
    product_data, reviews_data = scrape_reviews(pid)

    if product_data is None:
        # Rotate through proxies until one works
        for _ in range(len(proxies)):
            proxy = next_proxy()
            print(f"  Retrying with proxy {proxy}...")
            product_data, reviews_data = scrape_reviews(pid, proxy=proxy)
            if product_data is not None:
                break

    if product_data is None:
        print(f"  Skipping {pid} — all proxies failed")
        result[pid] = [None, None]
        continue

    result[pid] = [product_data, reviews_data]

    # Auto-save every 20 products
    if (i + 1) % 20 == 0:
        with open("scraping/data/json/scraper_result.json", "w") as f:
            json.dump(result, f)
        print(f"  Progress saved ({i+1} products)")

# ── Final save ────────────────────────────────────────────────────────────────
with open("scraping/data/json/scraper_result.json", "w") as f:
    json.dump(result, f)

total_reviews = sum(len(v[1]) for v in result.values() if v[1])
print(f"\nDone! {len(result)} products, {total_reviews} total reviews → scraping/data/json/scraper_result.json")