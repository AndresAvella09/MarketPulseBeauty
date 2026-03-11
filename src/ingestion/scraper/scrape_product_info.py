import re
import json
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=None,
                    help="Limit number of products to scrape (dev mode)")
args = parser.parse_args()

logging.basicConfig(filename='logs/scraping.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ── Keyword map: slug fragment → clean label ──────────────────────────────────
KEYWORD_MAP = {
    "niacinamide":    "Niacinamide",
    "hyaluronic-acid": "Hyaluronic Acid",
    "hyaluronic":     "Hyaluronic Acid",
    "sunscreen":   "Sunscreen",
}

def detect_keyword(url: str) -> str | None:
    url_lower = url.lower()
    for slug, label in KEYWORD_MAP.items():
        if slug in url_lower:
            return label
    return None


def find_in_json(data, *keys):
    """Recursively search nested JSON for first occurrence of any of the given keys."""
    if isinstance(data, dict):
        for k, v in data.items():
            if k in keys and isinstance(v, (str, int, float)) and v not in ("", None):
                return v
            result = find_in_json(v, *keys)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_in_json(item, *keys)
            if result is not None:
                return result
    return None


def get_data(page, product_link):
    clean_link = re.sub(r'&icid2=.*', '', product_link).replace("%20", " ").strip()

    pd_id_match = re.findall(r'(P[0-9]{4,9})', clean_link, re.IGNORECASE)
    if not pd_id_match:
        return None
    pd_id = pd_id_match[0].upper()

    keyword = detect_keyword(clean_link)

    try:
        page.goto(clean_link, wait_until="domcontentloaded", timeout=30000)
        time.sleep(random.uniform(1.5, 3))
        title = page.title()
        if "Access Denied" in title or not title:
            return None
    except PlaywrightTimeoutError:
        return None

    html = page.content()
    soup = BeautifulSoup(html, 'html.parser')

    category = size_and_item = price = love_count = reviews_count = brand = None

    script = soup.find("script", id="linkStore")
    jdata = None
    if script and script.string:
        try:
            jdata = json.loads(script.string)
        except json.JSONDecodeError:
            pass

    if jdata:
        try:
            jstr = json.dumps(jdata)
            bc = re.findall(r'"displayName"\s*:\s*"([^"]+)"', jstr)
            if bc:
                category = ', '.join(bc[:4])
        except Exception:
            pass

        price         = find_in_json(jdata, "currentSku", "listPrice", "salePrice") \
                        or find_in_json(jdata, "listPrice", "salePrice", "currentPrice")
        love_count    = find_in_json(jdata, "lovesCount", "loves", "loveCount", "love_count")
        reviews_count = find_in_json(jdata, "reviews", "reviewCount", "numReviews")
        size_and_item = find_in_json(jdata, "size", "itemNumber", "netWeight")
        brand         = find_in_json(jdata, "brandName", "brand", "BrandName")

    if category is None:
        try:
            bc_el = soup.find(attrs={"data-comp": re.compile(r"BreadCrumb", re.I)})
            if bc_el:
                cats = [a.get_text(strip=True) for a in bc_el.find_all("a") if a.get_text(strip=True)]
                category = ', '.join(cats) if cats else None
        except Exception:
            pass

    if size_and_item is None:
        try:
            el = soup.find(attrs={"data-comp": re.compile(r"SizeAndItem|ItemNumber", re.I)})
            if el:
                size_and_item = el.get_text(strip=True) or None
        except Exception:
            pass

    if price is None:
        try:
            el = soup.find(attrs={"data-comp": re.compile(r"\bPrice\b", re.I)})
            if el:
                price = el.get_text(strip=True) or None
        except Exception:
            pass
        if price is None:
            try:
                price_el = soup.find(string=re.compile(r'\$\d+'))
                if price_el:
                    price = price_el.strip()
            except Exception:
                pass

    if love_count is None:
        try:
            el = soup.find(attrs={"data-comp": "LovesCount "})
            if el:
                love_count = el.get_text(strip=True) or None
        except Exception:
            pass

    if reviews_count is None:
        try:
            lj = soup.find(attrs={"id": "linkJSON"})
            if lj:
                rv = re.findall(r'"reviews"\s*:\s*(\d+)', str(lj))
                if rv:
                    reviews_count = rv[0]
        except Exception:
            pass

    return {
        'pd_id':         pd_id,
        'keyword':       keyword,
        'brand':         brand,
        'size_and_item': size_and_item,
        'category':      category,
        'price':         price,
        'love_count':    love_count,
        'reviews_count': reviews_count,
    }


# ── Setup ─────────────────────────────────────────────────────────────────────

with open("src/ingestion/scraper/sephora_cookies.json", "r") as f:
    raw_cookies = json.load(f)

def normalize_cookies(raw_cookies):
    return [
        {"name":   c.get("name",   c.get("Name",   "")),
         "value":  c.get("value",  c.get("Value",  "")),
         "domain": c.get("domain", c.get("Domain", ".sephora.com")),
         "path":   c.get("path",   c.get("Path",   "/"))}
        for c in raw_cookies
        if c.get("name", c.get("Name")) and c.get("value", c.get("Value"))
    ]

with open('data/raw/links/product_links.txt', 'r') as f:
    all_links = [line.strip() for line in f if line.strip()]

seen_ids = set()
product_links = []

for link in all_links:
    m = re.findall(r'(P[0-9]{4,9})', link, re.IGNORECASE)
    if m:
        pid = m[0].upper()
        if pid not in seen_ids:
            seen_ids.add(pid)
            product_links.append(link)

print(f"Total links: {len(all_links)} → After dedup: {len(product_links)}")

if args.limit:
    print(f"Development mode: limiting to first {args.limit} products\n")
    product_links = product_links[:args.limit]

result = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        locale="en-US",
    )
    context.add_cookies(normalize_cookies(raw_cookies))
    page = context.new_page()
    Stealth().apply_stealth_sync(page)

    for i, link in enumerate(product_links):
        data = get_data(page, link)

        if data is None:
            logging.error(f'BLOCKED/SKIPPED || {link}')
            continue

        result.append(data)
        pd.DataFrame(result).to_csv('data/raw/csv/pd_info.csv', index=False)
        print(f'{i+1:04d} / {len(product_links)} || {data["pd_id"]} | keyword={data["keyword"]} | price={data["price"]} | reviews={data["reviews_count"]} || {link[:60]}')

    browser.close()


# ── Merge Bazaarvoice product stats from scraper_result.json ──────────────────
try:
    with open('data/raw/json/scraper_result.json') as f:
        bv_result = json.load(f)

    df = pd.DataFrame(result)

    for pid, value in bv_result.items():
        if not value or value[0] is None:
            continue

        product_result = value[0]
        if not product_result:
            continue

        # Navigate nested BV structure
        try:
            product_result = product_result[pid]
        except KeyError:
            try:
                product_result = product_result[pid.lower()]
            except KeyError:
                product_result = next(iter(product_result.values()), None)

        if not product_result:
            continue

        pid_upper = pid.upper()
        mask = df['pd_id'] == pid_upper

        for col in ['Name', 'Description']:
            if col in product_result:
                df.loc[mask, col] = product_result[col]

        # BV brand fallback — fills in any rows where Playwright didn't catch it
        bv_brand = product_result.get('Brand', {})
        if isinstance(bv_brand, dict) and bv_brand.get('Name'):
            df.loc[mask & df['brand'].isna(), 'brand'] = bv_brand['Name']

        stats = product_result.get('ReviewStatistics', {})
        for col in ['AverageOverallRating', 'FirstSubmissionTime', 'LastSubmissionTime']:
            if col in stats:
                df.loc[mask, col] = stats[col]

        try:
            age_data = stats['ContextDataDistribution']['age']['Values']
            for age_group in age_data:
                df.loc[mask, f'Age_{age_group["Value"]}'] = age_group['Count']
        except KeyError:
            pass

    df.to_csv('data/raw/csv/pd_info.csv', index=False)
    logging.info(f"Merged BV stats → {len(df)} products saved to data/raw/csv/pd_info.csv")

except FileNotFoundError:
    pd.DataFrame(result).to_csv('data/raw/csv/pd_info.csv', index=False)
    logging.info(f"No scraper_result.json found — saved {len(result)} products (Playwright data only) to data/raw/csv/pd_info.csv")

