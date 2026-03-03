import re
import json
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=None,
                    help="Limit number of products to scrape (dev mode)")
args = parser.parse_args()


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
    # Normalize URL — strip icid2 and deduplicate by pd_id
    clean_link = re.sub(r'&icid2=.*', '', product_link).replace("%20", " ").strip()

    pd_id_match = re.findall(r'(P[0-9]{4,9})', clean_link, re.IGNORECASE)
    if not pd_id_match:
        return None
    pd_id = pd_id_match[0].upper()

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

    # ── Try linkStore JSON first (most reliable) ──────────────────────────────
    category = size_and_item = price = love_count = reviews_count = None

    script = soup.find("script", id="linkStore")
    jdata = None
    if script and script.string:
        try:
            jdata = json.loads(script.string)
        except json.JSONDecodeError:
            pass

    if jdata:
        # Category from breadcrumbs array in JSON
        try:
            jstr = json.dumps(jdata)
            # breadcrumbs often stored as displayName arrays
            bc = re.findall(r'"displayName"\s*:\s*"([^"]+)"', jstr)
            if bc:
                category = ', '.join(bc[:4])  # first 4 breadcrumb items
        except Exception:
            pass

        price      = find_in_json(jdata, "currentSku", "listPrice", "salePrice") \
                     or find_in_json(jdata, "listPrice", "salePrice", "currentPrice")
        love_count = find_in_json(jdata, "lovesCount", "loves", "loveCount", "love_count")
        reviews_count = find_in_json(jdata, "reviews", "reviewCount", "numReviews")
        size_and_item = find_in_json(jdata, "size", "itemNumber", "netWeight")

    # ── HTML fallbacks (handles any data-comp naming) ─────────────────────────

    if category is None:
        try:
            # Match any element whose data-comp contains "BreadCrumb"
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
            # Match any data-comp containing "Price"
            el = soup.find(attrs={"data-comp": re.compile(r"\bPrice\b", re.I)})
            if el:
                price = el.get_text(strip=True) or None
        except Exception:
            pass
        # Fallback: look for $ in page
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
            # Try linkJSON (older page format)
            lj = soup.find(attrs={"id": "linkJSON"})
            if lj:
                rv = re.findall(r'"reviews"\s*:\s*(\d+)', str(lj))
                if rv:
                    reviews_count = rv[0]
        except Exception:
            pass

    return {
        'pd_id':         pd_id,
        'size_and_item': size_and_item,
        'category':      category,
        'price':         price,
        'love_count':    love_count,
        'reviews_count': reviews_count,
    }


# ── Setup ─────────────────────────────────────────────────────────────────────

with open("scraping/data/json/sephora_cookies.json", "r") as f:
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

with open('scraping/data/txt/product_links.txt', 'r') as f:
    all_links = [line.strip() for line in f if line.strip()]

# Deduplicate by pd_id — keep first occurrence, skip icid2 dupes
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

# 🔥 LIMITADOR SEGURO
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
            print(f'{i+1:04d} / {len(product_links)} || BLOCKED/SKIPPED || {link}')
            continue

        result.append(data)
        pd.DataFrame(result).to_csv('scraping/data/csv/pd_info.csv', index=False)
        print(f'{i+1:04d} / {len(product_links)} || {data["pd_id"]} | price={data["price"]} | reviews={data["reviews_count"]} || {link[:60]}')

    browser.close()

print(f'\nDone! {len(result)} products saved to scraping/data/csv/pd_info.csv')