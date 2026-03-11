from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
import time
import random
import json
import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=None,
                    help="Limit number of brands to scrape (for development)")
args = parser.parse_args()

logging.basicConfig(filename='logs/scraping.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ── Keyword filter ────────────────────────────────────────────────────────────
KEYWORDS = [
    "niacinamide",
    "hyaluronic-acid",
    "hyaluronic",
    "sunscreen",
]

def matches_keywords(url):
    url_lower = url.lower()
    return any(kw in url_lower for kw in KEYWORDS)


def extract_all_product_links(data, seen_set, result_list):
    """Recursively searches a JSON object for Sephora product URLs."""
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str) and "/product/" in value:
                if value.startswith("/product/"):
                    full_url = "https://www.sephora.com" + value.replace(" ", "%20")
                elif value.startswith("https://www.sephora.com/product/"):
                    full_url = value.replace(" ", "%20")
                else:
                    continue
                if full_url not in seen_set:
                    seen_set.add(full_url)
                    result_list.append(full_url)
            elif isinstance(value, (dict, list)):
                extract_all_product_links(value, seen_set, result_list)
    elif isinstance(data, list):
        for item in data:
            extract_all_product_links(item, seen_set, result_list)


def scrape_product(page, link):
    brand_slug = link.rstrip("/").split("/brand/")[-1].split("/")[0]
    full_link = f"https://www.sephora.com/brand/{brand_slug}/all?pageSize=300"
    brand_name = brand_slug

    try:
        page.goto(full_link, wait_until="domcontentloaded", timeout=30000)
        time.sleep(random.uniform(2, 4))
        if "Access Denied" in page.title():
            logging.error(f"Access Denied for {brand_name}")
            return None

        for _ in range(10):
            page.evaluate("window.scrollBy(0, document.body.scrollHeight / 10)")
            time.sleep(0.4)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

    except PlaywrightTimeoutError:
        logging.error(f"Timeout on {brand_name}")
        return None

    soup = BeautifulSoup(page.content(), "html.parser")
    seen = set()
    product_link_lst = []

    script_tag = soup.find("script", id="linkStore")
    if script_tag and script_tag.string:
        try:
            data = json.loads(script_tag.string)
            extract_all_product_links(data, seen, product_link_lst)
        except json.JSONDecodeError:
            logging.warning(f"JSON decode error for {brand_name}")

    for a in soup.find_all("a", href=True):
        href = a["href"].replace(" ", "%20")
        if "/product/" in href:
            full = "https://www.sephora.com" + href if href.startswith("/") else href
            if full not in seen:
                seen.add(full)
                product_link_lst.append(full)

    return product_link_lst


with open("src/ingestion/scraper/sephora_cookies.json", "r") as f:
    cookies = json.load(f)


def normalize_cookies(raw_cookies):
    normalized = []
    for c in raw_cookies:
        cookie = {
            "name":   c.get("name",   c.get("Name",   "")),
            "value":  c.get("value",  c.get("Value",  "")),
            "domain": c.get("domain", c.get("Domain", ".sephora.com")),
            "path":   c.get("path",   c.get("Path",   "/")),
        }
        if cookie["name"] and cookie["value"]:
            normalized.append(cookie)
    return normalized


with open("data/raw/links/brand_link.txt", "r") as f:
    brand_links = [line.strip() for line in f if line.strip()]

# 🔥 LIMITADOR SEGURO (NO ROMPE NADA)
if args.limit:
    print(f"\nDevelopment mode: limiting to first {args.limit} brands\n")
    brand_links = brand_links[:args.limit]

num_lines = len(brand_links)
all_product_links = []
ct = 1


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        locale="en-US",
    )
    context.add_cookies(normalize_cookies(cookies))
    page = context.new_page()
    stealth = Stealth()
    stealth.apply_stealth_sync(page)

    for brand_link in brand_links:
        brand_name = brand_link.split("/")[4]
        product_link_list = scrape_product(page, brand_link)

        retries = 0
        while product_link_list is None and retries < 2:
            retries += 1
            print(f"\r Retry {retries} for {brand_name}...", end="")
            time.sleep(random.uniform(5, 10))
            product_link_list = scrape_product(page, brand_link)

        if product_link_list is None:
            logging.warning(f"Skipping {brand_name} after retries.")
            product_link_list = []

        all_product_links.extend(product_link_list)
        print(f"\r === {ct} / {num_lines} === {brand_name} === {len(product_link_list)} products", end="")
        ct += 1

        if ct % 20 == 0:
            filtered = [l for l in all_product_links if matches_keywords(l)]  # ← add this
            with open("data/raw/links/product_links.txt", "w") as f:
                f.write("\n".join(filtered))  # ← and this

    browser.close()

all_product_links = [link for link in all_product_links if matches_keywords(link)]
logging.info(f"After keyword filter: {len(all_product_links)} products")
# ─────────────────────────────────────────────────────────────────────────────

with open("data/raw/links/product_links.txt", "w") as f:
    f.write("\n".join(all_product_links))

logging.info(f'Done! {len(all_product_links)} products saved to data/raw/links/product_links.txt')