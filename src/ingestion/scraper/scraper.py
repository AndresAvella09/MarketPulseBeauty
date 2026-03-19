"""
sephora_scraper.py
──────────────────
Reads products-sitemap.xml  →  calls Bazaarvoice API  →  writes CSV.

Extracted per review
────────────────────
Core        : ProductID, Brand, ProductName, CategoryId,
              Rating, Title, ReviewText, SubmissionTime,
              IsRecommended, HelpfulCount, NotHelpfulCount,
              IsFeatured, IsIncentivized, IsStaffReview
Reviewer    : UserNickname, UserLocation,
              skinTone, skinType, eyeColor,
              hairColor, hairType, hairConcerns, ageRange
Product     : AvgRating, TotalReviewCount, RecommendedCount,
              TotalPhotoCount, RatingDist_1..5
"""

import requests
import re
import csv
import time
import os
import json
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import config as cfg

# ── helpers ────────────────────────────────────────────────────────────────────

def _context_val(rev, field):
    """Pull the first selected value from ContextDataValues (hair colour, etc.)."""
    cdv = rev.get("ContextDataValues") or {}
    entry = cdv.get(field)
    if not entry:
        return None
    # Value is a dict with ValueLabel
    return entry.get("ValueLabel") or entry.get("Value")


def _tag_val(rev, field):
    """Pull pipe-joined labels from TagDimensions (multi-select fields like hairConcerns)."""
    td = rev.get("TagDimensions") or {}
    entry = td.get(field)
    if not entry:
        return None
    values = entry.get("Values") or []
    return " | ".join(v.get("ValueLabel", v.get("Value", "")) for v in values) or None


def _rating_dist(stats):
    """Return dict RatingDist_1..5 from RatingDistribution list."""
    out = {f"RatingDist_{i}": 0 for i in range(1, 6)}
    for item in (stats.get("RatingDistribution") or []):
        rv = item.get("RatingValue")
        ct = item.get("Count", 0)
        if rv:
            out[f"RatingDist_{rv}"] = ct
    return out


# ── scraper ────────────────────────────────────────────────────────────────────

class SephoraScraper:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        })

    # ── sitemap ────────────────────────────────────────────────────────────────

    def get_ids_from_local_sitemap(self):
        path = cfg.SITEMAP_LOCAL_PATH
        if not os.path.exists(path):
            print(f"[!] Sitemap not found at '{path}'. "
                  "Put products-sitemap.xml in the same folder as this script.")
            return []

        print(f"[*] Parsing {path} …")
        ids = set()
        kws = [k.lower() for k in cfg.TARGET_KEYWORDS]

        for _, elem in ET.iterparse(path, events=("end",)):
            if elem.tag.endswith("loc"):
                url = elem.text or ""
                if not kws or any(k in url.lower() for k in kws):
                    m = re.search(r"-(P\d{5,7})$", url.strip())
                    if m:
                        ids.add(m.group(1))
            elem.clear()

        print(f"[+] Found {len(ids):,} products in sitemap.")
        return list(ids)

    # ── API ────────────────────────────────────────────────────────────────────

    def fetch_all_reviews(self, product_id):
        """Return list-of-dicts, one row per review."""
        rows = []
        offset = 0
        total = None
        product_meta_cache = None   # fetched once from first page

        while True:
            if offset >= cfg.MAX_REVIEWS_PER_PRODUCT:
                break

            params = {
                "Filter":     [f"contentlocale:en*", f"ProductId:{product_id}"],
                "Sort":       "SubmissionTime:desc",
                "Include":    "Products,Comments",
                "Stats":      "Reviews",
                "Limit":      cfg.PAGE_SIZE,
                "Offset":     offset,
                "passkey":    cfg.BAZAARVOICE_PASSKEY,
                "apiversion": "5.4",
                "Locale":     "en_US",
            }

            try:
                resp = self.session.get(
                    cfg.BAZAARVOICE_URL, params=params, timeout=20
                )
                if resp.status_code != 200:
                    print(f"[!] {product_id}: HTTP {resp.status_code} at offset {offset}")
                    break

                data = resp.json()

                if data.get("HasErrors"):
                    print(f"[!] {product_id}: API errors → {data.get('Errors')}")
                    break

                if total is None:
                    total = data.get("TotalResults", 0)
                    if total == 0:
                        break   # no reviews at all

                # ── product-level metadata (only needed once) ──────────────
                if product_meta_cache is None:
                    pmeta = (
                        data.get("Includes", {})
                            .get("Products", {})
                            .get(product_id, {})
                    )
                    stats = pmeta.get("ReviewStatistics") or {}
                    rdist = _rating_dist(stats)
                    product_meta_cache = {
                        "Brand":             (pmeta.get("Brand") or {}).get("Name"),
                        "ProductName":       pmeta.get("Name"),
                        "CategoryId":        pmeta.get("CategoryId"),
                        "ProductPageUrl":    pmeta.get("ProductPageUrl"),
                        "AvgRating":         stats.get("AverageOverallRating"),
                        "TotalReviewCount":  stats.get("TotalReviewCount"),
                        "RecommendedCount":  stats.get("RecommendedCount"),
                        "TotalPhotoCount":   stats.get("TotalPhotoCount"),
                        **rdist,
                    }

                reviews = data.get("Results") or []
                if not reviews:
                    break

                for rev in reviews:
                    # ── reviewer context data ──────────────────────────────
                    cdv     = rev.get("ContextDataValues") or {}
                    is_inc  = (cdv.get("IncentivizedReview") or {}).get("Value")
                    is_staff= (cdv.get("StaffContext") or {}).get("Value")

                    row = {
                        # product-level
                        "ProductID":         product_id,
                        **product_meta_cache,
                        # review core
                        "ReviewID":          rev.get("Id"),
                        "Rating":            rev.get("Rating"),
                        "Title":             rev.get("Title"),
                        "ReviewText":        rev.get("ReviewText"),
                        "SubmissionTime":    rev.get("SubmissionTime"),
                        "LastModTime":       rev.get("LastModificationTime"),
                        "IsRecommended":     rev.get("IsRecommended"),
                        "HelpfulCount":      rev.get("TotalHelpfulVoteCount"),
                        "NotHelpfulCount":   rev.get("TotalNegativeFeedbackCount"),
                        "IsFeatured":        rev.get("IsFeatured"),
                        "IsIncentivized":    is_inc,
                        "IsStaffReview":     is_staff,
                        # reviewer demographics
                        "UserLocation":      rev.get("UserLocation"),
                        "skinTone":          _context_val(rev, "skinTone"),
                        "skinType":          _context_val(rev, "skinType"),
                        "eyeColor":          _context_val(rev, "eyeColor"),
                        "hairColor":         _context_val(rev, "hairColor"),
                        "hairType":          _context_val(rev, "hairType"),
                        "hairConcerns":      _tag_val(rev, "hairConcerns"),
                        "skinConcerns":      _tag_val(rev, "skinConcerns"),
                        "ageRange":          _context_val(rev, "ageRange"),
                        # photo count on this specific review
                        "ReviewPhotoCount":  len(rev.get("Photos") or []),
                    }
                    rows.append(row)

                offset += cfg.PAGE_SIZE
                if offset >= total:
                    break

                time.sleep(cfg.DELAY_SECS)

            except Exception as e:
                print(f"[!] {product_id} offset={offset}: {e}")
                break

        return rows

    # ── orchestration ──────────────────────────────────────────────────────────

    def run(self):
        product_ids = self.get_ids_from_local_sitemap()
        if not product_ids:
            return

        print(f"[*] Fetching reviews for {len(product_ids):,} products "
              f"using {cfg.MAX_WORKERS} threads …")

        all_reviews = []
        all_products = {} # Use a dict to store unique product info by ID
        done = 0

        with ThreadPoolExecutor(max_workers=cfg.MAX_WORKERS) as ex:
            future_map = {ex.submit(self.fetch_all_reviews, pid): pid
                          for pid in product_ids}

            for future in as_completed(future_map):
                pid = future_map[future]
                done += 1
                try:
                    rows = future.result()
                    if rows:
                        # 1. Extract Product Meta from the first review row found
                        # This avoids redundancy in the reviews CSV
                        p_info = {k: rows[0][k] for k in [
                            "ProductID", "Brand", "ProductName", "CategoryId", 
                            "ProductPageUrl", "AvgRating", "TotalReviewCount", 
                            "RecommendedCount", "TotalPhotoCount", "RatingDist_1", 
                            "RatingDist_2", "RatingDist_3", "RatingDist_4", "RatingDist_5"
                        ]}
                        all_products[pid] = p_info

                        # 2. Extract Review-specific data
                        # We remove the bulky product info from every review row
                        review_fields = [
                            "ProductID", "ReviewID", "Rating", "Title", "ReviewText", 
                            "SubmissionTime", "LastModTime", "IsRecommended", 
                            "HelpfulCount", "NotHelpfulCount", "IsFeatured", 
                            "IsIncentivized", "IsStaffReview", "UserLocation", 
                            "skinTone", "skinType", "eyeColor", "hairColor", 
                            "hairType", "hairConcerns", "skinConcerns", "ageRange", 
                            "ReviewPhotoCount"
                        ]
                        
                        for r in rows:
                            clean_review = {k: r[k] for k in review_fields}
                            all_reviews.append(clean_review)

                    print(f"  [{done}/{len(product_ids)}] {pid}: {len(rows)} reviews")
                except Exception as e:
                    print(f"  [{done}/{len(product_ids)}] {pid}: ERROR – {e}")

        # ── write CSVs ──────────────────────────────────────────────────────────
        
        # 1. Write Products CSV
        if all_products:
            prod_out = cfg.OUTPUT_PRODUCTS
            p_list = list(all_products.values())
            with open(prod_out, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=p_list[0].keys())
                writer.writeheader()
                writer.writerows(p_list)
            print(f"[+] {len(p_list)} products written to '{prod_out}'.")

        # 2. Write Reviews CSV
        if all_reviews:
            rev_out = cfg.OUTPUT_REVIEWS
            with open(rev_out, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_reviews[0].keys())
                writer.writeheader()
                writer.writerows(all_reviews)
            print(f"[+] {len(all_reviews):,} reviews written to '{rev_out}'.")

# ── entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    SephoraScraper().run()