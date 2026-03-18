"""
sephora_scraper.py
──────────────────
Reads products-sitemap.xml  →  calls Bazaarvoice API  →  returns raw data.

Field definitions live in schema.py — this file never hard-codes column names.

Public surface
──────────────
  SephoraScraper().run()  →  (products: dict[pid, dict], reviews: list[dict])

The scraper itself has no I/O side-effects beyond printing progress.
"""

import re
import time
import os
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config as cfg
from schema import PRODUCT_FIELDS, REVIEW_FIELDS


# ── Helpers ────────────────────────────────────────────────────────────────────

def _context_val(rev, field):
    """Pull the first selected value from ContextDataValues (skinTone, etc.)."""
    entry = (rev.get("ContextDataValues") or {}).get(field)
    if not entry:
        return None
    return entry.get("ValueLabel") or entry.get("Value")


def _tag_val(rev, field):
    """Pipe-joined labels from TagDimensions (multi-select: hairConcerns, etc.)."""
    entry = (rev.get("TagDimensions") or {}).get(field)
    if not entry:
        return None
    values = entry.get("Values") or []
    return " | ".join(v.get("ValueLabel", v.get("Value", "")) for v in values) or None


def _rating_dist(stats):
    """RatingDistribution list  →  flat dict RatingDist_1 … RatingDist_5."""
    out = {f"RatingDist_{i}": 0 for i in range(1, 6)}
    for item in (stats.get("RatingDistribution") or []):
        rv = item.get("RatingValue")
        if rv:
            out[f"RatingDist_{rv}"] = item.get("Count", 0)
    return out


# ── Scraper ────────────────────────────────────────────────────────────────────

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

    # ── Sitemap ────────────────────────────────────────────────────────────────

    def get_ids_from_local_sitemap(self) -> list[str]:
        path = cfg.SITEMAP_LOCAL_PATH
        if not os.path.exists(path):
            print(f"[!] Sitemap not found at '{path}'.")
            return []

        print(f"[*] Parsing {path} …")
        ids: set[str] = set()
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

    def _fetch_product_reviews(self, product_id: str) -> list[dict]:
        """
        Fetch all pages of reviews for one product.
        Returns a flat list of raw row dicts (product meta + review fields merged).
        """
        rows: list[dict] = []
        offset = 0
        total = None
        product_meta: dict | None = None

        while offset < cfg.MAX_REVIEWS_PER_PRODUCT:
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
                resp = self.session.get(cfg.BAZAARVOICE_URL, params=params, timeout=20)

                # --- MODIFICATION: Rate Limit Handling ---
                if resp.status_code == 429:
                    print(f"[!] 429 Too Many Requests for {product_id}. Sleeping 60s...")
                    time.sleep(60)
                    continue  # Retry the current offset after waiting
                # ----------------------------------------

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
                        break

                # Product metadata — parsed once from the first page
                if product_meta is None:
                    pmeta = (
                        data.get("Includes", {})
                            .get("Products", {})
                            .get(product_id, {})
                    )
                    stats = pmeta.get("ReviewStatistics") or {}
                    product_meta = {
                        "ProductID":        product_id,
                        "Brand":            (pmeta.get("Brand") or {}).get("Name"),
                        "ProductName":      pmeta.get("Name"),
                        "CategoryId":       pmeta.get("CategoryId"),
                        "ProductPageUrl":   pmeta.get("ProductPageUrl"),
                        "AvgRating":        stats.get("AverageOverallRating"),
                        "TotalReviewCount": stats.get("TotalReviewCount"),
                        "RecommendedCount": stats.get("RecommendedCount"),
                        "TotalPhotoCount":  stats.get("TotalPhotoCount"),
                        **_rating_dist(stats),
                    }

                reviews = data.get("Results") or []
                if not reviews:
                    break

                for rev in reviews:
                    cdv = rev.get("ContextDataValues") or {}
                    rows.append({
                        **product_meta,
                        "ReviewID":         rev.get("Id"),
                        "Rating":           rev.get("Rating"),
                        "Title":            rev.get("Title"),
                        "ReviewText":       rev.get("ReviewText"),
                        "SubmissionTime":   rev.get("SubmissionTime"),
                        "LastModTime":      rev.get("LastModificationTime"),
                        "IsRecommended":    rev.get("IsRecommended"),
                        "HelpfulCount":     rev.get("TotalHelpfulVoteCount"),
                        "NotHelpfulCount":  rev.get("TotalNegativeFeedbackCount"),
                        "IsFeatured":       rev.get("IsFeatured"),
                        "IsIncentivized":   (cdv.get("IncentivizedReview") or {}).get("Value"),
                        "IsStaffReview":    (cdv.get("StaffContext") or {}).get("Value"),
                        "UserLocation":     rev.get("UserLocation"),
                        "skinTone":         _context_val(rev, "skinTone"),
                        "skinType":         _context_val(rev, "skinType"),
                        "eyeColor":         _context_val(rev, "eyeColor"),
                        "hairColor":        _context_val(rev, "hairColor"),
                        "hairType":         _context_val(rev, "hairType"),
                        "hairConcerns":     _tag_val(rev, "hairConcerns"),
                        "skinConcerns":     _tag_val(rev, "skinConcerns"),
                        "ageRange":         _context_val(rev, "ageRange"),
                        "ReviewPhotoCount": len(rev.get("Photos") or []),
                    })

                offset += cfg.PAGE_SIZE
                if offset >= total:
                    break

                time.sleep(cfg.DELAY_SECS)

            except Exception as exc:
                print(f"[!] {product_id} offset={offset}: {exc}")
                break

        return rows

    # ── Orchestration ──────────────────────────────────────────────────────────

    def run(self) -> tuple[dict[str, dict], list[dict]]:
        """
        Scrape all products from the sitemap.
        """
        product_ids = self.get_ids_from_local_sitemap()
        if not product_ids:
            return {}, []

        print(
            f"[*] Fetching reviews for {len(product_ids):,} products "
            f"using {cfg.MAX_WORKERS} threads …"
        )

        all_products: dict[str, dict] = {}
        all_reviews:  list[dict]      = []
        done = 0

        with ThreadPoolExecutor(max_workers=cfg.MAX_WORKERS) as pool:
            futures = {pool.submit(self._fetch_product_reviews, pid): pid
                       for pid in product_ids}

            for future in as_completed(futures):
                pid   = futures[future]
                done += 1
                try:
                    rows = future.result()
                    if rows:
                        # Split using field lists from schema.py
                        all_products[pid] = {k: rows[0][k] for k in PRODUCT_FIELDS}
                        all_reviews.extend(
                            {k: row[k] for k in REVIEW_FIELDS} for row in rows
                        )
                    print(f"  [{done}/{len(product_ids)}] {pid}: {len(rows)} reviews")
                except Exception as exc:
                    print(f"  [{done}/{len(product_ids)}] {pid}: ERROR – {exc}")

        return all_products, all_reviews