"""
sephora_scraper_sample.py
─────────────────────────
- Only processes the first SAMPLE_SIZE products
- Only fetches the first page of reviews per product
- Splitting into Products and Reviews CSVs
"""

import requests
import re
import csv
import time
import os
import xml.etree.ElementTree as ET

import config as cfg

# ── tweak these to control the sample ─────────────────────────────────────────
SAMPLE_SIZE      = 5    # number of products to test
REVIEWS_PER_PROD = 10   # reviews to pull per product (max 100)
# ──────────────────────────────────────────────────────────────────────────────

def _context_val(rev, field):

    cdv = rev.get("ContextDataValues") or {}

    entry = cdv.get(field)

    if not entry:

        return None

    return entry.get("ValueLabel") or entry.get("Value")





def _tag_val(rev, field):

    td = rev.get("TagDimensions") or {}

    entry = td.get(field)

    if not entry:

        return None

    values = entry.get("Values") or []

    return " | ".join(v.get("ValueLabel", v.get("Value", "")) for v in values) or None





def _rating_dist(stats):

    out = {f"RatingDist_{i}": 0 for i in range(1, 6)}

    for item in (stats.get("RatingDistribution") or []):

        rv = item.get("RatingValue")

        if rv:

            out[f"RatingDist_{rv}"] = item.get("Count", 0)

    return out





def get_sample_ids():

    path = cfg.SITEMAP_LOCAL_PATH

    if not os.path.exists(path):

        print(f"[!] Sitemap not found at '{path}'.")

        return []



    print(f"[*] Reading sitemap — grabbing first {SAMPLE_SIZE} products …")

    ids = []

    for _, elem in ET.iterparse(path, events=("end",)):

        if elem.tag.endswith("loc"):

            url = elem.text or ""

            m = re.search(r"-(P\d{5,7})$", url.strip())

            if m:

                ids.append(m.group(1))

                if len(ids) >= SAMPLE_SIZE:

                    break

        elem.clear()



    print(f"[+] Sample product IDs: {ids}")

    return ids

def fetch_sample_reviews(product_id):
    # ... (Keeping session and params logic)
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

    params = {
        "Filter": [f"contentlocale:en*", f"ProductId:{product_id}"],
        "Sort": "SubmissionTime:desc",
        "Include": "Products,Comments",
        "Stats": "Reviews",
        "Limit": REVIEWS_PER_PROD,
        "Offset": 0,
        "passkey": cfg.BAZAARVOICE_PASSKEY,
        "apiversion": "5.4",
        "Locale": "en_US",
    }

    try:
        resp = session.get(cfg.BAZAARVOICE_URL, params=params, timeout=20)
        if resp.status_code != 200: return []
        data = resp.json()
        if data.get("HasErrors"): return []

        pmeta = data.get("Includes", {}).get("Products", {}).get(product_id, {})
        stats = pmeta.get("ReviewStatistics") or {}
        rdist = _rating_dist(stats)

        # Meta gathered for later separation
        product_meta = {
            "Brand": (pmeta.get("Brand") or {}).get("Name"),
            "ProductName": pmeta.get("Name"),
            "CategoryId": pmeta.get("CategoryId"),
            "ProductPageUrl": pmeta.get("ProductPageUrl"),
            "AvgRating": stats.get("AverageOverallRating"),
            "TotalReviewCount": stats.get("TotalReviewCount"),
            "RecommendedCount": stats.get("RecommendedCount"),
            "TotalPhotoCount": stats.get("TotalPhotoCount"),
            **rdist,
        }

        rows = []
        for rev in (data.get("Results") or []):
            cdv = rev.get("ContextDataValues") or {}
            rows.append({
                "ProductID": product_id,
                **product_meta, # Temporarily combined for print_preview
                "ReviewID": rev.get("Id"),
                "Rating": rev.get("Rating"),
                "Title": rev.get("Title"),
                "ReviewText": rev.get("ReviewText"),
                "SubmissionTime": rev.get("SubmissionTime"),
                "LastModTime": rev.get("LastModificationTime"),
                "IsRecommended": rev.get("IsRecommended"),
                "HelpfulCount": rev.get("TotalHelpfulVoteCount"),
                "NotHelpfulCount": rev.get("TotalNegativeFeedbackCount"),
                "IsFeatured": rev.get("IsFeatured"),
                "IsIncentivized": (cdv.get("IncentivizedReview") or {}).get("Value"),
                "IsStaffReview": (cdv.get("StaffContext") or {}).get("Value"),
                "UserLocation": rev.get("UserLocation"),
                "skinTone": _context_val(rev, "skinTone"),
                "skinType": _context_val(rev, "skinType"),
                "eyeColor": _context_val(rev, "eyeColor"),
                "hairColor": _context_val(rev, "hairColor"),
                "hairType": _context_val(rev, "hairType"),
                "hairConcerns": _tag_val(rev, "hairConcerns"),
                "skinConcerns": _tag_val(rev, "skinConcerns"),
                "ageRange": _context_val(rev, "ageRange"),
                "ReviewPhotoCount": len(rev.get("Photos") or []),
            })
        return rows
    except Exception as e:
        print(f"  [!] {product_id}: {e}")
        return []

# ... (Keeping print_preview as it is)

def main():
    ids = get_sample_ids()
    if not ids: return

    all_raw_data = []
    for i, pid in enumerate(ids, 1):
        print(f"  [{i}/{len(ids)}] Fetching {pid} …")
        rows = fetch_sample_reviews(pid)
        print(f"         → {len(rows)} reviews")
        all_raw_data.extend(rows)
        time.sleep(0.3)

    if not all_raw_data:
        return

    # ── Separation Logic ──────────────────────────────────────────────────────
    all_products = {}
    all_reviews = []

    prod_fields = [
        "ProductID", "Brand", "ProductName", "CategoryId", "ProductPageUrl", 
        "AvgRating", "TotalReviewCount", "RecommendedCount", "TotalPhotoCount", 
        "RatingDist_1", "RatingDist_2", "RatingDist_3", "RatingDist_4", "RatingDist_5"
    ]
    
    rev_fields = [
        "ProductID", "ReviewID", "Rating", "Title", "ReviewText", "SubmissionTime", 
        "LastModTime", "IsRecommended", "HelpfulCount", "NotHelpfulCount", 
        "IsFeatured", "IsIncentivized", "IsStaffReview", "UserLocation", 
        "skinTone", "skinType", "eyeColor", "hairColor", "hairType", 
        "hairConcerns", "skinConcerns", "ageRange", "ReviewPhotoCount"
    ]

    for row in all_raw_data:
        pid = row["ProductID"]
        if pid not in all_products:
            all_products[pid] = {k: row[k] for k in prod_fields}
        
        all_reviews.append({k: row[k] for k in rev_fields})

    # ── Write Products CSV ────────────────────────────────────────────────────
    prod_out = cfg.OUTPUT_PRODUCTS
    with open(prod_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=prod_fields)
        writer.writeheader()
        writer.writerows(all_products.values())
    print(f"[+] Products CSV saved → '{prod_out}' ({len(all_products)} rows)")

    # ── Write Reviews CSV ─────────────────────────────────────────────────────
    rev_out = cfg.OUTPUT_REVIEWS
    with open(rev_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rev_fields)
        writer.writeheader()
        writer.writerows(all_reviews)
    print(f"[+] Reviews CSV saved → '{rev_out}' ({len(all_reviews)} rows)\n")


if __name__ == "__main__":
    main()