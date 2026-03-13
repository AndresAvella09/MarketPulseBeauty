"""
sephora_scraper_sample.py
─────────────────────────
Same logic as the full scraper but:
  - Only processes the first SAMPLE_SIZE products from the sitemap
  - Only fetches the first page of reviews per product (max 100 reviews each)
  - Prints a rich preview in the terminal before saving
  - Saves to sephora_reviews_SAMPLE.csv

Run:
    python sephora_scraper_sample.py
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
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    params = {
        "Filter":     [f"contentlocale:en*", f"ProductId:{product_id}"],
        "Sort":       "SubmissionTime:desc",
        "Include":    "Products,Comments",
        "Stats":      "Reviews",
        "Limit":      REVIEWS_PER_PROD,
        "Offset":     0,
        "passkey":    cfg.BAZAARVOICE_PASSKEY,
        "apiversion": "5.4",
        "Locale":     "en_US",
    }

    try:
        resp = session.get(cfg.BAZAARVOICE_URL, params=params, timeout=20)
        if resp.status_code != 200:
            print(f"  [!] {product_id}: HTTP {resp.status_code}")
            return []

        data = resp.json()
        if data.get("HasErrors"):
            print(f"  [!] {product_id}: API error → {data.get('Errors')}")
            return []

        pmeta = data.get("Includes", {}).get("Products", {}).get(product_id, {})
        stats = pmeta.get("ReviewStatistics") or {}
        rdist = _rating_dist(stats)

        product_meta = {
            "Brand":            (pmeta.get("Brand") or {}).get("Name"),
            "ProductName":      pmeta.get("Name"),
            "CategoryId":       pmeta.get("CategoryId"),
            "ProductPageUrl":   pmeta.get("ProductPageUrl"),
            "AvgRating":        stats.get("AverageOverallRating"),
            "TotalReviewCount": stats.get("TotalReviewCount"),
            "RecommendedCount": stats.get("RecommendedCount"),
            "TotalPhotoCount":  stats.get("TotalPhotoCount"),
            **rdist,
        }

        rows = []
        for rev in (data.get("Results") or []):
            cdv      = rev.get("ContextDataValues") or {}
            is_inc   = (cdv.get("IncentivizedReview") or {}).get("Value")
            is_staff = (cdv.get("StaffContext") or {}).get("Value")

            rows.append({
                "ProductID":       product_id,
                **product_meta,
                "ReviewID":        rev.get("Id"),
                "Rating":          rev.get("Rating"),
                "Title":           rev.get("Title"),
                "ReviewText":      rev.get("ReviewText"),
                "SubmissionTime":  rev.get("SubmissionTime"),
                "LastModTime":     rev.get("LastModificationTime"),
                "IsRecommended":   rev.get("IsRecommended"),
                "HelpfulCount":    rev.get("TotalHelpfulVoteCount"),
                "NotHelpfulCount": rev.get("TotalNegativeFeedbackCount"),
                "IsFeatured":      rev.get("IsFeatured"),
                "IsIncentivized":  is_inc,
                "IsStaffReview":   is_staff,
                "UserNickname":    rev.get("UserNickname"),
                "UserLocation":    rev.get("UserLocation"),
                "skinTone":        _context_val(rev, "skinTone"),
                "skinType":        _context_val(rev, "skinType"),
                "eyeColor":        _context_val(rev, "eyeColor"),
                "hairColor":       _context_val(rev, "hairColor"),
                "hairType":        _context_val(rev, "hairType"),
                "hairConcerns":    _tag_val(rev, "hairConcerns"),
                "skinConcerns":    _tag_val(rev, "skinConcerns"),
                "ageRange":        _context_val(rev, "ageRange"),
                "ReviewPhotoCount": len(rev.get("Photos") or []),
            })

        return rows

    except Exception as e:
        print(f"  [!] {product_id}: {e}")
        return []


def print_preview(rows):
    if not rows:
        print("\n[!] No rows to preview.")
        return

    print(f"\n{'='*70}")
    print(f"  PREVIEW — {len(rows)} reviews across {SAMPLE_SIZE} products")
    print(f"{'='*70}")

    # Group by product
    by_product = {}
    for r in rows:
        by_product.setdefault(r["ProductID"], []).append(r)

    for pid, product_rows in by_product.items():
        first = product_rows[0]
        print(f"\n  Product : {first['ProductName']}")
        print(f"  Brand   : {first['Brand']}")
        print(f"  Avg ★   : {first['AvgRating']:.2f}  ({first['TotalReviewCount']} total reviews)")
        print(f"  Ratings : 5★={first['RatingDist_5']}  4★={first['RatingDist_4']}  "
              f"3★={first['RatingDist_3']}  2★={first['RatingDist_2']}  1★={first['RatingDist_1']}")
        print(f"  URL     : {first['ProductPageUrl']}")
        print(f"  ── Sample reviews ──")
        for rev in product_rows[:3]:
            print(f"    [{rev['Rating']}★] \"{rev['Title']}\"")
            text = (rev['ReviewText'] or "")[:120].replace("\n", " ")
            print(f"         {text}{'…' if len(rev['ReviewText'] or '') > 120 else ''}")
            demo = "  |  ".join(
                f"{k}: {rev[k]}"
                for k in ("hairColor", "hairType", "skinType", "skinTone", "ageRange")
                if rev.get(k)
            )
            if demo:
                print(f"         demographics → {demo}")
            print()

    print(f"{'='*70}")
    print(f"  Columns in CSV ({len(rows[0])} total): {', '.join(rows[0].keys())}")
    print(f"{'='*70}\n")


def main():
    ids = get_sample_ids()
    if not ids:
        return

    all_rows = []
    for i, pid in enumerate(ids, 1):
        print(f"  [{i}/{len(ids)}] Fetching {pid} …")
        rows = fetch_sample_reviews(pid)
        print(f"         → {len(rows)} reviews")
        all_rows.extend(rows)
        time.sleep(0.3)

    print_preview(all_rows)

    if all_rows:
        out = cfg.OUTPUT_FILE.replace(".csv", "_SAMPLE.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"[+] Sample CSV saved → '{out}'  ({len(all_rows)} rows)\n")


if __name__ == "__main__":
    main()