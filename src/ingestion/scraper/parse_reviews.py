import re
import json
import pandas as pd
import logging

logging.basicConfig(filename='logs/scraping.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('data/raw/json/scraper_result.json') as f:
    result = json.load(f)

# Load product links from txt and deduplicate by pd_id
with open('data/raw/links/product_links.txt', 'r') as f:
    all_links = [line.strip() for line in f if line.strip()]

seen = {}
for link in all_links:
    m = re.findall(r'P[0-9]{4,9}', link)
    if m:
        pid = m[0]
        if pid not in seen:
            seen[pid] = link

pd_links_df = pd.DataFrame({'pd_id': list(seen.keys()), 'product_links': list(seen.values())})


# ── Review data ───────────────────────────────────────────────────────────────
reviews_dic = {
    'pd_id': [], 'AuthorId': [], 'Rating': [], 'Title': [],
    'ReviewText': [], 'Helpfulness': [], 'SubmissionTime': [],
    'IsRecommended': [], 'eyeColor': [], 'hairColor': [],
    'skinTone': [], 'skinType': [],
}

for pid, value in result.items():
    if not value or value[1] is None:
        continue

    reviews_data = value[1]
    if not reviews_data:
        continue

    for review in reviews_data:
        reviews_dic['pd_id'].append(pid)

        for name in ['AuthorId', 'Rating', 'Title', 'ReviewText',
                     'Helpfulness', 'SubmissionTime', 'IsRecommended']:
            reviews_dic[name].append(review.get(name))

        ctx = review.get('ContextDataValues', {})
        for name in ['eyeColor', 'hairColor', 'skinTone', 'skinType']:
            reviews_dic[name].append(ctx.get(name, {}).get('Value'))

review_df = pd.DataFrame(reviews_dic)
review_df.to_csv('data/raw/csv/review_data.csv', index=False)
logging.info(f"Saved {len(review_df)} reviews ({review_df['pd_id'].nunique()} products) to data/raw/csv/review_data.csv")