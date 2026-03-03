import re
import json
import pandas as pd

with open('scraping/data/json/scraper_result.json') as f:
    result = json.load(f)

# Load product links from txt and deduplicate by pd_id
with open('scraping/data/txt/product_links.txt', 'r') as f:
    all_links = [line.strip() for line in f if line.strip()]

seen = {}
for link in all_links:
    m = re.findall(r'P[0-9]{4,9}', link)
    if m:
        pid = m[0]
        if pid not in seen:
            seen[pid] = link

pd_links_df = pd.DataFrame({'pd_id': list(seen.keys()), 'product_links': list(seen.values())})

# ── Product data ──────────────────────────────────────────────────────────────
for pid, value in result.items():
    if not value or value[0] is None:
        continue

    product_result = value[0]
    if not product_result:
        continue

    try:
        product_result = product_result[pid]
    except KeyError:
        try:
            product_result = product_result[pid.lower()]
        except KeyError:
            product_result = next(iter(product_result.values()), None)

    if not product_result:
        continue

    for name in ['Name', 'Description']:
        try:
            pd_links_df.loc[pd_links_df['pd_id'] == pid, name] = product_result[name]
        except KeyError:
            pass

    stats = product_result.get('ReviewStatistics', {})
    for name in ['AverageOverallRating', 'FirstSubmissionTime', 'LastSubmissionTime']:
        try:
            pd_links_df.loc[pd_links_df['pd_id'] == pid, name] = stats[name]
        except KeyError:
            pass

    try:
        age_data = stats['ContextDataDistribution']['age']['Values']
        for age_group in age_data:
            pd_links_df.loc[pd_links_df['pd_id'] == pid,
                            f'Age_{age_group["Value"]}'] = age_group['Count']
    except KeyError:
        pass

pd_links_df.to_csv('scraping/data/csv/product_data.csv', index=False)
print(f"Saved {len(pd_links_df)} products to scraping/data/csv/product_data.csv")

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
review_df.to_csv('scraping/data/csv/review_data.csv', index=False)
print(f"Saved {len(review_df)} reviews ({review_df['pd_id'].nunique()} products) to scraping/data/csv/review_data.csv")