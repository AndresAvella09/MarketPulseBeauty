# Technical Backlog

## Health score master table
- [ ] Define a master table schema that unifies sentiment and volume per product (category, product_id, sentiment_score, mention_count, source metadata).
- [ ] Add a pipeline step to aggregate review-level sentiment into per-product sentiment_score (mean) and review_count (volume proxy).
- [ ] Add a pipeline step to compute mention_count per product from all volume sources and store it as parquet.
- [ ] Build a DVC stage that materializes the master table parquet and tracks it in DVC.
- [ ] Add data quality checks for missing category/product_id, negative volume, and sentiment range [-1, 1].
