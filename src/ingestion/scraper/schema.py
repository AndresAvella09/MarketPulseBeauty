"""
schema.py
─────────
Single source of truth for the Sephora DataOps pipeline.

Defines:
  • PRODUCT_FIELDS  / REVIEW_FIELDS        – field lists used by the scraper
  • PRODUCTS_SCHEMA / REVIEWS_SCHEMA       – Bronze PyArrow schemas
  • SILVER_PRODUCTS_SCHEMA                 – Silver PyArrow schema (products)
  • SILVER_REVIEWS_SCHEMA                  – Silver PyArrow schema (reviews + NLP cols)
  • GOLD_BRAND_SCHEMA                      – Gold brand-level aggregates
  • GOLD_PRODUCT_SCHEMA                    – Gold product-level aggregates
  • GOLD_DEMOGRAPHIC_SCHEMA                – Gold demographic breakdown
  • GOLD_CATEGORY_SCHEMA                   – Gold category trends
"""

import pyarrow as pa

# ── Field lists (used by scraper to split raw rows) ───────────────────────────
#
# Keep these in sync with the PyArrow schemas below.
# The scraper never needs to hard-code column names anywhere else.

PRODUCT_FIELDS = [
    "ProductID",
    "Brand",
    "ProductName",
    "CategoryId",
    "ProductPageUrl",
    "AvgRating",
    "TotalReviewCount",
    "RecommendedCount",
    "TotalPhotoCount",
    "RatingDist_1",
    "RatingDist_2",
    "RatingDist_3",
    "RatingDist_4",
    "RatingDist_5",
]

REVIEW_FIELDS = [
    "ProductID",
    "ReviewID",
    "Rating",
    "Title",
    "ReviewText",
    "SubmissionTime",
    "LastModTime",
    "IsRecommended",
    "HelpfulCount",
    "NotHelpfulCount",
    "IsFeatured",
    "IsIncentivized",
    "IsStaffReview",
    "UserLocation",
    "skinTone",
    "skinType",
    "eyeColor",
    "hairColor",
    "hairType",
    "hairConcerns",
    "skinConcerns",
    "ageRange",
    "ReviewPhotoCount",
]

# ── Audit columns added by bronze ingestion (not emitted by the scraper) ──────

AUDIT_FIELDS = ["_ingestion_ts", "_source_file", "_run_id"]

# ── PyArrow schemas (used by bronze_ingestion.py) ─────────────────────────────
#
# Audit fields are appended at the end so the business columns stay first.

PRODUCTS_SCHEMA = pa.schema([
    # business
    pa.field("ProductID",         pa.string(),                  nullable=False),
    pa.field("Brand",             pa.string(),                  nullable=True),
    pa.field("ProductName",       pa.string(),                  nullable=True),
    pa.field("CategoryId",        pa.string(),                  nullable=True),
    pa.field("ProductPageUrl",    pa.string(),                  nullable=True),
    pa.field("AvgRating",         pa.float32(),                 nullable=True),
    pa.field("TotalReviewCount",  pa.int32(),                   nullable=True),
    pa.field("RecommendedCount",  pa.int32(),                   nullable=True),
    pa.field("TotalPhotoCount",   pa.int32(),                   nullable=True),
    pa.field("RatingDist_1",      pa.int32(),                   nullable=True),
    pa.field("RatingDist_2",      pa.int32(),                   nullable=True),
    pa.field("RatingDist_3",      pa.int32(),                   nullable=True),
    pa.field("RatingDist_4",      pa.int32(),                   nullable=True),
    pa.field("RatingDist_5",      pa.int32(),                   nullable=True),
    # audit
    pa.field("_ingestion_ts",     pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("_source_file",      pa.string(),                  nullable=False),
    pa.field("_run_id",           pa.string(),                  nullable=False),
])

REVIEWS_SCHEMA = pa.schema([
    # business
    pa.field("ProductID",         pa.string(),                  nullable=False),
    pa.field("ReviewID",          pa.string(),                  nullable=False),
    pa.field("Rating",            pa.int8(),                    nullable=True),
    pa.field("Title",             pa.string(),                  nullable=True),
    pa.field("ReviewText",        pa.string(),                  nullable=True),
    pa.field("SubmissionTime",    pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("LastModTime",       pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("IsRecommended",     pa.bool_(),                   nullable=True),
    pa.field("HelpfulCount",      pa.int32(),                   nullable=True),
    pa.field("NotHelpfulCount",   pa.int32(),                   nullable=True),
    pa.field("IsFeatured",        pa.bool_(),                   nullable=True),
    pa.field("IsIncentivized",    pa.string(),                  nullable=True),
    pa.field("IsStaffReview",     pa.string(),                  nullable=True),
    pa.field("UserLocation",      pa.string(),                  nullable=True),
    pa.field("skinTone",          pa.string(),                  nullable=True),
    pa.field("skinType",          pa.string(),                  nullable=True),
    pa.field("eyeColor",          pa.string(),                  nullable=True),
    pa.field("hairColor",         pa.string(),                  nullable=True),
    pa.field("hairType",          pa.string(),                  nullable=True),
    pa.field("hairConcerns",      pa.string(),                  nullable=True),
    pa.field("skinConcerns",      pa.string(),                  nullable=True),
    pa.field("ageRange",          pa.string(),                  nullable=True),
    pa.field("ReviewPhotoCount",  pa.int16(),                   nullable=True),
    # audit
    pa.field("_ingestion_ts",     pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("_source_file",      pa.string(),                  nullable=False),
    pa.field("_run_id",           pa.string(),                  nullable=False),
])

# ── Silver schemas ─────────────────────────────────────────────────────────────
#
# Silver inherits all Bronze business + audit fields and adds NLP-derived cols.
# Text fields that are cleaned get a *_clean, *_tokens, *_lemmas sibling.

SILVER_PRODUCTS_SCHEMA = pa.schema([
    # Bronze business cols
    pa.field("ProductID",            pa.string(),                  nullable=False),
    pa.field("Brand",                pa.string(),                  nullable=True),
    pa.field("ProductCategory",      pa.string(),                  nullable=True), 
    pa.field("ProductName",          pa.string(),                  nullable=True),
    pa.field("CategoryId",           pa.string(),                  nullable=True),
    pa.field("ProductPageUrl",       pa.string(),                  nullable=True),
    pa.field("AvgRating",            pa.float32(),                 nullable=True),
    pa.field("TotalReviewCount",     pa.int32(),                   nullable=True),
    pa.field("RecommendedCount",     pa.int32(),                   nullable=True),
    pa.field("TotalPhotoCount",      pa.int32(),                   nullable=True),
    pa.field("RatingDist_1",         pa.int32(),                   nullable=True),
    pa.field("RatingDist_2",         pa.int32(),                   nullable=True),
    pa.field("RatingDist_3",         pa.int32(),                   nullable=True),
    pa.field("RatingDist_4",         pa.int32(),                   nullable=True),
    pa.field("RatingDist_5",         pa.int32(),                   nullable=True),
    # NLP-derived
    pa.field("ProductName_clean",    pa.string(),                  nullable=True),
    pa.field("ProductName_tokens",   pa.string(),                  nullable=True),
    pa.field("ProductName_lemmas",   pa.string(),                  nullable=True),
    # Silver audit
    pa.field("revision_date",        pa.date32(),                  nullable=False),
    pa.field("_ingestion_ts",        pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("_source_file",         pa.string(),                  nullable=False),
    pa.field("_run_id",              pa.string(),                  nullable=False),
    pa.field("_silver_run_id",       pa.string(),                  nullable=False),
])

SILVER_REVIEWS_SCHEMA = pa.schema([
    # Bronze business cols
    pa.field("ProductID",            pa.string(),                  nullable=False),
    pa.field("ReviewID",             pa.string(),                  nullable=False),
    pa.field("Rating",               pa.int8(),                    nullable=True),
    pa.field("Title",                pa.string(),                  nullable=True),
    pa.field("ReviewText",           pa.string(),                  nullable=True),
    pa.field("SubmissionTime",       pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("LastModTime",          pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("IsRecommended",        pa.bool_(),                   nullable=True),
    pa.field("HelpfulCount",         pa.int32(),                   nullable=True),
    pa.field("NotHelpfulCount",      pa.int32(),                   nullable=True),
    pa.field("IsFeatured",           pa.bool_(),                   nullable=True),
    pa.field("IsIncentivized",       pa.string(),                  nullable=True),
    pa.field("IsStaffReview",        pa.string(),                  nullable=True),
    pa.field("UserLocation",         pa.string(),                  nullable=True),
    pa.field("skinTone",             pa.string(),                  nullable=True),
    pa.field("skinType",             pa.string(),                  nullable=True),
    pa.field("eyeColor",             pa.string(),                  nullable=True),
    pa.field("hairColor",            pa.string(),                  nullable=True),
    pa.field("hairType",             pa.string(),                  nullable=True),
    pa.field("hairConcerns",         pa.string(),                  nullable=True),
    pa.field("skinConcerns",         pa.string(),                  nullable=True),
    pa.field("ageRange",             pa.string(),                  nullable=True),
    pa.field("ReviewPhotoCount",     pa.int16(),                   nullable=True),
    # NLP-derived (ReviewText)
    pa.field("ReviewText_clean",     pa.string(),                  nullable=True),
    pa.field("ReviewText_tokens",    pa.string(),                  nullable=True),
    pa.field("ReviewText_lemmas",    pa.string(),                  nullable=True),
    pa.field("ReviewText_wordcount", pa.int32(),                   nullable=True),
    # NLP-derived (Title)
    pa.field("Title_clean",          pa.string(),                  nullable=True),
    pa.field("Title_tokens",         pa.string(),                  nullable=True),
    pa.field("Title_lemmas",         pa.string(),                  nullable=True),
    # Silver audit
    pa.field("revision_date",        pa.date32(),                  nullable=False),
    pa.field("_ingestion_ts",        pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("_source_file",         pa.string(),                  nullable=False),
    pa.field("_run_id",              pa.string(),                  nullable=False),
    pa.field("_silver_run_id",       pa.string(),                  nullable=False),
])

# ── Gold schemas ───────────────────────────────────────────────────────────────
#
# Slim, insights-only schemas. Heavy artifacts (embeddings, tokens, lemmas) live
# in MinIO under marketpulse-gold/embeddings/ — see GOLD_*_EMBEDDINGS_SCHEMA.

_EMB = pa.list_(pa.float32())   # shorthand reused below

GOLD_REVIEWS_SCHEMA = pa.schema([
    # Identifiers
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("ReviewID",                 pa.string(),                  nullable=False),
    # Core review fields used by the dashboard
    pa.field("Rating",                   pa.int8(),                    nullable=True),
    pa.field("Title",                    pa.string(),                  nullable=True),
    pa.field("ReviewText",               pa.string(),                  nullable=True),
    pa.field("SubmissionTime",           pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("LastModTime",              pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("IsRecommended",            pa.bool_(),                   nullable=True),
    pa.field("ReviewPhotoCount",         pa.int16(),                   nullable=True),
    # Trust / engagement signals (Bronze fields kept in Gold)
    pa.field("HelpfulCount",             pa.int32(),                   nullable=True),
    pa.field("NotHelpfulCount",          pa.int32(),                   nullable=True),
    pa.field("IsFeatured",               pa.bool_(),                   nullable=True),
    pa.field("IsIncentivized",           pa.bool_(),                   nullable=True),
    pa.field("IsStaffReview",            pa.bool_(),                   nullable=True),
    # Demographics (used as filters in the dashboard)
    pa.field("UserLocation",             pa.string(),                  nullable=True),
    pa.field("skinTone",                 pa.string(),                  nullable=True),
    pa.field("skinType",                 pa.string(),                  nullable=True),
    pa.field("eyeColor",                 pa.string(),                  nullable=True),
    pa.field("hairColor",                pa.string(),                  nullable=True),
    pa.field("hairType",                 pa.string(),                  nullable=True),
    pa.field("hairConcerns",             pa.string(),                  nullable=True),
    pa.field("skinConcerns",             pa.string(),                  nullable=True),
    pa.field("ageRange",                 pa.string(),                  nullable=True),
    # Derived insights
    pa.field("helpful_ratio",            pa.float32(),                 nullable=True),
    pa.field("review_age_days",          pa.int32(),                   nullable=True),
    pa.field("is_short_review",          pa.bool_(),                   nullable=True),
    pa.field("text_quality_score",       pa.float32(),                 nullable=True),
    pa.field("ReviewText_wordcount",     pa.int32(),                   nullable=True),
    pa.field("ReviewText_lemmas",        pa.string(),                  nullable=True),
    pa.field("Title_lemmas",             pa.string(),                  nullable=True),
    pa.field("sentiment_score",          pa.float32(),                 nullable=True),
    pa.field("sentiment_label",          pa.string(),                  nullable=True),
    pa.field("topic_id",                 pa.int32(),                   nullable=True),
    pa.field("topic_label",              pa.string(),                  nullable=True),
    # 2D projection of review_text_embedding for the "Mapa de reseñas" view.
    # Full embeddings stay in MinIO; only the 2D coords are promoted to Gold.
    pa.field("umap_x",                   pa.float32(),                 nullable=True),
    pa.field("umap_y",                   pa.float32(),                 nullable=True),
    # Audit
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

GOLD_PRODUCTS_SCHEMA = pa.schema([
    # Identifiers
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("Brand",                    pa.string(),                  nullable=True),
    pa.field("ProductName",              pa.string(),                  nullable=True),
    pa.field("ProductCategory",          pa.string(),                  nullable=True),
    pa.field("CategoryId",               pa.string(),                  nullable=True),
    pa.field("ProductPageUrl",           pa.string(),                  nullable=True),
    # Family taxonomy (precomputed — was runtime in dashboard data_loader)
    pa.field("focus_keyword",            pa.string(),                  nullable=True),
    # Aggregates
    pa.field("AvgRating",                pa.float32(),                 nullable=True),
    pa.field("TotalReviewCount",         pa.int32(),                   nullable=True),
    pa.field("RecommendedCount",         pa.int32(),                   nullable=True),
    pa.field("TotalPhotoCount",          pa.int32(),                   nullable=True),
    pa.field("RatingDist_1",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_2",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_3",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_4",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_5",             pa.int32(),                   nullable=True),
    # Derived insights
    pa.field("pct_recommended",          pa.float32(),                 nullable=True),
    pa.field("rating_entropy",           pa.float32(),                 nullable=True),
    pa.field("polarization_score",       pa.float32(),                 nullable=True),
    pa.field("avg_sentiment",            pa.float32(),                 nullable=True),
    pa.field("health_score",             pa.float32(),                 nullable=True),
    # Activity span / velocity (precomputed at gold build)
    pa.field("first_review_date",        pa.date32(),                  nullable=True),
    pa.field("last_review_date",         pa.date32(),                  nullable=True),
    pa.field("review_velocity_30d",      pa.int32(),                   nullable=True),
    pa.field("review_velocity_prior_30d",pa.int32(),                   nullable=True),
    pa.field("photo_coverage",           pa.float32(),                 nullable=True),
    pa.field("edit_rate",                pa.float32(),                 nullable=True),
    # Pre-rendered "voice" snippets (JSON-encoded text)
    pa.field("top_quote_positive",       pa.string(),                  nullable=True),
    pa.field("top_quote_negative",       pa.string(),                  nullable=True),
    pa.field("top_quote_neutral",        pa.string(),                  nullable=True),
    pa.field("top_locations",            pa.string(),                  nullable=True),
    # Audit
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# Per-product, per-day snapshot (last 90 days). Powers "last 7 days" widgets.
GOLD_PRODUCT_INSIGHTS_DAILY_SCHEMA = pa.schema([
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("day",                      pa.date32(),                  nullable=False),
    pa.field("reviews_count",            pa.int32(),                   nullable=True),
    pa.field("avg_rating",               pa.float32(),                 nullable=True),
    pa.field("pct_positive",             pa.float32(),                 nullable=True),
    pa.field("pct_negative",             pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# One row per brand. Pre-aggregated for the "Brand leaderboard" page.
GOLD_BRANDS_SCHEMA = pa.schema([
    pa.field("brand",                       pa.string(),               nullable=False),
    pa.field("products_count",              pa.int32(),                nullable=True),
    pa.field("total_reviews",               pa.int32(),                nullable=True),
    pa.field("avg_rating",                  pa.float32(),              nullable=True),
    pa.field("pct_recommended",             pa.float32(),              nullable=True),
    pa.field("avg_sentiment",               pa.float32(),              nullable=True),
    pa.field("avg_health_score",            pa.float32(),              nullable=True),
    pa.field("polarization_score",          pa.float32(),              nullable=True),
    pa.field("share_niacinamida",           pa.float32(),              nullable=True),
    pa.field("share_acido_hialuronico",     pa.float32(),              nullable=True),
    pa.field("share_shampoo_sin_sulfatos",  pa.float32(),              nullable=True),
    pa.field("top_product_id",              pa.string(),               nullable=True),
    pa.field("revision_date",               pa.date32(),               nullable=False),
    pa.field("_gold_run_id",                pa.string(),               nullable=False),
])

# One row per focus_keyword (3 rows). Family-level KPI strip.
GOLD_PRODUCT_FAMILIES_SCHEMA = pa.schema([
    pa.field("focus_keyword",            pa.string(),                  nullable=False),
    pa.field("products_count",           pa.int32(),                   nullable=True),
    pa.field("brands_count",             pa.int32(),                   nullable=True),
    pa.field("total_reviews",            pa.int32(),                   nullable=True),
    pa.field("avg_rating",               pa.float32(),                 nullable=True),
    pa.field("avg_sentiment",            pa.float32(),                 nullable=True),
    pa.field("pct_recommended",          pa.float32(),                 nullable=True),
    pa.field("health_score_p50",         pa.float32(),                 nullable=True),
    pa.field("health_score_p90",         pa.float32(),                 nullable=True),
    pa.field("top_brand",                pa.string(),                  nullable=True),
    pa.field("top_product_id",           pa.string(),                  nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# focus_keyword × demographic_dim × demographic_value rollup.
GOLD_DEMOGRAPHIC_INSIGHTS_SCHEMA = pa.schema([
    pa.field("focus_keyword",            pa.string(),                  nullable=False),
    pa.field("demographic_dim",          pa.string(),                  nullable=False),
    pa.field("demographic_value",        pa.string(),                  nullable=False),
    pa.field("reviews_count",            pa.int32(),                   nullable=True),
    pa.field("avg_rating",               pa.float32(),                 nullable=True),
    pa.field("avg_sentiment",            pa.float32(),                 nullable=True),
    pa.field("pct_recommended",          pa.float32(),                 nullable=True),
    pa.field("pct_positive",             pa.float32(),                 nullable=True),
    pa.field("pct_negative",             pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# Pre-joined demand (Google Trends) × supply (review counts) per family per month.
GOLD_FAMILY_DEMAND_SUPPLY_SCHEMA = pa.schema([
    pa.field("focus_keyword",            pa.string(),                  nullable=False),
    pa.field("month",                    pa.date32(),                  nullable=False),
    pa.field("search_interest_avg",      pa.float32(),                 nullable=True),
    pa.field("reviews_count",            pa.int32(),                   nullable=True),
    pa.field("avg_sentiment",            pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# Per-product, per-month snapshot for delta KPIs and trend lines.
GOLD_PRODUCT_INSIGHTS_MONTHLY_SCHEMA = pa.schema([
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("month",                    pa.date32(),                  nullable=False),  # first-of-month
    pa.field("reviews_count",            pa.int32(),                   nullable=True),
    pa.field("avg_rating",               pa.float32(),                 nullable=True),
    pa.field("pct_recommended",          pa.float32(),                 nullable=True),
    pa.field("pct_positive",             pa.float32(),                 nullable=True),
    pa.field("pct_neutral",              pa.float32(),                 nullable=True),
    pa.field("pct_negative",             pa.float32(),                 nullable=True),
    pa.field("avg_helpful_ratio",        pa.float32(),                 nullable=True),
    pa.field("health_score",             pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# Theme rollup per product → powers "Temas recurrentes" and "Principales quejas".
GOLD_REVIEW_THEMES_SCHEMA = pa.schema([
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("topic_id",                 pa.int32(),                   nullable=False),
    pa.field("theme_label",              pa.string(),                  nullable=True),
    pa.field("polarity",                 pa.string(),                  nullable=True),  # 'pos' | 'neg' | 'neu'
    pa.field("count",                    pa.int32(),                   nullable=True),
    pa.field("pct",                      pa.float32(),                 nullable=True),
    pa.field("avg_sentiment",            pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# Google Trends serving table.
GOLD_SEARCH_TRENDS_SCHEMA = pa.schema([
    pa.field("keyword",                  pa.string(),                  nullable=False),
    pa.field("geo",                      pa.string(),                  nullable=True),
    pa.field("date",                     pa.date32(),                  nullable=False),
    pa.field("interest",                 pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# Spike detections rolled up from search trends.
GOLD_SEARCH_SPIKES_SCHEMA = pa.schema([
    pa.field("keyword",                  pa.string(),                  nullable=False),
    pa.field("geo",                      pa.string(),                  nullable=True),
    pa.field("start_date",               pa.date32(),                  nullable=False),
    pa.field("end_date",                 pa.date32(),                  nullable=False),
    pa.field("pct_change",               pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

# Embeddings live in MinIO marketpulse-gold/embeddings/ — never in Postgres.
GOLD_REVIEW_EMBEDDINGS_SCHEMA = pa.schema([
    pa.field("ReviewID",                 pa.string(),                  nullable=False),
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("SubmissionTime",           pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("review_text_embedding",    _EMB,                         nullable=True),
    pa.field("title_embedding",          _EMB,                         nullable=True),
    pa.field("embedding_norm_review",    pa.float32(),                 nullable=True),
    pa.field("embedding_norm_title",     pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

GOLD_PRODUCT_EMBEDDINGS_SCHEMA = pa.schema([
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("product_name_embedding",   _EMB,                         nullable=True),
    pa.field("embedding_norm_name",      pa.float32(),                 nullable=True),
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])