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

_EMB = pa.list_(pa.float32())   # shorthand reused below

GOLD_REVIEWS_SCHEMA = pa.schema([
    # ── All Silver business columns ──────────────────────────────────────────
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("ReviewID",                 pa.string(),                  nullable=False),
    pa.field("Rating",                   pa.int8(),                    nullable=True),
    pa.field("Title",                    pa.string(),                  nullable=True),
    pa.field("ReviewText",               pa.string(),                  nullable=True),
    pa.field("SubmissionTime",           pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("LastModTime",              pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("IsRecommended",            pa.bool_(),                   nullable=True),
    pa.field("HelpfulCount",             pa.int32(),                   nullable=True),
    pa.field("NotHelpfulCount",          pa.int32(),                   nullable=True),
    pa.field("IsFeatured",               pa.bool_(),                   nullable=True),
    pa.field("IsIncentivized",           pa.string(),                  nullable=True),
    pa.field("IsStaffReview",            pa.string(),                  nullable=True),
    pa.field("UserLocation",             pa.string(),                  nullable=True),
    pa.field("skinTone",                 pa.string(),                  nullable=True),
    pa.field("skinType",                 pa.string(),                  nullable=True),
    pa.field("eyeColor",                 pa.string(),                  nullable=True),
    pa.field("hairColor",                pa.string(),                  nullable=True),
    pa.field("hairType",                 pa.string(),                  nullable=True),
    pa.field("hairConcerns",             pa.string(),                  nullable=True),
    pa.field("skinConcerns",             pa.string(),                  nullable=True),
    pa.field("ageRange",                 pa.string(),                  nullable=True),
    pa.field("ReviewPhotoCount",         pa.int16(),                   nullable=True),
    # Silver NLP columns
    pa.field("ReviewText_clean",         pa.string(),                  nullable=True),
    pa.field("ReviewText_tokens",        pa.string(),                  nullable=True),
    pa.field("ReviewText_lemmas",        pa.string(),                  nullable=True),
    pa.field("ReviewText_wordcount",     pa.int32(),                   nullable=True),
    pa.field("Title_clean",              pa.string(),                  nullable=True),
    pa.field("Title_tokens",             pa.string(),                  nullable=True),
    pa.field("Title_lemmas",             pa.string(),                  nullable=True),
    # ── Embeddings ────────────────────────────────────────────────────────────
    pa.field("review_text_embedding",    _EMB,                         nullable=True),
    pa.field("title_embedding",          _EMB,                         nullable=True),
    # ── Quality metrics ───────────────────────────────────────────────────────
    pa.field("text_quality_score",       pa.float32(),                 nullable=True),
    pa.field("helpful_ratio",            pa.float32(),                 nullable=True),
    pa.field("review_age_days",          pa.int32(),                   nullable=True),
    pa.field("is_short_review",          pa.bool_(),                   nullable=True),
    pa.field("embedding_norm_review",    pa.float32(),                 nullable=True),
    pa.field("embedding_norm_title",     pa.float32(),                 nullable=True),
    # ── Audit ─────────────────────────────────────────────────────────────────
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_ingestion_ts",            pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("_source_file",             pa.string(),                  nullable=False),
    pa.field("_run_id",                  pa.string(),                  nullable=False),
    pa.field("_silver_run_id",           pa.string(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])

GOLD_PRODUCTS_SCHEMA = pa.schema([
    # ── All Silver business columns ──────────────────────────────────────────
    pa.field("ProductID",                pa.string(),                  nullable=False),
    pa.field("Brand",                    pa.string(),                  nullable=True),
    pa.field("ProductCategory",          pa.string(),                  nullable=True), 
    pa.field("ProductName",              pa.string(),                  nullable=True),
    pa.field("CategoryId",               pa.string(),                  nullable=True),
    pa.field("ProductPageUrl",           pa.string(),                  nullable=True),
    pa.field("AvgRating",                pa.float32(),                 nullable=True),
    pa.field("TotalReviewCount",         pa.int32(),                   nullable=True),
    pa.field("RecommendedCount",         pa.int32(),                   nullable=True),
    pa.field("TotalPhotoCount",          pa.int32(),                   nullable=True),
    pa.field("RatingDist_1",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_2",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_3",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_4",             pa.int32(),                   nullable=True),
    pa.field("RatingDist_5",             pa.int32(),                   nullable=True),
    # Silver NLP columns
    pa.field("ProductName_clean",        pa.string(),                  nullable=True),
    pa.field("ProductName_tokens",       pa.string(),                  nullable=True),
    pa.field("ProductName_lemmas",       pa.string(),                  nullable=True),
    # ── Embeddings ────────────────────────────────────────────────────────────
    pa.field("product_name_embedding",   _EMB,                         nullable=True),
    # ── Quality metrics ───────────────────────────────────────────────────────
    pa.field("rating_entropy",           pa.float32(),                 nullable=True),
    pa.field("polarization_score",       pa.float32(),                 nullable=True),
    pa.field("embedding_norm_name",      pa.float32(),                 nullable=True),
    # ── Audit ─────────────────────────────────────────────────────────────────
    pa.field("revision_date",            pa.date32(),                  nullable=False),
    pa.field("_ingestion_ts",            pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("_source_file",             pa.string(),                  nullable=False),
    pa.field("_run_id",                  pa.string(),                  nullable=False),
    pa.field("_silver_run_id",           pa.string(),                  nullable=False),
    pa.field("_gold_run_id",             pa.string(),                  nullable=False),
])