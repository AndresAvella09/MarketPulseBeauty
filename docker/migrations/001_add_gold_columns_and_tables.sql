-- Migration: bring an existing marketpulse DB up to the current init-postgres.sql.
-- Safe to re-run; every statement uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.

\connect marketpulse

-- ── gold.products: derived extras ────────────────────────────────────────────
ALTER TABLE gold.products
    ADD COLUMN IF NOT EXISTS first_review_date         DATE,
    ADD COLUMN IF NOT EXISTS last_review_date          DATE,
    ADD COLUMN IF NOT EXISTS review_velocity_30d       INTEGER,
    ADD COLUMN IF NOT EXISTS review_velocity_prior_30d INTEGER,
    ADD COLUMN IF NOT EXISTS photo_coverage            REAL,
    ADD COLUMN IF NOT EXISTS edit_rate                 REAL,
    ADD COLUMN IF NOT EXISTS top_quote_positive        TEXT,
    ADD COLUMN IF NOT EXISTS top_quote_negative        TEXT,
    ADD COLUMN IF NOT EXISTS top_quote_neutral         TEXT,
    ADD COLUMN IF NOT EXISTS top_locations             TEXT;

-- ── gold.reviews: trust signals + lemmas + 2D coords ─────────────────────────
ALTER TABLE gold.reviews
    ADD COLUMN IF NOT EXISTS last_mod_time         TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS helpful_count         INTEGER,
    ADD COLUMN IF NOT EXISTS not_helpful_count     INTEGER,
    ADD COLUMN IF NOT EXISTS is_featured           BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_incentivized       BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_staff_review       BOOLEAN,
    ADD COLUMN IF NOT EXISTS eye_color             TEXT,
    ADD COLUMN IF NOT EXISTS hair_color            TEXT,
    ADD COLUMN IF NOT EXISTS review_text_wordcount INTEGER,
    ADD COLUMN IF NOT EXISTS review_text_lemmas    TEXT,
    ADD COLUMN IF NOT EXISTS title_lemmas          TEXT,
    ADD COLUMN IF NOT EXISTS umap_x                REAL,
    ADD COLUMN IF NOT EXISTS umap_y                REAL;

CREATE INDEX IF NOT EXISTS idx_reviews_is_incentivized ON gold.reviews (is_incentivized);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment_label ON gold.reviews (sentiment_label);

-- ── New tables ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.product_insights_daily (
    product_id    TEXT,
    day           DATE,
    reviews_count INTEGER,
    avg_rating    REAL,
    pct_positive  REAL,
    pct_negative  REAL,
    revision_date DATE,
    gold_run_id   TEXT,
    PRIMARY KEY (product_id, day)
);
CREATE INDEX IF NOT EXISTS idx_pid_day ON gold.product_insights_daily (day);

CREATE TABLE IF NOT EXISTS gold.brands (
    brand                       TEXT PRIMARY KEY,
    products_count              INTEGER,
    total_reviews               INTEGER,
    avg_rating                  REAL,
    pct_recommended             REAL,
    avg_sentiment               REAL,
    avg_health_score            REAL,
    polarization_score          REAL,
    share_niacinamida           REAL,
    share_acido_hialuronico     REAL,
    share_shampoo_sin_sulfatos  REAL,
    top_product_id              TEXT,
    revision_date               DATE,
    gold_run_id                 TEXT
);
CREATE INDEX IF NOT EXISTS idx_brands_health ON gold.brands (avg_health_score);

CREATE TABLE IF NOT EXISTS gold.product_families (
    focus_keyword     TEXT PRIMARY KEY,
    products_count    INTEGER,
    brands_count      INTEGER,
    total_reviews     INTEGER,
    avg_rating        REAL,
    avg_sentiment     REAL,
    pct_recommended   REAL,
    health_score_p50  REAL,
    health_score_p90  REAL,
    top_brand         TEXT,
    top_product_id    TEXT,
    revision_date     DATE,
    gold_run_id       TEXT
);

CREATE TABLE IF NOT EXISTS gold.demographic_insights (
    focus_keyword     TEXT,
    demographic_dim   TEXT,
    demographic_value TEXT,
    reviews_count     INTEGER,
    avg_rating        REAL,
    avg_sentiment     REAL,
    pct_recommended   REAL,
    pct_positive      REAL,
    pct_negative      REAL,
    revision_date     DATE,
    gold_run_id       TEXT,
    PRIMARY KEY (focus_keyword, demographic_dim, demographic_value)
);
CREATE INDEX IF NOT EXISTS idx_demographic_dim ON gold.demographic_insights (demographic_dim);

CREATE TABLE IF NOT EXISTS gold.family_demand_supply (
    focus_keyword       TEXT,
    month               DATE,
    search_interest_avg REAL,
    reviews_count       INTEGER,
    avg_sentiment       REAL,
    revision_date       DATE,
    gold_run_id         TEXT,
    PRIMARY KEY (focus_keyword, month)
);
