-- Create the marketpulse database and gold schema
CREATE DATABASE marketpulse;

\connect marketpulse

CREATE SCHEMA IF NOT EXISTS gold;

-- ── Slim products table — one row per product ────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.products (
    product_id                 TEXT PRIMARY KEY,
    brand                      TEXT,
    product_name               TEXT,
    product_category           TEXT,
    category_id                TEXT,
    product_page_url           TEXT,
    focus_keyword              TEXT,
    avg_rating                 REAL,
    total_review_count         INTEGER,
    recommended_count          INTEGER,
    total_photo_count          INTEGER,
    rating_dist_1              INTEGER,
    rating_dist_2              INTEGER,
    rating_dist_3              INTEGER,
    rating_dist_4              INTEGER,
    rating_dist_5              INTEGER,
    pct_recommended            REAL,
    rating_entropy             REAL,
    polarization_score         REAL,
    avg_sentiment              REAL,
    health_score               REAL,
    -- Activity span / velocity (precomputed at gold build) ───────────────
    first_review_date          DATE,
    last_review_date           DATE,
    review_velocity_30d        INTEGER,
    review_velocity_prior_30d  INTEGER,
    photo_coverage             REAL,
    edit_rate                  REAL,
    -- Pre-rendered "voice" snippets (JSON-encoded TEXT, dashboard json.loads)
    top_quote_positive         TEXT,
    top_quote_negative         TEXT,
    top_quote_neutral          TEXT,
    top_locations              TEXT,
    revision_date              DATE,
    gold_run_id                TEXT
);
CREATE INDEX IF NOT EXISTS idx_products_focus_keyword ON gold.products (focus_keyword);
CREATE INDEX IF NOT EXISTS idx_products_brand          ON gold.products (brand);

-- ── Slim reviews table — one row per review, no embeddings ───────────────────
CREATE TABLE IF NOT EXISTS gold.reviews (
    review_id              TEXT PRIMARY KEY,
    product_id             TEXT,
    rating                 SMALLINT,
    title                  TEXT,
    review_text            TEXT,
    submission_time        TIMESTAMPTZ,
    last_mod_time          TIMESTAMPTZ,
    is_recommended         BOOLEAN,
    review_photo_count     SMALLINT,
    -- Trust / engagement signals promoted from Bronze ────────────────────
    helpful_count          INTEGER,
    not_helpful_count      INTEGER,
    is_featured            BOOLEAN,
    is_incentivized        BOOLEAN,
    is_staff_review        BOOLEAN,
    -- Reviewer demographics (full set) ────────────────────────────────────
    user_location          TEXT,
    skin_tone              TEXT,
    skin_type              TEXT,
    eye_color              TEXT,
    hair_color             TEXT,
    hair_type              TEXT,
    hair_concerns          TEXT,
    skin_concerns          TEXT,
    age_range              TEXT,
    -- Quality / NLP ───────────────────────────────────────────────────────
    helpful_ratio          REAL,
    review_age_days        INTEGER,
    is_short_review        BOOLEAN,
    text_quality_score     REAL,
    review_text_wordcount  INTEGER,
    review_text_lemmas     TEXT,
    title_lemmas           TEXT,
    sentiment_score        REAL,
    sentiment_label        TEXT,
    topic_id               INTEGER,
    topic_label            TEXT,
    -- 2D projection of review_text_embedding ──────────────────────────────
    umap_x                 REAL,
    umap_y                 REAL,
    revision_date          DATE,
    gold_run_id            TEXT
);
CREATE INDEX IF NOT EXISTS idx_reviews_is_incentivized ON gold.reviews (is_incentivized);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment_label ON gold.reviews (sentiment_label);
CREATE INDEX IF NOT EXISTS idx_reviews_product_id      ON gold.reviews (product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_submission_time ON gold.reviews (submission_time);
CREATE INDEX IF NOT EXISTS idx_reviews_topic_id        ON gold.reviews (topic_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating          ON gold.reviews (rating);

-- ── Per-product monthly snapshot (powers delta KPIs + trend lines) ───────────
CREATE TABLE IF NOT EXISTS gold.product_insights_monthly (
    product_id           TEXT,
    month                DATE,
    reviews_count        INTEGER,
    avg_rating           REAL,
    pct_recommended      REAL,
    pct_positive         REAL,
    pct_neutral          REAL,
    pct_negative         REAL,
    avg_helpful_ratio    REAL,
    health_score         REAL,
    revision_date        DATE,
    gold_run_id          TEXT,
    PRIMARY KEY (product_id, month)
);
CREATE INDEX IF NOT EXISTS idx_pim_month ON gold.product_insights_monthly (month);

-- ── Theme rollup per product (Temas recurrentes / Principales quejas) ────────
CREATE TABLE IF NOT EXISTS gold.review_themes (
    product_id           TEXT,
    topic_id             INTEGER,
    theme_label          TEXT,
    polarity             TEXT,
    count                INTEGER,
    pct                  REAL,
    avg_sentiment        REAL,
    revision_date        DATE,
    gold_run_id          TEXT,
    PRIMARY KEY (product_id, topic_id)
);
CREATE INDEX IF NOT EXISTS idx_review_themes_polarity ON gold.review_themes (polarity);

-- ── Google Trends serving table ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.search_trends (
    keyword              TEXT,
    geo                  TEXT,
    date                 DATE,
    interest             REAL,
    revision_date        DATE,
    gold_run_id          TEXT,
    PRIMARY KEY (keyword, geo, date)
);
CREATE INDEX IF NOT EXISTS idx_search_trends_date ON gold.search_trends (date);

-- ── Detected spikes (Picos de búsqueda) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.search_spikes (
    keyword              TEXT,
    geo                  TEXT,
    start_date           DATE,
    end_date             DATE,
    pct_change           REAL,
    revision_date        DATE,
    gold_run_id          TEXT,
    PRIMARY KEY (keyword, geo, start_date)
);

-- ── Per-product daily snapshot (last 90 days, "last 7 days" view) ────────────
CREATE TABLE IF NOT EXISTS gold.product_insights_daily (
    product_id           TEXT,
    day                  DATE,
    reviews_count        INTEGER,
    avg_rating           REAL,
    pct_positive         REAL,
    pct_negative         REAL,
    revision_date        DATE,
    gold_run_id          TEXT,
    PRIMARY KEY (product_id, day)
);
CREATE INDEX IF NOT EXISTS idx_pid_day ON gold.product_insights_daily (day);

-- ── Brand-level aggregate (one row per brand) ────────────────────────────────
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

-- ── Family-level aggregate (one row per focus_keyword) ───────────────────────
CREATE TABLE IF NOT EXISTS gold.product_families (
    focus_keyword        TEXT PRIMARY KEY,
    products_count       INTEGER,
    brands_count         INTEGER,
    total_reviews        INTEGER,
    avg_rating           REAL,
    avg_sentiment        REAL,
    pct_recommended      REAL,
    health_score_p50     REAL,
    health_score_p90     REAL,
    top_brand            TEXT,
    top_product_id       TEXT,
    revision_date        DATE,
    gold_run_id          TEXT
);

-- ── Demographic insights (family × demographic_dim × demographic_value) ──────
CREATE TABLE IF NOT EXISTS gold.demographic_insights (
    focus_keyword        TEXT,
    demographic_dim      TEXT,    -- 'skin_tone' | 'skin_type' | 'hair_type' | 'hair_concerns' | 'skin_concerns' | 'age_range' | 'eye_color' | 'hair_color'
    demographic_value    TEXT,
    reviews_count        INTEGER,
    avg_rating           REAL,
    avg_sentiment        REAL,
    pct_recommended      REAL,
    pct_positive         REAL,
    pct_negative         REAL,
    revision_date        DATE,
    gold_run_id          TEXT,
    PRIMARY KEY (focus_keyword, demographic_dim, demographic_value)
);
CREATE INDEX IF NOT EXISTS idx_demographic_dim ON gold.demographic_insights (demographic_dim);

-- ── Family demand vs supply (search_trends × monthly review counts) ──────────
CREATE TABLE IF NOT EXISTS gold.family_demand_supply (
    focus_keyword        TEXT,
    month                DATE,
    search_interest_avg  REAL,    -- avg of search_trends.interest for the family that month
    reviews_count        INTEGER,
    avg_sentiment        REAL,
    revision_date        DATE,
    gold_run_id          TEXT,
    PRIMARY KEY (focus_keyword, month)
);

-- ── Pipeline run log ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
    run_id             TEXT PRIMARY KEY,
    dag_name           TEXT,
    status             TEXT,
    rows_written       INTEGER,
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    duration_seconds   REAL
);
