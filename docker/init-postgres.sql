-- Create the marketpulse database and gold schema
CREATE DATABASE marketpulse;

\connect marketpulse

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.products (
    product_id           TEXT PRIMARY KEY,
    brand                TEXT,
    product_category     TEXT,
    product_name         TEXT,
    category_id          TEXT,
    product_page_url     TEXT,
    avg_rating           REAL,
    total_review_count   INTEGER,
    recommended_count    INTEGER,
    total_photo_count    INTEGER,
    rating_dist_1        INTEGER,
    rating_dist_2        INTEGER,
    rating_dist_3        INTEGER,
    rating_dist_4        INTEGER,
    rating_dist_5        INTEGER,
    product_name_clean   TEXT,
    product_name_tokens  TEXT,
    product_name_lemmas  TEXT,
    product_name_embedding REAL[],
    rating_entropy       REAL,
    polarization_score   REAL,
    embedding_norm_name  REAL,
    revision_date        DATE,
    ingestion_ts         TIMESTAMPTZ,
    source_file          TEXT,
    run_id               TEXT,
    silver_run_id        TEXT,
    gold_run_id          TEXT
);

CREATE TABLE IF NOT EXISTS gold.reviews (
    review_id            TEXT PRIMARY KEY,
    product_id           TEXT,
    rating               SMALLINT,
    title                TEXT,
    review_text          TEXT,
    submission_time      TIMESTAMPTZ,
    last_mod_time        TIMESTAMPTZ,
    is_recommended       BOOLEAN,
    helpful_count        INTEGER,
    not_helpful_count    INTEGER,
    is_featured          BOOLEAN,
    is_incentivized      TEXT,
    is_staff_review      TEXT,
    user_location        TEXT,
    skin_tone            TEXT,
    skin_type            TEXT,
    eye_color            TEXT,
    hair_color           TEXT,
    hair_type            TEXT,
    hair_concerns        TEXT,
    skin_concerns        TEXT,
    age_range            TEXT,
    review_photo_count   SMALLINT,
    review_text_clean    TEXT,
    review_text_tokens   TEXT,
    review_text_lemmas   TEXT,
    review_text_wordcount INTEGER,
    title_clean          TEXT,
    title_tokens         TEXT,
    title_lemmas         TEXT,
    review_text_embedding REAL[],
    title_embedding      REAL[],
    text_quality_score   REAL,
    helpful_ratio        REAL,
    review_age_days      INTEGER,
    is_short_review      BOOLEAN,
    embedding_norm_review REAL,
    embedding_norm_title REAL,
    revision_date        DATE,
    ingestion_ts         TIMESTAMPTZ,
    source_file          TEXT,
    run_id               TEXT,
    silver_run_id        TEXT,
    gold_run_id          TEXT
);

CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
    run_id             TEXT PRIMARY KEY,
    dag_name           TEXT,
    status             TEXT,
    rows_written       INTEGER,
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    duration_seconds   REAL
);

