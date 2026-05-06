# MarketPulse Beauty - Setup Guide

This guide covers how to set up the full MarketPulse Beauty data pipeline, both locally and with the Docker/Airflow orchestration environment.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Repository Structure](#repository-structure)
3. [Environment Variables](#environment-variables)
4. [Option A: Docker Setup (Recommended)](#option-a-docker-setup-recommended)
5. [Option B: Local Setup (Without Docker)](#option-b-local-setup-without-docker)
6. [Airflow DAGs](#airflow-dags)
7. [MinIO Object Storage](#minio-object-storage)
8. [PostgreSQL Gold Layer](#postgresql-gold-layer)
9. [Running the Pipeline](#running-the-pipeline)
10. [Streamlit Dashboard](#streamlit-dashboard)
11. [GPU / CUDA Configuration](#gpu--cuda-configuration)
12. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Hardware

- **GPU (recommended):** NVIDIA GPU with CUDA 12.1+ support and at least 8 GB VRAM for embeddings (e.g., RTX 3070, RTX 4060, etc.)
- **RAM:** Minimum 16 GB recommended
- **Disk:** ~20 GB free for Docker images + data

### Software

- **Docker Desktop** (v4.x+) with Docker Compose v2
- **NVIDIA Container Toolkit** (required for GPU passthrough to Docker)
  - Windows: Comes with Docker Desktop when WSL2 backend + NVIDIA drivers are installed
  - Linux: Install via `nvidia-container-toolkit` package
- **NVIDIA Driver** 525+ (for CUDA 12.1 support)
- **Git**

To verify GPU availability in Docker:

```bash
docker run --rm --gpus all nvidia/cuda:12.1.0-base-ubuntu22.04 nvidia-smi
```

If this prints your GPU info, you're good.

---

## Repository Structure

```
MarketPulseBeauty/
├── docker-compose.yml          # Multi-container orchestration
├── docker/
│   ├── Dockerfile.airflow      # CUDA 12.1 + Airflow + NLP stack
│   ├── Dockerfile.jupyter      # CUDA 12.1 + JupyterLab
│   └── Dockerfile.streamlit    # Lightweight dashboard image
├── airflow/
│   ├── dags/                   # 5 Airflow DAG files
│   ├── logs/                   # Task execution logs (gitignored)
│   └── plugins/                # Custom Airflow operators
├── config/
│   ├── airflow.env             # Airflow-specific env vars
│   ├── postgres.env            # PostgreSQL credentials
│   ├── minio.env               # MinIO credentials
│   └── trends_keywords.json    # Google Trends keyword config
├── src/                        # Pipeline source code
│   ├── ingestion/
│   │   ├── scraper/            # Scraper + Bronze/Silver/Gold transforms
│   │   └── fetch_google_trends.py
│   └── processing/
│       ├── data_contracts.py   # Quality gate validation
│       ├── gold_writer.py      # PostgreSQL upsert logic
│       └── ...
├── data/                       # Data lake (medallion architecture)
├── .env                        # Credentials and Docker infra vars
├── requirements.txt            # Python dependencies
└── app.py                      # Streamlit dashboard
```

---

## Environment Variables

The `.env` file at the project root contains all secrets and infrastructure config. It is **gitignored** and must be created manually on each machine.

```env
# API credentials
BAZAARVOICE_PASSKEY=your_bazaarvoice_passkey_here
HF_TOKEN=hf_your_huggingface_token_here

# Docker infrastructure
POSTGRES_PASSWORD=your_postgres_password
MINIO_ACCESS_KEY=your_minio_access_key
MINIO_SECRET_KEY=your_minio_secret_key
AIRFLOW_FERNET_KEY=your_fernet_key_here   # generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
JUPYTER_TOKEN=your_jupyter_token
```

> **Important:** Change default passwords before deploying to any shared environment.

To generate a new Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Option A: Docker Setup (Recommended)

This is the production-like setup with all services containerized.

### 1. Build the images

```bash
docker compose build
```

This builds three images:
- **airflow** (CUDA 12.1 + Python 3.11 + Airflow 2.9.1 + PyTorch + spaCy + sentence-transformers)
- **jupyter** (CUDA 12.1 + JupyterLab + ML stack)
- **streamlit** (Python 3.11 slim)

The airflow image is large (~12-15 GB) due to CUDA + PyTorch + NLP models. First build takes 15-30 minutes depending on internet speed.

### 2. Start the services

```bash
docker compose up -d
```

This starts 8 containers:

| Service             | Port  | Purpose                              |
|---------------------|-------|--------------------------------------|
| `postgres`          | 5433  | Airflow metadata + Gold layer DB     |
| `redis`             | -     | Celery message broker                |
| `minio`             | 9000/9001 | S3-compatible object storage     |
| `airflow-webserver` | 8080  | Airflow UI                           |
| `airflow-scheduler` | -     | DAG scheduler                        |
| `airflow-worker`    | -     | Task executor (GPU-enabled)          |
| `jupyterlab`        | 8888  | Notebook server (GPU-enabled)        |
| `streamlit`         | 8501  | Dashboard                            |

### 3. Verify services are running

```bash
docker compose ps
```

All services should show `healthy` or `running`.

### 4. Access the UIs

| Service      | URL                          | Credentials             |
|--------------|------------------------------|-------------------------|
| Airflow      | http://localhost:8080         | admin / admin           |
| MinIO Console| http://localhost:9001         | minioadmin / minioadmin123 |
| JupyterLab   | http://localhost:8888         | Token: `changeme`       |
| Streamlit    | http://localhost:8501         | No auth                 |

### 5. Create MinIO buckets

On first run, create the required buckets via the MinIO console (http://localhost:9001) or CLI:

```bash
docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin123
docker compose exec minio mc mb local/marketpulse-raw
docker compose exec minio mc mb local/marketpulse-bronze
docker compose exec minio mc mb local/marketpulse-silver
docker compose exec minio mc mb local/marketpulse-gold
```

### 6. Create PostgreSQL Gold schema

Connect to PostgreSQL and create the `marketpulse` database and `gold` schema:

```bash
docker compose exec postgres psql -U postgres -c "CREATE DATABASE marketpulse;"
docker compose exec postgres psql -U postgres -d marketpulse -c "CREATE SCHEMA IF NOT EXISTS gold;"
```

Then create the required tables:

```bash
docker compose exec postgres psql -U postgres -d marketpulse <<'SQL'
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

SQL
```

### 7. Stop services

```bash
docker compose down          # Stop containers (data persists in volumes)
docker compose down -v       # Stop and delete volumes (destroys all data)
```

---

## Option B: Local Setup (Without Docker)

For development or machines without Docker.

### 1. Create conda environment

```bash
conda create -n marketpulse python=3.11 -y
conda activate marketpulse
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Install PyTorch with CUDA

```bash
# CUDA 12.1
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu121

# Or CPU-only
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
```

### 4. Install spaCy models

```bash
python -m spacy download en_core_web_sm    # Silver NLP (fast)
python -m spacy download en_core_web_md    # Gold embedding fallback (300-dim)
```

### 5. Install GPU acceleration (optional)

```bash
pip install sentence-transformers    # BAAI/bge-m3 embeddings
pip install cupy-cuda12x             # spaCy GPU via cupy
```

### 6. Run the pipeline locally

```bash
# Full pipeline (scrape -> bronze -> silver -> gold)
python -m src.ingestion.scraper.pipeline

# Just silver + gold (if bronze data exists)
python -m src.ingestion.scraper.pipeline --silver-only

# Just gold (if silver data exists)
python -m src.ingestion.scraper.pipeline --gold-only
```

Data is written to local `data/` directory (no MinIO needed).

---

## Airflow DAGs

Five DAGs are configured in `airflow/dags/`:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `dag_01_ingestion_sephora` | Daily 2:00 AM UTC | Scrape Sephora -> CSV backup -> Bronze Parquet |
| `dag_02_silver_transform`  | Daily 4:00 AM UTC | Bronze -> Silver (NLP enrichment) |
| `dag_03_gold_build`        | Daily 6:00 AM UTC | Silver -> Gold (embeddings + PostgreSQL) |
| `dag_04_google_trends`     | Weekly Monday 1:00 AM UTC | Google Trends -> Bronze |
| `dag_05_data_quality`      | Daily 7:00 AM UTC | Cross-layer quality checks |

DAGs are designed to run sequentially: 01 -> 02 -> 03 -> 05. DAG 04 runs independently on Mondays.

To trigger a DAG manually, go to the Airflow UI (http://localhost:8080) and click the play button.

---

## MinIO Object Storage

MinIO acts as a local S3-compatible store. The pipeline uses these buckets:

| Bucket | Layer | Content |
|--------|-------|---------|
| `marketpulse-raw` | Raw | CSV backups from scraper |
| `marketpulse-bronze` | Bronze | Typed Parquet + quality reports |
| `marketpulse-silver` | Silver | NLP-enriched Parquet |
| `marketpulse-gold` | Gold | Embedded Parquet (also in PostgreSQL) |

Access the MinIO console at http://localhost:9001 to browse data.

---

## PostgreSQL Gold Layer

PostgreSQL serves dual purpose:
1. **Airflow metadata** (database: `airflow`)
2. **Gold analytics tables** (database: `marketpulse`, schema: `gold`)

Connect from your machine:

```bash
psql -h localhost -p 5433 -U postgres -d marketpulse
```

Or from inside Docker:

```bash
docker compose exec postgres psql -U postgres -d marketpulse
```

Key tables in the `gold` schema:
- `gold.products` - Full product dataset with embeddings
- `gold.reviews` - Full review dataset with embeddings
- `gold.pipeline_runs` - Audit log of all DAG executions

---

## Running the Pipeline

### Via Airflow (Docker)

1. Open http://localhost:8080
2. Enable the DAGs you want to run
3. Trigger `dag_01_ingestion_sephora` to start the full flow
4. Monitor progress in the Airflow UI

### Via CLI (Docker)

```bash
# Run full pipeline inside the worker container
docker compose exec airflow-worker python -m src.ingestion.scraper.pipeline
```

### Via CLI (Local)

```bash
python -m src.ingestion.scraper.pipeline                # Full run
python -m src.ingestion.scraper.pipeline --gold-only    # Re-run gold only
python -m src.ingestion.scraper.pipeline --from-csv \
    --products data/raw/backups/2026-04-01/sephora_products.csv \
    --reviews  data/raw/backups/2026-04-01/sephora_reviews.csv
```

---

## Streamlit Dashboard

The dashboard reads from PostgreSQL (Docker mode) or local Parquet files.

### Docker mode

Automatically connects via `PG_CONN` environment variable. Access at http://localhost:8501.

### Local mode

```bash
streamlit run app.py
```

Configure gold data paths in the sidebar.

---

## GPU / CUDA Configuration

### Architecture

The Docker setup uses `nvidia/cuda:12.1.0-cudnn8-runtime-ubuntu22.04` as the base image for both the Airflow worker and JupyterLab containers. GPU access is controlled by Docker Compose's `deploy.resources.reservations.devices` configuration.

### What uses the GPU

| Component | GPU Usage | Model |
|-----------|-----------|-------|
| **Gold embeddings** (sentence-transformers) | CUDA fp16 | BAAI/bge-m3 (384-dim) |
| **Silver NLP** (spaCy + cupy) | CUDA via cupy | en_core_web_sm |
| **Gold fallback** (spaCy) | CPU | en_core_web_md (300-dim) |

### Memory management

- Embeddings run in fp16 on CUDA (halves VRAM usage)
- Reviews are processed in mini-batches of 2,000 for late-chunking
- Encode batch size is capped at 32 on CUDA to prevent OOM
- `torch.cuda.empty_cache()` is called between mini-batches

### Verifying GPU inside containers

```bash
# Check GPU is visible
docker compose exec airflow-worker python -c "import torch; print(torch.cuda.is_available(), torch.cuda.get_device_name(0))"

# Check spaCy GPU
docker compose exec airflow-worker python -c "import spacy; print(spacy.prefer_gpu())"
```

### Running without GPU

The pipeline automatically falls back to CPU if no GPU is detected. Embeddings will use spaCy `en_core_web_md` (300-dim word vectors) instead of sentence-transformers. This is slower but functional.

To run Docker without GPU, remove the `deploy` section from `airflow-worker` and `jupyterlab` in `docker-compose.yml`.

---

## Troubleshooting

### Docker build fails on CUDA image

Make sure NVIDIA Container Toolkit is installed:

```bash
# Linux
sudo apt-get install -y nvidia-container-toolkit
sudo systemctl restart docker

# Windows
# Ensure Docker Desktop uses WSL2 backend and NVIDIA drivers are up to date
```

### Airflow webserver won't start

Check that PostgreSQL is healthy first:

```bash
docker compose logs postgres
docker compose exec postgres pg_isready -U postgres
```

### MinIO "bucket does not exist" errors

Create the buckets (see step 5 in Docker Setup above).

### Out of Memory (OOM) during Gold build

Reduce batch size:

```bash
# In docker-compose.yml, add to airflow-worker environment:
GOLD_BATCH_SIZE: "16"
```

Or set in `.env`:

```env
GOLD_BATCH_SIZE=16
```

### spaCy model not found

Inside the container:

```bash
docker compose exec airflow-worker python -m spacy download en_core_web_sm
```

### Pipeline works locally but not in Docker

Check that `USE_MINIO=true` is set in the container environment (it is by default in `docker-compose.yml`). When running locally without Docker, `USE_MINIO` defaults to `false` and the pipeline writes to local `data/` directories instead.
