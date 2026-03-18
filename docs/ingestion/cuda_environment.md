# Sephora Review Pipeline

Medallion-architecture ETL pipeline (Bronze → Silver → Gold) for scraping and processing Sephora product reviews, with NLP enrichment and semantic embeddings.

---

## Requirements

| Requirement  | Minimum                        |
| ------------ | ------------------------------ |
| Python       | 3.11                           |
| NVIDIA GPU   | 8 GB VRAM (RTX 3060 or better) |
| CUDA Toolkit | 12.x                           |
| RAM          | 16 GB recommended              |
| Disk         | ~5 GB (model cache + data)     |

> **CPU-only fallback:** The pipeline will run on CPU if no GPU is detected, but the Gold step (bge-m3 embeddings on 200k+ reviews) will take several hours instead of minutes.

---

## Setup

### 2. Create the conda environment

```bash
conda env create -f environment.yml
conda activate embed_env
```

### 3. Install PyTorch with CUDA support

> `environment.yml` cannot pin the CUDA wheel index, so this must be run manually once.

```bash
# CUDA 12.x (check your version with: nvidia-smi)
pip install "torch>=2.6.0" --index-url https://download.pytorch.org/whl/cu121 --upgrade

# Verify GPU is visible
python -c "import torch; print(torch.cuda.get_device_name(0))"
```

### 4. Download the spaCy model

```bash
python -m spacy download en_core_web_sm
```

### 5. (Optional) Set your Hugging Face token

Avoids rate limits when downloading `BAAI/bge-m3` (~2.3 GB on first run).

```bash
conda env config vars set HF_TOKEN=hf_your_token_here -n embed_env
conda activate embed_env
```

---

## Running the pipeline

```bash
# Full pipeline (scrape → bronze → silver → gold)
python pipeline.py

# Individual stages
python pipeline.py --silver-only
python pipeline.py --gold-only

# Process a specific ingestion date
python silver_transform.py --bronze-date 2026-03-18

# Custom batch size (lower if you get CUDA out-of-memory errors)
python gold_transform.py --batch-size 16
```

### If you get a CUDA out-of-memory error

```bash
# Set this before running — helps PyTorch manage fragmented VRAM
set PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True   # Windows
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True  # Linux/Mac

# Then lower the batch size
python pipeline.py --gold-only --batch-size 16
```

---

## Output layout

```
bronze/
  products/ingestion_date=YYYY-MM-DD/*.parquet
  reviews/ingestion_date=YYYY-MM-DD/category_id=<cat>/*.parquet
  _quality_reports/report_<run_id>.json

silver/
  products/revision_date=YYYY-MM-DD/*.parquet
  reviews/revision_date=YYYY-MM-DD/category_id=<cat>/*.parquet

gold/
  products/revision_date=YYYY-MM-DD/*.parquet
  reviews/revision_date=YYYY-MM-DD/*.parquet
```

---

## Key dependencies

| Package                      | Purpose                                 |
| ---------------------------- | --------------------------------------- |
| `torch`(CUDA build)        | GPU compute for embeddings              |
| `sentence-transformers`    | `BAAI/bge-m3`embedding model          |
| `spacy`+`en_core_web_sm` | Silver NLP: tokenisation, lemmatisation |
| `pyarrow`                  | Parquet read/write across all layers    |
| `numpy`                    | Vector math for embeddings              |

---

## Troubleshooting

**`AssertionError: Torch not compiled with CUDA enabled`**
You have the CPU-only torch build. Re-run step 3 with `--force-reinstall`.

**`OSError: Can't find model 'en_core_web_sm'`**
Run `python -m spacy download en_core_web_sm` inside the active conda env.

**`CUDA out of memory`**
Lower `--batch-size` (try 16 or 8). Also set `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True`.

**`[GPU] No CUDA-capable GPU found`** (Silver stage)
The spaCy GPU detection uses cupy. If cupy isn't installed or conflicts with your CUDA version, Silver runs on CPU — this is fine, it's fast enough. Only Gold needs the GPU.
