# Team Setup — Run the App Without the Pipeline

This guide is for **teammates who want to develop the Streamlit app locally** without running the heavy Airflow DAGs (scraping, GPU embeddings, BERTopic). You'll restore a pre-baked snapshot of the PostgreSQL `gold` schema and connect Streamlit to it.

**Estimated time: 10 minutes** (most of which is the dump download).

---

## What the data owner delivers to you

The person who runs the pipeline (currently @Ch0comilo) will share **two things** in the team Slack / Drive:

1. A Google Drive link to a file named `marketpulse_gold.dump` (the Postgres snapshot, ~50–200 MB).
2. The file's **Drive ID** — the long alphanumeric string in the URL (between `/d/` and `/view`). You'll need this only if you choose the one-command download path.

> When the data is refreshed (e.g. after a new pipeline run), you'll get a notification and can re-run the restore steps below to update your local DB.

---

## Prerequisites

You need:

- **Docker Desktop** running.
- This repo cloned: `git clone https://github.com/AndresAvella09/MarketPulseBeauty.git`
- A `.env` file at the repo root. Copy `.env.example` if it exists, or ask the data owner for one. **Don't commit it.**

You do **not** need:

- A GPU.
- Python on your host machine (Docker handles it).
- The scraper, embedding models, or any Airflow service.

---

## Step 1 — Get the dump file

Pick one of the two paths.

### Path A — Manual download (simplest)

1. Open the Drive link the data owner sent.
2. Click **Download** in the top-right of the Drive preview.
3. Move the downloaded `marketpulse_gold.dump` into the **root of this repo** (same folder as `docker-compose.yml`).

### Path B — One-command download (recommended if you'll re-fetch often)

Install `gdown` (one-time):

```powershell
pip install gdown
```

Then download the file directly into the repo root, replacing `<FILE_ID>` with the Drive ID the data owner sent:

```powershell
gdown <FILE_ID> -O marketpulse_gold.dump
```

Example (placeholder ID):

```powershell
gdown 1aBcDeFgHiJkLmNoPqRsTuVwXyZ -O marketpulse_gold.dump
```

---

## Step 2 — Start only the services you need

Skip Airflow entirely — those are the heavy containers. You only need Postgres for the data and Streamlit for the app:

```powershell
docker compose up -d postgres streamlit
```

Wait ~15 seconds for Postgres to report healthy:

```powershell
docker compose ps
```

You should see `marketpulsebeauty-postgres-1` with status `(healthy)`.

---

## Step 3 — Apply the schema migration (first time only)

The `gold` tables are defined in `docker/init-postgres.sql`, which auto-runs on the very first Postgres startup. If your `postgres-data` volume already existed before today, also apply the migration so your column set matches the dump:

```powershell
docker cp docker/migrations/001_add_gold_columns_and_tables.sql marketpulsebeauty-postgres-1:/tmp/migration.sql
docker compose exec -T postgres psql -U postgres -f /tmp/migration.sql
```

Safe to run more than once — every statement uses `IF NOT EXISTS`.

---

## Step 4 — Restore the snapshot

Copy the dump into the container and restore it. `--clean --if-exists` makes it safe to run repeatedly (it drops the old data first).

```powershell
docker cp marketpulse_gold.dump marketpulsebeauty-postgres-1:/tmp/marketpulse_gold.dump
docker compose exec -T postgres pg_restore -U postgres -d marketpulse --schema=gold --clean --if-exists /tmp/marketpulse_gold.dump
```

You may see a few `NOTICE:` lines about missing objects on the first run — that's expected and harmless.

---

## Step 5 — Verify the data is there

```powershell
docker compose exec -T postgres psql -U postgres -d marketpulse -c "SELECT COUNT(*) AS products FROM gold.products; SELECT COUNT(*) AS reviews FROM gold.reviews;"
```

Expected: a few hundred products, ~190k reviews. If you see `0`, the restore didn't take — check Step 4 for errors.

---

## Step 6 — Open the app

```
http://localhost:8501
```

That's it. The Streamlit container has the repo mounted as a volume at `/app`, so any change you make to files under `src/dashboard/` reloads in the browser.

---

## Refreshing the data later

When the data owner pushes a new dump:

1. Re-download the file (Path A or B).
2. Re-run only Step 4. Skip Steps 1–3 — Postgres is already running and the schema is already correct.

---

## Stopping / starting

- **Stop everything**: `docker compose down`
- **Start again**: `docker compose up -d postgres streamlit`
- **Wipe local DB and start fresh** (rare, only if something is broken):
  ```powershell
  docker compose down -v   # ⚠️ this deletes the volume
  docker compose up -d postgres streamlit
  ```
  Then redo Steps 3 and 4.

---

## Troubleshooting

| Symptom | What to try |
|---|---|
| `Bind for 0.0.0.0:8501 failed: port is already allocated` | Another process holds the port. Run `docker compose down`, then `docker compose up -d postgres streamlit`. |
| `pg_restore: error: connection to server failed` | Postgres isn't healthy yet. Wait 15 s and re-run Step 4. |
| `relation "gold.products" does not exist` | Step 3 was skipped or failed. Re-run Step 3, then Step 4. |
| Streamlit shows `0 rows` everywhere | Restore didn't load. Re-run Step 5 to verify counts; if still 0, redo Step 4 from a fresh download. |
| `gdown` says "Permission denied" or "Cannot retrieve" | The Drive file isn't shared with "Anyone with the link." Ask the data owner to update sharing. |

---

## What you can and can't do without the pipeline

| You can | You can't |
|---|---|
| Read all `gold.*` tables | Re-scrape new reviews |
| Build / modify Streamlit pages | Re-run BERTopic / regenerate topic clusters |
| Add new SQL queries / aggregations | Recompute embeddings or UMAP coordinates |
| Test new charts on existing data | Get data fresher than the latest dump |

If you need fresher data or to re-process, the data owner has to run the Airflow DAGs and produce a new dump.

---

## For the data owner — how to produce a new dump

Run after a successful `dag_03_gold_build`:

```powershell
docker compose exec -T postgres pg_dump -U postgres -d marketpulse --schema=gold --format=custom --file=/tmp/marketpulse_gold.dump
docker cp marketpulsebeauty-postgres-1:/tmp/marketpulse_gold.dump ./marketpulse_gold.dump
```

Then upload `marketpulse_gold.dump` to the team's Google Drive folder, **replacing the existing file** (so the Drive ID stays the same and teammates' `gdown` commands keep working). Notify the team in Slack with the snapshot date.

The file is git-ignored (see `*.dump` in `.gitignore`) — never commit it.
