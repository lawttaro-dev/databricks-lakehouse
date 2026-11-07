# Operations

How to run, deploy and inspect the lakehouse day to day. Setup of a new machine: [01-local-setup.md](01-local-setup.md). How the pieces fit: [04-architecture.md](04-architecture.md).

## Extract from the Library of Congress

The extractor runs outside Databricks (Free Edition has no outbound internet, ADR-005) and writes raw pages to the landing Volume.

```bash
# Rolling window: last N publication years (default from sources/loc_sru.yml)
uv run lakehouse-extract --mode window --catalog dev

# Full-history backfill, in the background, resumable
nohup uv run lakehouse-extract --mode backfill --catalog dev \
  --from-year 1990 --to-year 2026 --max-requests 1500 \
  > backfill-$(date +%F).log 2>&1 &
tail -f backfill-$(date +%F).log
```

- **Resume:** run the same command again. Partitions with `_SUCCESS` are skipped. Keep `--from-year` and `--to-year` fixed: they name the batch (`backfill-1990-2026`), and a different name starts over.
- **Budget:** `--max-requests` caps one run (3 s between requests, so 1,500 requests ≈ 75 min). When it is spent the run exits 0 and lists pending partitions.
- **Exit codes:** `0` done or budget spent; `1` some partitions failed and will be retried on the next run; `2` the run could not continue (endpoint down, unexpected response).
- **Try without Databricks:** `--local-dir /tmp/landing` instead of `--catalog`.

## Deploy and run the pipeline

```bash
uv run pytest                          # always first
databricks bundle validate -t dev
databricks bundle deploy -t dev        # builds the wheel, uploads it, creates/updates job loc-sru
databricks bundle run -t dev loc_sru   # bronze then silver, only new data
```

- In `dev` the job is named `[dev <user>] loc-sru` and its schedule (none yet) would be paused.
- `qa` and `prod` are deployed by CI (planned, Phase 3).
- Runs are visible in the workspace under **Jobs & Pipelines**: each task's output, duration and errors.

## Unity Catalog objects

Catalogs, schemas and Volumes are not managed by the bundle (ADR-010):

```bash
uv run python infra/apply_unity_catalog.py --env dev --env qa --env prod   # idempotent
uv run python infra/apply_unity_catalog.py --env dev --dry-run             # print the SQL only
```

Run it after any change to `infra/unity_catalog.sql`.

## Table documentation

Descriptions of every table and column are defined in code (`TableDoc` in `src/lakehouse/*/transform.py`):

- Jobs apply them as Unity Catalog comments after each run (only those that changed), so they show up in Catalog Explorer and the SQL editor.
- `docs/07-data-dictionary.md` is generated from them: `uv run python -m lakehouse.catalog > docs/07-data-dictionary.md`. A test fails if it is out of date.

## Inspect the data

| Where | Use it for |
|---|---|
| **SQL Editor** (warehouse *Serverless Starter Warehouse*) | Ad hoc queries |
| **Catalog** → `dev` → schema → table | Sample data, schema, Delta history, lineage |
| **Notebook** (serverless) | Exploration with PySpark, `display(df)` |
| `databricks fs ls dbfs:/Volumes/dev/landing/loc_sru/...` | Landed files (one level per call, no recursive flag) |
| `notebooks/bronze_dev.py` | Local development against the test fixtures |

```sql
-- Bronze: rows and distinct records per partition
SELECT country, year, batch, count(*) AS rows, count(DISTINCT record_id) AS ids
FROM dev.bronze.loc_records GROUP BY ALL ORDER BY country, year;

-- Silver: latest records
SELECT record_id, title, author, pub_year, pub_country, lc_class, harvest_countries
FROM dev.silver.loc_records ORDER BY record_modified DESC LIMIT 50;

-- Works about more than one country
SELECT title, harvest_countries FROM dev.silver.loc_records WHERE size(harvest_countries) > 1;

-- Most frequent LCSH topics
SELECT s.heading, count(*) AS works
FROM dev.silver.loc_records LATERAL VIEW explode(subjects_topical) AS s
WHERE s.vocabulary = 'lcsh'
GROUP BY s.heading ORDER BY works DESC LIMIT 20;

-- Delta history and time travel
DESCRIBE HISTORY dev.silver.loc_records;
SELECT count(*) FROM dev.bronze.loc_records VERSION AS OF 1;
```

## Troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| Extractor: `First record position out of range` | LoC server quirk, intermittent, can repeat several times | Retried automatically with backoff; if a partition still fails, rerun the same command |
| Extractor: `RemoteDisconnected` | LoC network flakiness | Retried automatically (10 s, 20 s, 40 s) |
| Extractor: HTML instead of XML | Cloudflare challenge on loc.gov | Do not bypass; wait and retry later |
| VS Code cannot find `uv` | Editor started before `~/.local/bin` was on the session `PATH` | Log out of Ubuntu and back in |
| VS Code offers to `pip install` packages into `.venv` | uv venvs have no pip | Decline; add packages with `uv add --dev` |
| Spark: `INVALID_ARRAY_INDEX` | ANSI mode (Spark 4 default): `array[0]` on an empty array | Use `F.get(array, 0)` |
