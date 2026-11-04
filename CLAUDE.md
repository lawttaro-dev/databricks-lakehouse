# lakehouse

Databricks lakehouse for a world bibliographic catalog: what was written about each country, per year and topic. First source: Library of Congress catalog via SRU ([docs/02-data-source-loc-catalog.md](docs/02-data-source-loc-catalog.md)).

The project is also how the owner learns Spark and Databricks. When touching Databricks, explain what we're doing and what alternatives exist, comparing to pandas / Airflow / Dagster.

Architecture overview and diagrams: [docs/04-architecture.md](docs/04-architecture.md). Keep its status table and diagrams in sync when components land.

## Platform

- Databricks **Free Edition**: one workspace, serverless only, non-commercial. Limits shape the design ([docs/03-decisions.md](docs/03-decisions.md)).
- Environments `dev`, `qa`, `prod` = Unity Catalog **catalogs** in the same workspace, and Asset Bundle targets in `databricks.yml`.
- Medallion layout per catalog: schemas `bronze`, `silver`, `gold`; landed files in Volumes.
- Serverless environment version 6 (Python 3.12). Local Spark: `pyspark` 4.0 + `delta-spark` 4.0.

## Commands

```bash
uv sync                          # create/update .venv from uv.lock
uv run pytest                    # tests (local Spark + Delta; first run downloads Delta jars)
uv run ruff check . && uv run ruff format .
databricks bundle validate -t dev
databricks bundle deploy -t dev  # ask before running: writes to the workspace
```

## Conventions

- Code, comments, identifiers, commits and docs in English.
- Commits: Conventional Commits (`feat(scope): ...`, `fix`, `docs`, `test`, `refactor`, `chore`, `ci`).
- Transformations are pure `DataFrame -> DataFrame` functions, unit-tested locally. Table and Volume I/O stays at the edges, and the catalog is always a parameter, never hard-coded.
- Python dependencies only through `uv add` (`--dev` for local-only ones like pyspark, which the Databricks runtime already provides). Never `pip install` into the system Python.
- Record architecture decisions as new ADRs in `docs/03-decisions.md`.
- `explore/` holds throwaway exploration scripts (stdlib only) and is excluded from ruff.

## Safety

- Free to run: tests, linters, `bundle validate`, read-only CLI/SQL queries.
- Ask first: `bundle deploy`/`run`, anything that writes to the workspace or Unity Catalog, `git push`, creating or changing GitHub settings or secrets.
- Never commit secrets. CI authenticates as a service principal (OAuth M2M) through GitHub secrets.
