# Architecture decisions

Short log of decisions taken, newest last. Each entry: context, decision, consequences.

## ADR-001 — Databricks Free Edition, one catalog per environment

- **Context:** zero budget. Free Edition allows one workspace, serverless compute only, 5 concurrent job tasks, one active declarative pipeline per type, restricted outbound internet, non-commercial use.
- **Decision:** use Free Edition. Environments `dev`, `qa`, `prod` are Unity Catalog **catalogs** in the single workspace.
- **Consequences:** the design must stay portable to a paid account, where only Asset Bundle targets change (e.g. one workspace per environment).

## ADR-002 — Local development with open-source Spark

- **Context:** Databricks cannot run locally.
- **Decision:** local unit tests use `pyspark` + `delta-spark`. Unity Catalog OSS is deferred.
- **Consequences:** transformations are pure `DataFrame -> DataFrame` functions, and table I/O stays at the edges, parameterized by catalog.

## ADR-003 — CI/CD authenticates with OAuth M2M (service principal)

- **Context:** verified on 2026-11-04 that Free Edition supports service principals and OAuth M2M secrets. A service principal needs the `workspace-access` entitlement.
- **Decision:** GitHub Actions authenticates as a service principal. No personal access tokens.

## ADR-004 — Conventional Commits

- **Decision:** commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `ci`, with a scope).
- **Consequences:** enables automatic changelog and versioning for prod releases.

## ADR-005 — First source: Library of Congress catalog (SRU)

- See [02-data-source-loc-catalog.md](02-data-source-loc-catalog.md).
- **Decision:** the extractor runs outside Databricks (GitHub Actions), lands files in a Volume, and Auto Loader ingests them into bronze.

## ADR-006 — Land raw MARCXML, parse in Spark

- **Context:** landing-zone rule: store what the source returns, untransformed.
- **Decision:** the extractor writes the **raw SRU MARCXML pages** to the Volume. Parsing happens in the bronze Spark job (how: ADR-011, which replaces the original `pymarc` plan).
- **Consequences:** parsing bugs are fixed by reprocessing landed files, without calling LoC again.

## ADR-007 — Pipelines in PySpark on Lakeflow Jobs

- **Context:** Free Edition supports neither Scala nor R and allows only one active declarative pipeline per type, so dev/qa/prod pipelines could not run at the same time.
- **Decision:** transformations are written in PySpark (with `spark.sql` where SQL reads better) and orchestrated with Lakeflow Jobs defined in the Asset Bundle.
- **Consequences:** same language as the extractor and the future NLP stages, unit-testable with pytest. Declarative pipelines may be tried later for comparison.

## ADR-008 — Full-history backfill plus variable rolling window

- **Context:** SRU has no "changed since" query, and LoC catalogs recent years late.
- **Decision:** one extractor with two modes:
  - `backfill`: the whole history, split into `(country, year)` partitions, resumable over several days with a request budget per run.
  - `window`: re-harvests the last N publication years daily. N is a parameter (`--window-years`), with a default in the source config.
- **Consequences:** a partition counts as done only when its success marker exists in the Volume, so any run can be stopped and resumed. Older corrections are picked up by re-running `backfill` periodically.

## ADR-009 — Connectors: protocol client + source config

- **Decision:** one reusable client per protocol (SRU first), and one YAML config per source (endpoint, query template, countries and terms, window default, throttle). Parsers per format (MARCXML) live in the bronze job (see ADR-006).
- **Consequences:** a new library that speaks SRU is a new YAML, not new code.

## ADR-010 — Unity Catalog objects managed outside the bundle

- **Context:** Asset Bundles can manage schemas and volumes, but a resource removed from the bundle is deleted on the next deploy, and deleting a schema drops its tables. In `mode: development` the bundle also prefixes schema names per user (`dev_<user>_bronze`).
- **Decision:** catalogs, schemas and volumes are created by an idempotent SQL template (`infra/unity_catalog.sql`) applied with `infra/apply_unity_catalog.py`. The bundle only manages jobs and code.
- **Consequences:** long-lived data containers cannot be dropped by a bundle change. Layout per catalog: schemas `landing` (Volumes with raw files), `bronze`, `silver`, `gold`. One Volume per source (`landing.loc_sru`).

## ADR-011 — Parse MARCXML with Spark's built-in XML source

- **Context:** ADR-006 planned a Python UDF with `pymarc` producing MARC-in-JSON. Spark 4 and Databricks ship a native XML data source, also supported by Auto Loader (`cloudFiles.format = "xml"`).
- **Decision:** bronze reads pages with `format("xml")`, `rowTag = "record"` and an explicit schema (`leader`, `controlfield[]`, `datafield[]` with `subfield[]`). Bronze stores these typed arrays, not a JSON string. Auto Loader checkpoints live in the Volume `<catalog>.bronze.checkpoints`.
- **Alternatives:** `pymarc` in a UDF: familiar Python and full MARC support, but every row crosses the JVM/Python boundary, it adds a dependency to the serverless environment, and silver would have to parse JSON again.
- **Consequences:** no Python per row and no extra dependency. The schema must be explicit, because inference turns tags into numbers and breaks on blank indicators. Silver works with array functions (`filter`, `transform`, `explode`). `pymarc` stays an option for edge cases (for example MARC-8 encoded records).

## ADR-012 — Silver: latest version per record, MERGE with country union

- **Context:** bronze holds every record as landed: the same `001` appears again after re-harvests, inside overlapping or repeated pages, and once per country query that matched it.
- **Decision:**
  - The silver job streams only the rows appended to bronze since its last run (Delta table as a streaming source, `availableNow`) and applies each micro-batch with `foreachBatch` + Delta `MERGE` on `record_id`.
  - Within a batch, keep the version with the newest `005` and collect every country that matched (`harvest_countries`).
  - In the MERGE: a newer `005` replaces every column; an older or equal one only adds new countries; anything else is a no-op, so reprocessing is safe.
  - Three notions of "country" are kept apart: `harvest_countries` (subject query), `pub_country` (`008/15-17`) and `geo_areas` (`043`). Gold decides which to use (doc 02, open decision 1).
  - Subjects (`650`, `651`) are arrays of structs `{heading, topics, places, periods, forms, vocabulary}`, keeping LCSH apart from other vocabularies such as BISAC.
- **Alternatives:** rebuilding silver from the whole of bronze on every run: simpler and always consistent, but its cost grows with history (~500k records expected). Can be used as a periodic repair.
- **Consequences:** silver is updated incrementally and idempotently. A record deleted at the source stays in silver (SRU cannot report deletions).

## ADR-013 — CI/CD with GitHub Actions, one service principal per environment

- **Context:** qa and prod must only change through reviewed, tested code, deployed by a non-human identity. Free GitHub accounts offer branch protection and deployment approvals only on public repositories.
- **Decision:**
  - The repository is **public**. `main` is protected: changes arrive through pull requests with green checks (`test`, `validate`).
  - Pull request: `ruff`, `pytest` (local Spark + Delta on the runner) and `bundle validate -t qa`.
  - Merge to `main`: deploy to **qa** automatically (deploy only; running the job waits for scheduled extraction).
  - Tag `vX.Y.Z` on a commit of `main`: tests again, manual approval in the GitHub `prod` environment, deploy to **prod**.
  - **One service principal per environment** (`sp-lakehouse-qa`, `sp-lakehouse-prod`), each with privileges on its own catalog only (`infra/grants.sql`), authenticated with OAuth M2M secrets stored as GitHub environment secrets.
- **Alternatives:** a private repository (no enforced protection on the free plan); one shared principal (a qa mistake could write prod); a manual "deploy to prod" button instead of tags (no release history).
- **Consequences:** jobs in qa/prod run as their principal and the principal owns the tables it creates. Releases are git tags, so a changelog can be generated from Conventional Commits between tags. Development moves to branches and pull requests.

