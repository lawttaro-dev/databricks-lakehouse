# Lessons learned

What surprised us while building, newest first. Each entry: what happened, why, and what we changed. Decisions themselves live in [03-decisions.md](03-decisions.md).

## 2026-11-16 — Nine red scheduled runs

- **Owning a catalog does not let you read its tables.** qa/prod tables are owned by the service principal that created them; the catalog owner could manage grants but got `INSUFFICIENT_PERMISSIONS` on `SELECT`. `infra/grants.sql` now gives `account users` read-only access (`USE`, `SELECT`, `READ VOLUME`) on qa and prod.
- **The block on job runs lifted by itself** on 11-16 (a manual run of the qa job succeeded at 15:29 UTC). No banner or message in the workspace explained it.
- **Every prod extraction failed from 11-07 to 11-16, and the prod backfill never started.** After ~40 minutes the LoC server dropped the connection (`RemoteDisconnected`) and stayed down for minutes. That error was not handled per partition, so it crashed the run; the backfill, next in line, then failed on its first request. The local dev backfill died the same way, at the same point (right after MX 2023).
- **Brazil was a suspect, but it is not deterministic:** BR 2023 (2,056 records) and BR 2024 answered fine on 11-16, also from position 1,001.
- **More intermittent SRU diagnostics:** `Query feature unsupported` and `Permanent system error`, on queries that work on other days.
- **Red should mean "act".** Expected source flakiness turned every run red, which hid the one real signal: Databricks refused job runs from 11-13 (`Triggering new runs ... disabled temporarily`) although usage was ~1 DBU/day.
- **Changes:** retry the new diagnostics (also on count queries); stop cleanly when the server is down and leave partitions pending; exit 0 for source flakiness and report it in the GitHub step summary; run the backfill before the window in prod.

## 2026-11-07 — CI/CD setup

- **The CLI and the Python SDK find the workspace differently.** `databricks bundle ...` takes the host from `databricks.yml`; `WorkspaceClient()` needs `DATABRICKS_HOST` (or a profile in `~/.databrickscfg`). On a laptop the profile hides the difference; in CI the first extractor run failed with `cannot configure default credentials` until the workflow set `DATABRICKS_HOST`.
- **Not every action publishes a moving major tag.** `astral-sh/setup-uv@v10` does not exist (only exact tags like `v10.3.3`), so the first CI run failed in 5 s. Check tags with `gh api repos/<owner>/<repo>/git/ref/tags/<tag>` before pinning.
- **Squash merges rewrite history:** a commit that was only local on `main` and also travelled in the PR made local `main` diverge after the merge. Never commit on `main` now that it is protected; if it happens, check the change is in `origin/main` and `git reset --hard origin/main`.
- **Free GitHub accounts:** branch protection and deployment approvals only for public repositories (ADR-013).
- **In qa the job has no `[dev <user>]` prefix** (`mode: production`), and it is created by and runs as the deploying service principal.

## 2026-11-07 — SRU flaky diagnostic can repeat

- **What:** the backfill stopped after 11 requests: `First record position out of range` came back on four consecutive retries for AR 2026. Minutes later the same partition worked in one page.
- **Why:** server-side instability at lx2.loc.gov, not a client bug.
- **Change:** retries wait 10 s, 20 s, ... (6 attempts), and a failing partition no longer stops the run: it is reported and retried next time (exit code 1).

## 2026-11-07 — Bronze and silver on Databricks

- **Auto Loader split one run into two micro-batches** (439 + 262 rows). Each is an atomic Delta commit tracked by the checkpoint, so a crash in between neither duplicates nor loses files.
- **Repeated records inside one SRU page:** BR 2026 reported 371 records and returned 371, but 3 were repeats inside `page-000201`, so 3 real records never arrived. Bronze keeps the repeats; silver deduplicates. Detection and re-harvest are still open (doc 02).
- **The same work matches several countries** (10 of 688): kept once in silver with `harvest_countries = [AR, CL]`.

## 2026-11-06 — Spark 4 specifics

- **ANSI mode is on by default.** `array[0]` on an empty array and `cast("19uu" as int)` raise errors instead of returning null (pandas or Spark 3 habits). Use `F.get(array, 0)` and check with `rlike` before casting.
- **Spark 4 has a built-in XML source** (formerly the `spark-xml` package), also supported by Auto Loader. It replaced the planned `pymarc` UDF (ADR-011).
- **Schema inference breaks MARCXML:** tag `"001"` becomes the number `1`, and a blank indicator `" "` fails to parse as a number, which nulls the whole `datafield` array. Always pass an explicit schema.
- **`rowTag="record"` matches only the unprefixed MARC `<record>`**, not the SRU envelope's `<zs:record>`.
- **Hidden `_metadata` column:** file sources expose `file_path` and friends, but only when selected explicitly (`select("*", "_metadata")`).
- **Auto Loader (`cloudFiles`) exists only on Databricks.** Locally, the plain file stream source with the same options stands in for it; keeping transformations as pure functions makes both paths share code.

## 2026-11-04 — Extraction

- **SRU pages can be short:** 40 records with page size 100 came back as 17 + 15 + 3 + 5. Advance by records received, never by the size requested.
- **Pages differ between runs** (AR 2026: pages at 1, 18, 33, 36 locally and 1, 27, 36, 39 in dev), so deduplication must be by record id (MARC `001`), never by file.

## 2026-11-04 — Platform and tooling

- **Free Edition has no outbound internet**, which pushed extraction outside Databricks (ADR-005).
- **A service principal needs the `workspace-access` entitlement** before OAuth M2M works on Free Edition.
- **`~/.databrickscfg` overrides environment variables** in some CLI flows; tests of M2M auth used `DATABRICKS_CONFIG_FILE=/dev/null`.
- **Bundles warn about `/Workspace/Shared`** as `root_path`: writable by every user. qa/prod use the deploying identity's home instead.
- **Ubuntu's `gh` 2.4 is too old** (no `--insecure-storage`); install from GitHub's apt repository.
- **GUI apps keep the `PATH` of the login session:** tools installed later (uv in `~/.local/bin`) are invisible to an already open VS Code until you log out and back in.
