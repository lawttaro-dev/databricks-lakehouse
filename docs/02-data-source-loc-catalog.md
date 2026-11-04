# Data source: Library of Congress catalog (SRU)

First source of the **world bibliographic catalog**: for each country (starting with Latin America), what was written about it, each year, on which topics. Later stages add more sources and NLP (topic detection, cross-source deduplication).

Status: **access validated by hand (Nov 2026)**. Exploration scripts live in [`explore/`](../explore/).

## Why this source

| Pro | Con |
|---|---|
| Worldwide coverage, standardized cataloging (MARC21, LC Classification, LC Subject Headings, MARC country/geographic codes) | Reflects what LoC **acquired**, not everything published (e.g. Brazil and Law are over-represented: LoC has an office in Rio and the Law Library buys foreign legislation in bulk) |
| Free, no API key | No "changed since" query: incremental loads need a rolling window (see [Load strategy](#load-strategy)) |
| Subject headings are human-assigned topics: usable as labels to train/validate topic detection | Recent years arrive late (cataloging lag) |

### Sources evaluated and discarded for now

- **loc.gov JSON API** (`https://www.loc.gov/collections/...?fo=json`), including the *Handbook of Latin American Studies* (HLAS) collection: **blocked by a Cloudflare bot challenge** (HTTP 403 "Just a moment...", `cf-mitigated: challenge`) since ~Oct 2026, from home and cloud IPs. No API key or access request exists. Do not try to bypass it.
- HLAS records **cannot** be isolated through SRU: the catalog does not flag which works were reviewed in HLAS (`cql.anywhere="Handbook of Latin American studies"` returns only the 7 Handbook volumes).

### Planned complementary sources

| Source | Adds | Access | Cadence |
|---|---|---|---|
| Open Library dumps | 20M+ editions worldwide, JSON, ISBNs, work/edition links | Monthly dumps on archive.org (~12 GB) | Monthly |
| LoC MDSConnect | ~25M MARC records worldwide, bulk (approx. 1968–2014); weekly updates are paid | Bulk files | Static |
| OpenAlex | Scholarly works with author-institution country and pre-computed topics | Free API + S3 snapshot | Daily/monthly |
| National libraries | What was actually published per country (legal deposit) | SRU / OAI-PMH / Z39.50, to be surveyed | Varies |

Libraries share standards (MARC21, SRU, OAI-PMH, ISBN/LCCN/OCLC), so ingestion should be **one connector per protocol + one parser per format, configured per source**, not one script per library.

## Endpoint

```
http://lx2.loc.gov:210/lcdb?version=1.1&operation=searchRetrieve&query=<CQL>&startRecord=1&maximumRecords=100&recordSchema=marcxml
```

Reference: [LC SRU server configuration](https://loc.gov/z3950/lcserver.html). Not behind Cloudflare.

### Query capabilities (tested)

| Index | Works | Example | Hits |
|---|---|---|---|
| `dc.subject` | yes | `dc.subject="Mexico"` | 144,991 |
| `dc.date` | **equality only** | `dc.date=1950` | 33,653 |
| `dc.creator`, `dc.title`, `cql.anywhere` | yes | `dc.creator="Borges"` | 2,877 |
| `bath.isbn`, `bath.lccn` | yes | | |
| `and` / `or` | yes | `(dc.subject="Argentina" or dc.subject="Argentine") and dc.date=1950` | 175 |
| `dc.publisher`, `dc.language`, `bath.geographicName`, `bath.topicalSubject` | no | | "Unsupported index" |
| date ranges (`dc.date>1950`) | no | | "Unsupported relation" |

Consequences:

- Partition queries by **(subject terms, publication year)**: one query per country per year.
- A country needs its **name and adjective** OR-ed: `Argentina` (76,913) and `Argentine` (7,597) are different terms; the adjective covers literature/culture headings (`Argentine fiction`).
- `dc.subject` matches works **cataloged as being about** the country (fields 650/651), not every work mentioning it. Fiction rarely carries geographic subjects.
- Language and publication country cannot be filtered server-side: extract them in silver.

### Record schemas

- `marcxml`: full record. **Use this one.**
- `dc`: simplified Dublin Core (7 fields), loses most of the useful data.
- `mods`: server errors, not needed (MODS is derived from MARC).

### Server behavior (handle in the client)

| Behavior | Handling |
|---|---|
| Rate limit not documented | Throttle (3 s between requests worked fine for ~300 requests) and cap requests per run |
| Intermittent SRU diagnostic `First record position out of range` on valid pages (e.g. size 101 failed, 130 and 150 worked; 1000 always fails) | Page size 100; on this diagnostic retry with a smaller page |
| Intermittent `RemoteDisconnected` / network errors (3 in ~110 requests) | Retry with exponential backoff (10 s, 20 s, 40 s) |
| Diagnostics come as HTTP 200 with `<diag:message>` in the body | Always parse and raise on diagnostics |

The reference implementation of all of the above is [`explore/sru_explore.py`](../explore/sru_explore.py) (stdlib only).

## Data available per record (MARC21)

Fields to extract, grouped by use. `008` positions are 0-based.

| Use | Field | Content | Example |
|---|---|---|---|
| Key / change detection | `001` | Record ID | `1740479` |
| | `005` | Last modification timestamp | `20250607051334.9` |
| When | `008/06` | Date type: `s` single, **`r` reprint** (then `008/11-14` is the original year), `m` range… | `s` |
| | `008/07-10` | Publication year (date 1) | `1950` |
| | `008/11-14` | Date 2 (original year for reprints, end year for ranges) | |
| Where published | `008/15-17` | MARC country code (`ag` Argentina, `xx` unknown) | `ag` |
| | `260` / `264` | Place, publisher, date (free text) | `Buenos Aires : Depalma, 1950` |
| About which place | `043` | Geographic area codes, repeatable `$a` | `s-ag---` |
| | `651` | Geographic subject headings | `Jujuy (Argentina : Province) -- Antiquities` |
| Topic | `050` | LC call number; **first letter = discipline** (F history of the Americas, H social sciences, K law, P literature…) | `LB2899.A7` |
| | `650` | Topical subjects, **keep subfields**: `$a` topic, `$x` subtopic, `$z` place, **`$y` historical period** | `Education $z Argentina` |
| Who | `100` / `110` | Main author (person / corporate); **`$d` life dates** disambiguate homonyms | `Perón, Juan Domingo, $d 1895-1974` |
| | `700` / `710` | Co-authors, translators, institutions | |
| Identity (dedup) | `010` | LCCN | `74282969` |
| | `020` | ISBN | |
| | `035` | OCLC number | `(OCoLC)4236544` |
| | `041` | All languages (original + translation); `008/35-37` holds only one | `eng`, `spa` |
| | `250` / `300` | Edition / extent: tell a reprint from a duplicate | `xi, 284 l.` |
| Text for NLP | `245` | Title (`$a`, `$b`), statement of responsibility (`$c`) | |
| | `5xx` | Notes: `502` thesis, `504` bibliography, `505` contents, `520` summary (rare) | `Thesis--University of Utah` |

Observed completeness (sample: Argentina, 1950, 175 records): year/country/language/LC class/title 100%; author 99%; topical subjects 73%; `043` 62%; geographic subjects 29%.

Most records only have title + subjects (no summary): NLP will work on short texts. LC subject headings can serve as labels.

**Not yet implemented** in `flatten_marc` (current output is a draft): `008/06` date type and date 2, `010`/`020`/`035`, `100$d`, `7xx`, `041`, `250`, `5xx`, structured subjects (currently joined with ` -- `).

## Observed volumes

Records per publication year, subject = name OR adjective (from `explore/sru_out/counts_*_2000_2025.csv`):

| Year | Argentina | Mexico | Brazil | Chile |
|---|---|---|---|---|
| 2000 | 683 | 1,691 | 1,943 | 269 |
| 2010 | 654 | 1,903 | 2,776 | 194 |
| 2019 | 514 | 1,130 | 2,378 | 247 |
| 2020 | 417 | 1,045 | 2,110 | 144 |
| 2023 | 280 | 937 | 2,044 | 188 |
| 2024 | 183 | 557 | 1,825 | 121 |
| 2025 | 166 | 485 | 1,380 | 76 |
| All years | 81,312 | 161,105 | | |

- 2023–2025 drop in every country: **cataloging lag**, not less output. Recent years must be re-harvested and flagged as incomplete in gold.
- 2020 drop in all four countries, likely pandemic (acquisition/cataloging halted). Inferred, not verified.
- Estimate for Latin America: ~6,500 records per publication year, ~500k records for 1900–2025.

## Load strategy

| Load | Scope | Cadence | Cost (estimate) |
|---|---|---|---|
| Backfill | All publication years, all Latin American countries | Once, then monthly/quarterly to pick up corrections to old records | ~5,000 requests, 4–5 h |
| Rolling window | Last 3–5 publication years, per country | Daily | ~300 requests, ~15 min |

- Idempotent upsert into Delta: `MERGE` on `001`, update only when `005` changed.
- The same record can match several countries (a work about Argentina and Chile): dedup on `001` before merging.

## Medallion layout (proposal)

- **Bronze**, per source: the full record as received, as **MARC-in-JSON** (`pymarc` `record.as_dict()`), plus ingestion metadata (source, query, page, ingest timestamp). Partition by `source` / `ingest_date`. Nothing dropped, so NLP can use any field later without re-calling the API.
- **Silver, per source**: canonical flattened record (fields in the table above), with `source` and `source_record_id`. Subjects as structs `{topic, subtopic, place, period}`.
- **Silver, conformed** (later): `work` entity linking records across sources; author, country and topic dimensions harmonized (LC Classification vs Dewey/UDC used by Latin American libraries).
- **Gold**: works per year × country × topic; LoC (external view) vs national libraries (internal view).

Use `pymarc` for parsing instead of the hand-written XML parsing in the exploration script.

## Open decisions

1. **What "country" means** for the catalog: country the work is about (subjects / `043`), country of publication (`008/15-17`) or author nationality. Recommendation: store all three, choose in gold. Ingestion is still driven by subject, the only country filter SRU offers.
2. Country list and the name/adjective terms per country (e.g. `Mexico` + `Mexican`, `Brazil` + `Brazilian`).
3. Rolling window size (3 vs 5 years).
4. Connector design: generic per-protocol connectors configured per source (recommended) vs one script per source vs a framework such as `dlt`.

## Exploration scripts

```bash
cd explore
python3 sru_explore.py counts --terms Argentina Argentine --from-year 2000 --to-year 2025
python3 sru_explore.py sample --terms Argentina Argentine --year 1950 --limit 250
```

- `explore/sru_explore.py`: SRU client (throttle, request cap, retries) + `counts` and `sample` commands.
- `explore/sru_out/`: counts CSVs and a flattened JSONL sample.
- `explore/sru_samples/`: raw MARCXML and DC responses.
- `explore/hlas_probe.py`: loc.gov JSON API probe (currently blocked, kept for reference).
