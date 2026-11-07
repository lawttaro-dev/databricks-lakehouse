# Data dictionary

Generated from code (`src/lakehouse/*/transform.py`) with `uv run python -m lakehouse.catalog > docs/07-data-dictionary.md`. Do not edit by hand.
The same descriptions are applied as Unity Catalog comments by the jobs.
Tables exist in every environment catalog (`dev`, `qa`, `prod`).

## bronze.loc_records

Library of Congress catalog records (MARC21) as landed from SRU, one row per record per landed page. Nothing is filtered or deduplicated: the same record can appear several times (re-harvests, repeated pages, several country queries). Source: Volume landing.loc_sru, ingested by Auto Loader.

| Column | Description |
|---|---|
| `record_id` | MARC 001: LoC record control number. Not unique in bronze. |
| `record_modified` | MARC 005: when LoC last changed the record. Null if absent. |
| `leader` | MARC leader: 24 fixed positions (record status, type, encoding level). |
| `controlfield` | MARC control fields 001-009 as {_tag, _VALUE}, in record order. |
| `datafield` | MARC data fields as {_tag, _ind1, _ind2, subfield: [{_code, _VALUE}]}, in record order. The full record: nothing is dropped. |
| `batch` | Extraction batch: backfill-<from>-<to> or window-<run date>. From the landing path batch=<batch>/. |
| `country` | Country code whose subject query returned the record (AR, MX, BR, CL). From the landing path. |
| `year` | Publication year used in the query (dc.date). From the landing path. |
| `source_file` | Full path of the landed SRU page the record came from. |
| `ingested_at` | When the row was written to bronze. |

## silver.loc_records

Library of Congress catalog records, one row per record (MARC 001): the latest version by 005, flattened into canonical columns. Upserted from bronze.loc_records with MERGE (ADR-012). Trailing ISBD punctuation is removed from text fields.

| Column | Description |
|---|---|
| `record_id` | MARC 001: LoC record control number. Unique. |
| `record_modified` | MARC 005: when LoC last changed the record. Drives updates. |
| `title` | 245 $a $b: title and subtitle. |
| `responsibility` | 245 $c: statement of responsibility as printed (authors, editors). |
| `author` | 100 $a (person) or 110 $a (organization): main entry. |
| `author_dates` | 100 $d: life dates of the main author, to tell homonyms apart. |
| `date_type` | 008/06: s single date, r reprint, m range, etc. (MARC date type). |
| `pub_year` | 008/07-10: publication year. Null when partly unknown (e.g. 19uu). |
| `date2` | 008/11-14: original year of a reprint or end year of a range. |
| `pub_country` | 008/15-17: MARC country code of publication (ag Argentina, xx unknown). |
| `language` | 008/35-37: main language, MARC code (spa, eng, por). |
| `languages` | 041 $a: every language of the work (original and translations). |
| `geo_areas` | 043 $a: geographic areas the work is about (s-ag Argentina, s-cl Chile). |
| `lc_call_number` | 050 $a $b: LC call number. |
| `lc_class` | Leading letters of the LC call number; the first letter is the discipline. |
| `publication` | 264 (2nd indicator 1) or 260, $a $b $c: place, publisher, date. |
| `subjects_topical` | 650 topical subjects. Array of {heading ($a), topics ($x), places ($z), periods ($y), forms ($v), vocabulary}; vocabulary is lcsh (2nd indicator 0) or the code in $2 (e.g. bisacsh). |
| `subjects_geographic` | 651 geographic subjects. Array of {heading ($a), topics ($x), places ($z), periods ($y), forms ($v), vocabulary}; vocabulary is lcsh (2nd indicator 0) or the code in $2 (e.g. bisacsh). |
| `summary` | 520 $a: summary or abstract. Often absent. |
| `lccn` | 010 $a: Library of Congress Control Number. |
| `isbns` | 020 $a: valid ISBNs (cancelled or invalid ones in $z are left out). |
| `oclc` | 035 $a (OCoLC) number: OCLC WorldCat id, to link with other catalogs. |
| `harvest_countries` | Every country query that returned the record (AR, MX, BR, CL), sorted. More than one for works about several countries. |
| `ingested_at` | When the stored version was written to bronze. |
