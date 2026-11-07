-- Unity Catalog objects for one environment. Idempotent: safe to re-run.
-- {catalog} is replaced by infra/apply_unity_catalog.py (dev, qa, prod).

CREATE CATALOG IF NOT EXISTS {catalog}
  COMMENT 'Lakehouse environment: {catalog}';

CREATE SCHEMA IF NOT EXISTS {catalog}.landing
  COMMENT 'Raw files exactly as received from sources, stored in Volumes';
CREATE SCHEMA IF NOT EXISTS {catalog}.bronze
  COMMENT 'Source records as Delta tables, unmodified, plus ingestion metadata';
CREATE SCHEMA IF NOT EXISTS {catalog}.silver
  COMMENT 'Cleaned, flattened and deduplicated records per source';
CREATE SCHEMA IF NOT EXISTS {catalog}.gold
  COMMENT 'Aggregates for analysis: works per year, country and topic';

CREATE VOLUME IF NOT EXISTS {catalog}.landing.loc_sru
  COMMENT 'Library of Congress catalog: raw SRU MARCXML pages';
CREATE VOLUME IF NOT EXISTS {catalog}.bronze.checkpoints
  COMMENT 'Streaming checkpoints (Auto Loader state) for bronze ingestion jobs';
CREATE VOLUME IF NOT EXISTS {catalog}.silver.checkpoints
  COMMENT 'Streaming checkpoints for silver jobs (bronze table as source)';
