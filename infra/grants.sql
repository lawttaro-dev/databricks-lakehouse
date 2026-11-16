-- Privileges for the service principal that deploys and runs jobs in one environment.
-- {catalog} and {principal} (application id) are replaced by infra/apply_unity_catalog.py.
-- Granted on the catalog, so they apply to every schema, table and volume inside it.
-- No CREATE SCHEMA / CREATE VOLUME: those stay in infra/unity_catalog.sql (ADR-010).

GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE, READ VOLUME, WRITE VOLUME
  ON CATALOG {catalog} TO `{principal}`;

-- Read-only access for workspace users: qa/prod tables are owned by the service principal,
-- and owning the catalog does not grant SELECT on them. Writes stay with the principal.
GRANT USE CATALOG, USE SCHEMA, SELECT, READ VOLUME
  ON CATALOG {catalog} TO `account users`;
