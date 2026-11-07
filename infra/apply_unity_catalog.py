"""Create the Unity Catalog objects (catalogs, schemas, volumes) for each environment.

Also grants each environment's service principal (infra/principals.yml) the privileges in
infra/grants.sql on its own catalog only, so the qa principal cannot touch prod.

Catalogs, schemas and volumes are long-lived infrastructure, managed here instead of in the
Asset Bundle, so that removing something from the bundle can never drop a schema with its
tables (see docs/03-decisions.md, ADR-010).

Usage:
    uv run python infra/apply_unity_catalog.py --dry-run          # print the SQL only
    uv run python infra/apply_unity_catalog.py                    # dev, qa and prod
    uv run python infra/apply_unity_catalog.py --env dev
"""

import argparse
import sys
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

ENVIRONMENTS = ("dev", "qa", "prod")
SQL_TEMPLATE = Path(__file__).with_name("unity_catalog.sql")
GRANTS_TEMPLATE = Path(__file__).with_name("grants.sql")
PRINCIPALS = Path(__file__).with_name("principals.yml")


def split_sql(template: Path, **values: str) -> list[str]:
    """Fill a SQL template and split it into single statements."""
    lines = [
        line for line in template.read_text().splitlines() if not line.lstrip().startswith("--")
    ]
    sql = "\n".join(lines)
    for key, value in values.items():
        sql = sql.replace(f"{{{key}}}", value)
    return [stmt.strip() for stmt in sql.split(";") if stmt.strip()]


def render_statements(catalog: str) -> list[str]:
    """Objects for one catalog, plus grants if the environment has a service principal."""
    statements = split_sql(SQL_TEMPLATE, catalog=catalog)
    principals = {}
    if PRINCIPALS.exists():
        principals = yaml.safe_load(PRINCIPALS.read_text()) or {}
    if principals.get(catalog):
        statements += split_sql(GRANTS_TEMPLATE, catalog=catalog, principal=principals[catalog])
    return statements


def pick_warehouse(client: WorkspaceClient, warehouse_id: str | None) -> str:
    if warehouse_id:
        return warehouse_id
    warehouses = list(client.warehouses.list())
    if not warehouses:
        sys.exit("No SQL warehouse found; pass --warehouse-id")
    return warehouses[0].id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env", choices=ENVIRONMENTS, action="append", help="repeatable")
    parser.add_argument("--warehouse-id", help="defaults to the first warehouse found")
    parser.add_argument("--profile", help="Databricks CLI profile (defaults to the CLI default)")
    parser.add_argument("--dry-run", action="store_true", help="print the SQL, run nothing")
    args = parser.parse_args()

    envs = args.env or list(ENVIRONMENTS)
    if args.dry_run:
        for env in envs:
            for stmt in render_statements(env):
                print(f"{stmt};\n")
        return

    client = WorkspaceClient(profile=args.profile)
    warehouse_id = pick_warehouse(client, args.warehouse_id)
    for env in envs:
        for stmt in render_statements(env):
            first_line = stmt.splitlines()[0]
            response = client.statement_execution.execute_statement(
                statement=stmt, warehouse_id=warehouse_id, wait_timeout="50s"
            )
            state = response.status.state
            if state != StatementState.SUCCEEDED:
                error = response.status.error.message if response.status.error else state
                sys.exit(f"FAILED [{env}] {first_line}\n  {error}")
            print(f"ok  [{env}] {first_line}")


if __name__ == "__main__":
    main()
