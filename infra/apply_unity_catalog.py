"""Create the Unity Catalog objects (catalogs, schemas, volumes) for each environment.

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

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

ENVIRONMENTS = ("dev", "qa", "prod")
SQL_TEMPLATE = Path(__file__).with_name("unity_catalog.sql")


def render_statements(catalog: str) -> list[str]:
    """Split the SQL template into single statements for one catalog."""
    lines = [
        line for line in SQL_TEMPLATE.read_text().splitlines() if not line.lstrip().startswith("--")
    ]
    sql = "\n".join(lines).replace("{catalog}", catalog)
    return [stmt.strip() for stmt in sql.split(";") if stmt.strip()]


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
