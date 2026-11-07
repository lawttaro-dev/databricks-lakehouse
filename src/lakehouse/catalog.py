"""Table documentation: descriptions defined in code, applied to Unity Catalog as comments.

Each layer declares a `TableDoc` next to its transformation. Jobs call `apply_comments` after
writing, so Catalog Explorer, the SQL editor and workspace search show the same text as the
generated data dictionary (`python -m lakehouse.catalog > docs/07-data-dictionary.md`).
"""

from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class TableDoc:
    name: str  # table name inside its schema, e.g. "loc_records"
    schema: str  # bronze, silver, gold
    description: str
    columns: dict[str, str]  # column -> description, in table order

    def full_name(self, catalog: str) -> str:
        return f"{catalog}.{self.schema}.{self.name}"


def _quote(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def apply_comments(spark: SparkSession, table: str, doc: TableDoc) -> list[str]:
    """Set table and column comments that differ from `doc`. Returns the statements run.

    Each ALTER is a Delta commit, so unchanged comments are skipped to keep the table
    history readable.
    """
    statements = []
    if (spark.catalog.getTable(table).description or "") != doc.description:
        statements.append(f"COMMENT ON TABLE {table} IS {_quote(doc.description)}")

    current = {column.name: column.description or "" for column in spark.catalog.listColumns(table)}
    for column, description in doc.columns.items():
        if column in current and current[column] != description:
            statements.append(
                f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT {_quote(description)}"
            )

    for statement in statements:
        spark.sql(statement)
    return statements


def to_markdown(docs: list[TableDoc]) -> str:
    lines = [
        "# Data dictionary",
        "",
        "Generated from code (`src/lakehouse/*/transform.py`) with "
        "`uv run python -m lakehouse.catalog > docs/07-data-dictionary.md`. Do not edit by hand.",
        "The same descriptions are applied as Unity Catalog comments by the jobs.",
        "Tables exist in every environment catalog (`dev`, `qa`, `prod`).",
    ]
    for doc in docs:
        lines += ["", f"## {doc.schema}.{doc.name}", "", doc.description, ""]
        lines += ["| Column | Description |", "|---|---|"]
        lines += [f"| `{column}` | {text} |" for column, text in doc.columns.items()]
    return "\n".join(lines) + "\n"


def all_docs() -> list[TableDoc]:
    from lakehouse.bronze.transform import BRONZE_DOC
    from lakehouse.silver.transform import SILVER_DOC

    return [BRONZE_DOC, SILVER_DOC]


if __name__ == "__main__":
    print(to_markdown(all_docs()), end="")
