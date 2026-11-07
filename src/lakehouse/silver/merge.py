"""Upsert of silver rows into a Delta table (the only I/O in silver besides the job)."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

NEWER = (
    "s.record_modified > t.record_modified"
    " OR (t.record_modified IS NULL AND s.record_modified IS NOT NULL)"
)
COUNTRIES_UNION = "array_sort(array_union(t.harvest_countries, s.harvest_countries))"


def merge_into(updates: DataFrame, table: str) -> None:
    """MERGE `updates` (one row per record_id) into `table`, creating it on the first run.

    - New record: inserted.
    - Known record with a newer 005: every column replaced, countries unioned.
    - Known record, same or older 005, new country: only the countries unioned.
    - Anything else: untouched, so re-running a batch changes nothing.
    """
    spark = updates.sparkSession
    if not spark.catalog.tableExists(table):
        updates.write.format("delta").saveAsTable(table)
        return

    replace_all = {column: f"s.{column}" for column in updates.columns}
    replace_all["harvest_countries"] = COUNTRIES_UNION
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(updates.alias("s"), "t.record_id = s.record_id")
        .whenMatchedUpdate(condition=NEWER, set=replace_all)
        .whenMatchedUpdate(
            condition="size(array_except(s.harvest_countries, t.harvest_countries)) > 0",
            set={"harvest_countries": COUNTRIES_UNION},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
