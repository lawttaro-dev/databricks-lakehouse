"""Pure bronze transformations: DataFrame -> DataFrame, no I/O."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MARC_TIMESTAMP = "yyyyMMddHHmmss.S"  # controlfield 005, e.g. 20250607235301.6


def controlfield(tag: str) -> Column:
    """Value of the first controlfield with this tag, null if the record has none.

    `F.get` instead of `[0]`: with ANSI mode (Spark 4 default) indexing an empty
    array raises an error instead of returning null.
    """
    matches = F.filter("controlfield", lambda field: field["_tag"] == tag)
    return F.get(matches, 0)["_VALUE"]


def path_segment(path: Column, key: str) -> Column:
    """Value of a Hive-style `key=value` folder in a file path."""
    return F.regexp_extract(path, rf"/{key}=([^/]+)/", 1)


def to_bronze(pages: DataFrame) -> DataFrame:
    """Shape parsed MARC records into bronze rows.

    Input: the output of `marcxml.read_pages` (or the Auto Loader stream), with columns
    `leader`, `controlfield`, `datafield` and `_metadata`.

    Output columns, in this order:
        record_id        string     controlfield 001
        record_modified  timestamp  controlfield 005, format yyyyMMddHHmmss.S (null if absent)
        leader           string     unchanged
        controlfield     array      unchanged
        datafield        array      unchanged
        batch            string     from the file path: batch=<batch>/
        country          string     from the file path: country=<CC>/
        year             int        from the file path: year=<YYYY>/
        source_file      string     full path of the landed page
        ingested_at      timestamp  when this row was written

    Nothing is filtered or deduplicated here: bronze keeps every record as received.
    """
    path = F.col("_metadata.file_path")
    return pages.select(
        controlfield("001").alias("record_id"),
        F.to_timestamp(controlfield("005"), MARC_TIMESTAMP).alias("record_modified"),
        "leader",
        "controlfield",
        "datafield",
        path_segment(path, "batch").alias("batch"),
        path_segment(path, "country").alias("country"),
        path_segment(path, "year").cast("int").alias("year"),
        path.alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )
