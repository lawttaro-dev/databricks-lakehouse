"""Pure bronze transformations: DataFrame -> DataFrame, no I/O."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse.catalog import TableDoc

MARC_TIMESTAMP = "yyyyMMddHHmmss.S"  # controlfield 005, e.g. 20250607235301.6


BRONZE_DOC = TableDoc(
    name="loc_records",
    schema="bronze",
    description=(
        "Library of Congress catalog records (MARC21) as landed from SRU, one row per record "
        "per landed page. Nothing is filtered or deduplicated: the same record can appear "
        "several times (re-harvests, repeated pages, several country queries). Source: Volume "
        "landing.loc_sru, ingested by Auto Loader."
    ),
    columns={
        "record_id": "MARC 001: LoC record control number. Not unique in bronze.",
        "record_modified": "MARC 005: when LoC last changed the record. Null if absent.",
        "leader": "MARC leader: 24 fixed positions (record status, type, encoding level).",
        "controlfield": "MARC control fields 001-009 as {_tag, _VALUE}, in record order.",
        "datafield": (
            "MARC data fields as {_tag, _ind1, _ind2, subfield: [{_code, _VALUE}]}, "
            "in record order. The full record: nothing is dropped."
        ),
        "batch": (
            "Extraction batch: backfill-<from>-<to> or window-<run date>. "
            "From the landing path batch=<batch>/."
        ),
        "country": (
            "Country code whose subject query returned the record (AR, MX, BR, CL). "
            "From the landing path."
        ),
        "year": "Publication year used in the query (dc.date). From the landing path.",
        "source_file": "Full path of the landed SRU page the record came from.",
        "ingested_at": "When the row was written to bronze.",
    },
)


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
    `leader`, `controlfield`, `datafield` and `_metadata`. Output columns: `BRONZE_DOC`.

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
