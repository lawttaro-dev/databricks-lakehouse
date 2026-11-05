"""Pure bronze transformations: DataFrame -> DataFrame, no I/O."""

from pyspark.sql import DataFrame


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
    raise NotImplementedError("apprentice mode: your turn")
