"""Reading MARCXML pages with Spark's built-in XML source (ADR-011).

Each `<record>` element (MARC21 slim namespace) becomes one row. The SRU envelope
(`zs:record`, `zs:recordData`) is skipped because the row tag matches only `record`.

The schema is explicit on purpose. Inference turns tags into numbers ("001" -> 1)
and fails on blank indicators (" "), which nulls the whole `datafield` array.
XML attributes are prefixed with "_" and element text is "_VALUE".
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

ROW_TAG = "record"
PAGE_GLOB = "page-*.xml"  # skips _SUCCESS markers and anything else in the folders

SUBFIELD = StructType([StructField("_code", StringType()), StructField("_VALUE", StringType())])
CONTROLFIELD = StructType([StructField("_tag", StringType()), StructField("_VALUE", StringType())])
DATAFIELD = StructType(
    [
        StructField("_tag", StringType()),
        StructField("_ind1", StringType()),
        StructField("_ind2", StringType()),
        StructField("subfield", ArrayType(SUBFIELD)),
    ]
)
MARC_SCHEMA = StructType(
    [
        StructField("leader", StringType()),
        StructField("controlfield", ArrayType(CONTROLFIELD)),
        StructField("datafield", ArrayType(DATAFIELD)),
    ]
)

# Options shared by the batch reader (tests) and Auto Loader (job)
READ_OPTIONS = {
    "rowTag": ROW_TAG,
    "pathGlobFilter": PAGE_GLOB,
    "ignoreSurroundingSpaces": "false",  # indicators are often a single space
}


def read_pages(spark: SparkSession, path: str) -> DataFrame:
    """Batch read of every landed page under `path`.

    Returns the MARC columns plus `_metadata`, the hidden file-source struct
    (`file_path`, `file_name`, `file_modification_time`, ...).
    """
    return (
        spark.read.format("xml")
        .options(**READ_OPTIONS)
        # Keep Hive-style folders as plain path segments: to_bronze parses them itself,
        # the same way it must under Auto Loader.
        .option("recursiveFileLookup", "true")
        .schema(MARC_SCHEMA)
        .load(path)
        .select("*", "_metadata")
    )
