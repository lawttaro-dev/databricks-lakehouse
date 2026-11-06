# %% [markdown]
# # Bronze development notebook (local)
#
# Develop and try the bronze layer end to end on your machine:
#
# 1. Simulate the landing Volume with the test fixtures.
# 2. Explore the parsed MARC records.
# 3. Build the `to_bronze` expressions step by step (scratch cells).
# 4. Run `lakehouse.bronze.transform.to_bronze` (the real code, edited in `transform.py`).
# 5. Simulate the Auto Loader job: incremental ingestion into a local Delta table.
#
# Run cell by cell in VS Code ("Run Cell" above each `# %%`), or the whole file with
# `uv run python notebooks/bronze_dev.py`. Local state lives in `.local/` (git-ignored).

# %%
import importlib
import shutil
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import lakehouse.bronze.transform as transform
from lakehouse.bronze.marcxml import MARC_SCHEMA, READ_OPTIONS, read_pages

ROOT = Path(__file__).resolve().parent.parent if "__file__" in globals() else Path.cwd()
FIXTURES = ROOT / "tests/bronze/data/landing"
WORK = ROOT / ".local/bronze_dev"
LANDING = WORK / "landing"  # plays the role of /Volumes/<catalog>/landing/loc_sru
CHECKPOINT = WORK / "checkpoints/loc_records"  # /Volumes/<catalog>/bronze/checkpoints/...
TABLE = WORK / "tables/loc_records"  # <catalog>.bronze.loc_records, here a path-based table

builder = (
    SparkSession.builder.master("local[2]")
    .appName("bronze-dev")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "2")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def land(*partitions: str) -> None:
    """Copy fixture partitions into the fake landing Volume, as the extractor would."""
    for partition in partitions:
        shutil.copytree(FIXTURES / partition, LANDING / partition, dirs_exist_ok=True)


# Start from scratch on every full run
shutil.rmtree(WORK, ignore_errors=True)
land("batch=b1/country=AR/year=1950")

# %% [markdown]
# ## 2. Explore the parsed records

# %%
pages = read_pages(spark, str(LANDING))
pages.printSchema()
pages.select("leader", "controlfield", "_metadata.file_path").show(truncate=60)

# %%
# One row per datafield of the first record: tag, indicators and subfields
(
    pages.limit(1)
    .select(F.explode("datafield").alias("f"))
    .select("f._tag", "f._ind1", "f._ind2", "f.subfield")
    .show(30, truncate=80)
)

# %% [markdown]
# ## 3. Scratch: build the expressions one at a time
#
# Try each column here, then move the working version into `to_bronze` in
# `src/lakehouse/bronze/transform.py`. Hints are in the function's docstring and tests.

# %%
# record_id: controlfield 001
# pages.select(..., "controlfield").show(truncate=60)

# %%
# record_modified: controlfield 005 as a timestamp (watch out: not every record has a 005)
# pages.select(...).show()

# %%
# batch, country, year: parsed from _metadata.file_path
# pages.select("_metadata.file_path", ...).show(truncate=False)

# %% [markdown]
# ## 4. Run the real `to_bronze`
#
# Re-run this cell after editing `transform.py`: the reload picks up your changes
# without restarting Spark.

# %%
importlib.reload(transform)
bronze = transform.to_bronze(pages)
bronze.printSchema()
bronze.drop("controlfield", "datafield").show(truncate=40)

# %% [markdown]
# ## 5. Simulate the Auto Loader job
#
# Same shape as `src/lakehouse/bronze/job.py`, with two local substitutions:
#
# | Databricks (job.py)                  | Local (here)                          |
# |--------------------------------------|---------------------------------------|
# | `format("cloudFiles")` + `cloudFiles.format=xml` | `format("xml")` file stream source |
# | `.toTable("<catalog>.bronze.loc_records")` | `.start(<path>)` Delta table on disk |
#
# Both remember processed files in the checkpoint, so each run only ingests new pages.


# %%
def run_bronze_job() -> None:
    importlib.reload(transform)
    stream = (
        spark.readStream.format("xml")
        .options(**READ_OPTIONS)
        .option("recursiveFileLookup", "true")
        .schema(MARC_SCHEMA)
        .load(str(LANDING))
        .select("*", "_metadata")
    )
    query = (
        transform.to_bronze(stream)
        .writeStream.format("delta")
        .option("checkpointLocation", str(CHECKPOINT))
        .trigger(availableNow=True)
        .start(str(TABLE))
    )
    query.awaitTermination()


def show_table() -> None:
    table = spark.read.format("delta").load(str(TABLE))
    table.groupBy("batch", "country", "year").count().orderBy("batch", "country").show()


# First run: only AR 1950 has landed
run_bronze_job()
show_table()

# %%
# The extractor lands two more partitions; the next run ingests only those
land("batch=window-2026-11-04/country=CL/year=2026", "batch=b2/country=MX/year=1900")
run_bronze_job()
show_table()

# %%
# Nothing new landed: the run is a no-op
run_bronze_job()
show_table()

# %%
# Every streaming run is a Delta commit: history and time travel come for free
spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`").select(
    "version", "operation", "operationMetrics.numOutputRows"
).show()
spark.read.format("delta").option("versionAsOf", 0).load(str(TABLE)).count()
