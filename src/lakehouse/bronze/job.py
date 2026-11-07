"""Job entry point: lakehouse-bronze.

Incrementally ingests new landed pages into <catalog>.bronze.loc_records with Auto Loader.
Runs with `availableNow`: processes every file not seen yet, then stops (a batch job with
streaming bookkeeping). The checkpoint remembers which files were already ingested.
"""

import argparse

from pyspark.sql import SparkSession

from lakehouse.bronze.marcxml import MARC_SCHEMA, READ_OPTIONS
from lakehouse.bronze.transform import BRONZE_DOC, to_bronze
from lakehouse.catalog import apply_comments


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest landed LoC pages into bronze.")
    parser.add_argument("--catalog", required=True, help="environment catalog: dev, qa or prod")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    landing = f"/Volumes/{args.catalog}/landing/loc_sru"
    checkpoint = f"/Volumes/{args.catalog}/bronze/checkpoints/loc_records"
    table = BRONZE_DOC.full_name(args.catalog)

    spark = SparkSession.builder.getOrCreate()
    pages = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .options(**READ_OPTIONS)
        .schema(MARC_SCHEMA)
        .load(landing)
        .select("*", "_metadata")
    )
    query = (
        to_bronze(pages)
        .writeStream.option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()
    if spark.catalog.tableExists(table):  # absent until the first file lands
        apply_comments(spark, table, BRONZE_DOC)


if __name__ == "__main__":
    main()
