"""Job entry point: lakehouse-bronze.

Incrementally ingests new landed pages into <catalog>.bronze.loc_records with Auto Loader.
Runs with `availableNow`: processes every file not seen yet, then stops (a batch job with
streaming bookkeeping). The checkpoint remembers which files were already ingested.
"""

import argparse

from pyspark.sql import SparkSession

from lakehouse.bronze.marcxml import MARC_SCHEMA, READ_OPTIONS
from lakehouse.bronze.transform import to_bronze


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest landed LoC pages into bronze.")
    parser.add_argument("--catalog", required=True, help="environment catalog: dev, qa or prod")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    landing = f"/Volumes/{args.catalog}/landing/loc_sru"
    checkpoint = f"/Volumes/{args.catalog}/bronze/checkpoints/loc_records"
    table = f"{args.catalog}.bronze.loc_records"

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


if __name__ == "__main__":
    main()
