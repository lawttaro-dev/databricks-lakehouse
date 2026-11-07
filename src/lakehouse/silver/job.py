"""Job entry point: lakehouse-silver.

Reads the rows appended to <catalog>.bronze.loc_records since the last run (Delta table as a
streaming source) and upserts them into <catalog>.silver.loc_records, one micro-batch at a time.
`foreachBatch` is what allows a MERGE, which plain streaming writes (append only) cannot do.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession

from lakehouse.silver.merge import merge_into
from lakehouse.silver.transform import to_silver


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upsert bronze LoC records into silver.")
    parser.add_argument("--catalog", required=True, help="environment catalog: dev, qa or prod")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    source = f"{args.catalog}.bronze.loc_records"
    table = f"{args.catalog}.silver.loc_records"
    checkpoint = f"/Volumes/{args.catalog}/silver/checkpoints/loc_records"

    def upsert(batch: DataFrame, _batch_id: int) -> None:
        merge_into(to_silver(batch), table)

    spark = SparkSession.builder.getOrCreate()
    query = (
        spark.readStream.table(source)
        .writeStream.foreachBatch(upsert)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
