"""Smoke tests: the local JVM, PySpark and Delta Lake work together."""

from pyspark.testing import assertDataFrameEqual


def test_dataframe_roundtrip(spark):
    df = spark.createDataFrame([(1, "Argentina"), (2, "Chile")], ["id", "country"])

    assert df.count() == 2


def test_delta_write_merge_and_time_travel(spark, tmp_path):
    path = str(tmp_path / "records")
    spark.createDataFrame([(1, "v1")], ["id", "value"]).write.format("delta").save(path)

    updates = spark.createDataFrame([(1, "v2"), (2, "v1")], ["id", "value"])
    updates.createOrReplaceTempView("updates")
    spark.sql(f"""
        MERGE INTO delta.`{path}` AS t
        USING updates AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    current = spark.read.format("delta").load(path)
    first_version = spark.read.format("delta").option("versionAsOf", 0).load(path)
    assertDataFrameEqual(current, [(1, "v2"), (2, "v1")], checkRowOrder=False)
    assertDataFrameEqual(first_version, [(1, "v1")])
