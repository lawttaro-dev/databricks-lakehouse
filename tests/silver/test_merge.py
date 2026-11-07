from datetime import timedelta
from uuid import uuid4

import pytest
from pyspark.sql import functions as F

from lakehouse.silver.merge import merge_into
from lakehouse.silver.transform import to_silver


@pytest.fixture
def table():
    return f"silver_{uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def silver(bronze):
    return to_silver(bronze).cache()


def read(spark, table, record_id):
    return spark.table(table).where(F.col("record_id") == record_id).first()


def test_first_run_creates_the_table(spark, silver, table):
    merge_into(silver, table)

    assert spark.table(table).count() == 32


def test_rerunning_the_same_batch_changes_nothing(spark, silver, table):
    merge_into(silver, table)
    merge_into(silver, table)

    assert spark.table(table).count() == 32
    history = spark.sql(f"DESCRIBE HISTORY {table}").orderBy("version")
    assert history.collect()[-1].operationMetrics["numTargetRowsUpdated"] == "0"


def test_newer_version_replaces_the_record(spark, silver, table):
    merge_into(silver, table)
    newer = silver.where("record_id = '6490066'").withColumns(
        {
            "title": F.lit("The voice of Perón (corrected)"),
            "record_modified": F.col("record_modified") + timedelta(days=1),
        }
    )

    merge_into(newer, table)

    assert read(spark, table, "6490066").title == "The voice of Perón (corrected)"


def test_older_version_is_ignored(spark, silver, table):
    merge_into(silver, table)
    older = silver.where("record_id = '6490066'").withColumns(
        {
            "title": F.lit("stale"),
            "record_modified": F.col("record_modified") - timedelta(days=1),
        }
    )

    merge_into(older, table)

    assert read(spark, table, "6490066").title == "The voice of Peron"


def test_same_record_from_another_country_adds_the_country(spark, silver, table):
    merge_into(silver, table)
    from_chile = silver.where("record_id = '6490066'").withColumn(
        "harvest_countries", F.array(F.lit("CL"))
    )

    merge_into(from_chile, table)

    row = read(spark, table, "6490066")
    assert row.harvest_countries == ["AR", "CL"]
    assert row.title == "The voice of Peron"
