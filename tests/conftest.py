import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark session with Delta Lake, shared by the whole test run."""
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        # Small local runs: avoid the default 200 shuffle partitions
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
