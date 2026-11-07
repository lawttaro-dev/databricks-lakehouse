from pathlib import Path

import pytest

from lakehouse.bronze.marcxml import read_pages
from lakehouse.bronze.transform import to_bronze

LANDING = str(Path(__file__).parent.parent / "bronze" / "data" / "landing")


@pytest.fixture(scope="session")
def bronze(spark):
    """Bronze rows built from the landing fixtures: 32 records, AR, CL and MX."""
    return to_bronze(read_pages(spark, LANDING)).cache()
