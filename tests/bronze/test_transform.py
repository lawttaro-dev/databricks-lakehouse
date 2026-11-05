from datetime import datetime
from pathlib import Path

import pytest

from lakehouse.bronze.marcxml import read_pages
from lakehouse.bronze.transform import to_bronze

LANDING = str(Path(__file__).parent / "data" / "landing")

BRONZE_COLUMNS = [
    ("record_id", "string"),
    ("record_modified", "timestamp"),
    ("leader", "string"),
    ("controlfield", "array<struct<_tag:string,_VALUE:string>>"),
    (
        "datafield",
        "array<struct<_tag:string,_ind1:string,_ind2:string,"
        "subfield:array<struct<_code:string,_VALUE:string>>>>",
    ),
    ("batch", "string"),
    ("country", "string"),
    ("year", "int"),
    ("source_file", "string"),
    ("ingested_at", "timestamp"),
]


@pytest.fixture(scope="module")
def bronze(spark):
    return to_bronze(read_pages(spark, LANDING)).cache()


def by_id(bronze, record_id):
    return bronze.where(bronze.record_id == record_id).first()


def test_output_columns_and_types(bronze):
    assert bronze.dtypes == BRONZE_COLUMNS


def test_keeps_every_record(bronze):
    assert bronze.count() == 32


def test_record_id_comes_from_controlfield_001(bronze):
    ids = {row.record_id for row in bronze.where("country = 'AR'").collect()}

    assert ids == {"6490066", "1740479", "8170781"}


def test_record_modified_parses_controlfield_005(bronze):
    assert by_id(bronze, "6490066").record_modified == datetime(2025, 6, 7, 23, 53, 1, 600000)


def test_record_modified_is_null_without_005(bronze):
    assert by_id(bronze, "fixture-no-005").record_modified is None


def test_partition_columns_come_from_the_path(bronze):
    row = by_id(bronze, "in00024301146")

    assert (row.batch, row.country, row.year) == ("window-2026-11-04", "CL", 2026)


def test_source_file_points_to_the_landed_page(bronze):
    path = by_id(bronze, "6490066").source_file

    assert path.endswith("batch=b1/country=AR/year=1950/page-000001.xml")


def test_ingested_at_is_set(bronze):
    assert bronze.where("ingested_at is null").count() == 0


def test_marc_fields_are_unchanged(bronze):
    row = by_id(bronze, "6490066")

    assert row.leader == "00905cam a22003131  4500"
    assert len(row.datafield) == 21
