from pathlib import Path

import pytest

from lakehouse.bronze.transform import BRONZE_DOC, to_bronze
from lakehouse.catalog import TableDoc, all_docs, apply_comments, to_markdown
from lakehouse.silver.transform import SILVER_DOC, to_silver

DICTIONARY = Path(__file__).parent.parent / "docs" / "07-data-dictionary.md"


@pytest.fixture(scope="module")
def bronze(spark):
    from lakehouse.bronze.marcxml import read_pages

    landing = str(Path(__file__).parent / "bronze" / "data" / "landing")
    return to_bronze(read_pages(spark, landing))


def test_every_bronze_column_is_documented_in_order(bronze):
    assert list(BRONZE_DOC.columns) == bronze.columns


def test_every_silver_column_is_documented_in_order(bronze):
    assert list(SILVER_DOC.columns) == to_silver(bronze).columns


def test_generated_dictionary_is_up_to_date():
    expected = to_markdown(all_docs())

    assert DICTIONARY.read_text() == expected, (
        "Regenerate: uv run python -m lakehouse.catalog > docs/07-data-dictionary.md"
    )


def test_apply_comments_sets_only_what_changed(spark):
    spark.sql("CREATE TABLE comments_demo (id STRING, name STRING) USING delta")
    doc = TableDoc("comments_demo", "bronze", "Demo table", {"id": "The id", "name": "It's a name"})

    first = apply_comments(spark, "comments_demo", doc)
    second = apply_comments(spark, "comments_demo", doc)

    assert len(first) == 3  # table + 2 columns
    assert second == []
    columns = {c.name: c.description for c in spark.catalog.listColumns("comments_demo")}
    assert columns == {"id": "The id", "name": "It's a name"}
    assert spark.catalog.getTable("comments_demo").description == "Demo table"
