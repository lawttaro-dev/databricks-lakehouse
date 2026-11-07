from datetime import timedelta

import pytest
from pyspark.sql import functions as F

from lakehouse.silver.transform import latest_per_record, to_silver

SUBJECT = (
    "struct<heading:string,topics:array<string>,places:array<string>,"
    "periods:array<string>,forms:array<string>,vocabulary:string>"
)
SILVER_COLUMNS = [
    ("record_id", "string"),
    ("record_modified", "timestamp"),
    ("title", "string"),
    ("responsibility", "string"),
    ("author", "string"),
    ("author_dates", "string"),
    ("date_type", "string"),
    ("pub_year", "int"),
    ("date2", "string"),
    ("pub_country", "string"),
    ("language", "string"),
    ("languages", "array<string>"),
    ("geo_areas", "array<string>"),
    ("lc_call_number", "string"),
    ("lc_class", "string"),
    ("publication", "string"),
    ("subjects_topical", f"array<{SUBJECT}>"),
    ("subjects_geographic", f"array<{SUBJECT}>"),
    ("summary", "string"),
    ("lccn", "string"),
    ("isbns", "array<string>"),
    ("oclc", "string"),
    ("harvest_countries", "array<string>"),
    ("ingested_at", "timestamp"),
]


@pytest.fixture(scope="module")
def silver(bronze):
    return to_silver(bronze).cache()


def by_id(df, record_id):
    return df.where(df.record_id == record_id).first()


def test_output_columns_and_types(silver):
    assert silver.dtypes == SILVER_COLUMNS


def test_one_row_per_record(silver):
    assert silver.count() == 32


def test_flattens_a_premarc_record(silver):
    row = by_id(silver, "6490066")

    assert row.title == "The voice of Peron"
    assert row.author == "Perón, Juan Domingo"  # combining accent, as cataloged
    assert row.author_dates == "1895-1974"
    assert (row.date_type, row.pub_year, row.date2) == ("s", 1950, None)
    assert (row.pub_country, row.language, row.languages) == ("ag", "eng", ["eng", "spa"])
    assert (row.lc_call_number, row.lc_class) == ("F2848 .P475", "F")
    assert row.publication == "[Buenos Aires] 1950"
    assert (row.lccn, row.oclc, row.isbns) == ("50012535", "4236544", [])
    assert row.subjects_topical == []
    assert row.subjects_geographic[0].asDict(recursive=True) == {
        "heading": "Argentina",
        "topics": [],
        "places": [],
        "periods": [],
        "forms": [],
        "vocabulary": "lcsh",
    }


def test_flattens_a_current_rda_record(silver):
    row = by_id(silver, "in00024363109")

    assert row.title == (
        "Land's language : on Mapuche memory, translation, and the territorial aporia"
    )
    assert row.responsibility == "Ethan Madarieta"
    assert row.author == "Madarieta, Ethan"
    assert row.publication == "Evanston, Illinois : Northwestern University Press, 2026"
    assert row.geo_areas == ["s-cl", "s-ag"]
    assert row.isbns == ["9798899480096", "9798899480102"]  # 020 $z (ebook) left out
    assert row.oclc is None  # "(OCoLC)0NEW" is a placeholder, not a number
    assert row.summary.startswith('"Presenting a new framework')


def test_subjects_keep_subdivisions_and_vocabulary(silver):
    subjects = by_id(silver, "in00024363109").subjects_topical

    assert subjects[0].asDict(recursive=True) == {
        "heading": "Mapuche Indians",
        "topics": ["Land tenure"],
        "places": ["Chile"],
        "periods": [],
        "forms": [],
        "vocabulary": "lcsh",
    }
    assert {s.vocabulary for s in subjects} == {"lcsh", "bisacsh"}


def test_missing_fields_are_null_not_errors(silver):
    row = by_id(silver, "fixture-no-005")

    assert row.record_modified is None
    assert (row.author, row.lc_class, row.publication) == (None, None, None)
    assert row.pub_year == 1900


def test_partly_unknown_year_gives_null_year(bronze):
    unknown_decade = "000101s19uu    mx            000 0 spa  "
    record = bronze.where("record_id = 'fixture-no-005'").withColumn(
        "controlfield",
        F.transform(
            "controlfield",
            lambda field: F.when(
                field["_tag"] == "008",
                F.struct(field["_tag"].alias("_tag"), F.lit(unknown_decade).alias("_VALUE")),
            ).otherwise(field),
        ),
    )

    assert to_silver(record).first().pub_year is None


def test_latest_per_record_keeps_newest_version_and_all_countries(bronze):
    record = bronze.where("record_id = '6490066'")
    newer_from_chile = record.withColumn("country", F.lit("CL")).withColumn(
        "record_modified", F.col("record_modified") + timedelta(days=1)
    )
    older_from_mexico = record.withColumn("country", F.lit("MX")).withColumn(
        "record_modified", F.col("record_modified") - timedelta(days=1)
    )

    rows = latest_per_record(record.unionByName(newer_from_chile).unionByName(older_from_mexico))

    assert rows.count() == 1
    row = rows.first()
    assert row.harvest_countries == ["AR", "CL", "MX"]
    assert row.country == "CL"  # the newest version wins
