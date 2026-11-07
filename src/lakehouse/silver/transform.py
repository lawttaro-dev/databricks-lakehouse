"""Pure silver transformations: DataFrame -> DataFrame, no I/O.

Field choice and MARC positions: docs/02-data-source-loc-catalog.md ("Data available per record").
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.silver.marc import (
    all_subfields,
    first_datafield,
    first_subfield,
    fixed_field,
    joined,
    subject_headings,
    tidy,
)


def latest_per_record(bronze: DataFrame) -> DataFrame:
    """One row per record_id: the newest version (by 005), with every country it matched.

    The same record shows up several times in bronze: landed twice (re-harvests, overlapping
    pages) or matched by several country queries (a work about Argentina and Chile).
    """
    by_record = Window.partitionBy("record_id")
    newest_first = by_record.orderBy(
        F.col("record_modified").desc_nulls_last(), F.col("ingested_at").desc()
    )
    countries = F.array_sort(F.collect_set("country").over(by_record))
    return (
        bronze.withColumn("harvest_countries", countries)
        .withColumn("rank", F.row_number().over(newest_first))
        .where("rank = 1")
        .drop("rank")
    )


def flatten(records: DataFrame) -> DataFrame:
    """Bronze MARC arrays -> canonical silver columns."""
    title = first_datafield("245")
    author = F.coalesce(first_datafield("100"), first_datafield("110"))
    publication = F.coalesce(first_datafield("264", ind2="1"), first_datafield("260"))
    call_number = first_datafield("050")
    pub_year = fixed_field("008", 7, 4)
    oclc = F.filter(all_subfields("035", "a"), lambda value: value.rlike(r"^\(OCoLC\)\d+$"))

    return records.select(
        "record_id",
        "record_modified",
        joined(title, "a", "b").alias("title"),
        joined(title, "c").alias("responsibility"),
        joined(author, "a").alias("author"),
        tidy(first_subfield(author, "d")).alias("author_dates"),
        fixed_field("008", 6, 1).alias("date_type"),
        # Years like "19uu" are partly unknown: cast only full years (ANSI cast would fail)
        F.when(pub_year.rlike(r"^\d{4}$"), pub_year.cast("int")).alias("pub_year"),
        fixed_field("008", 11, 4).alias("date2"),
        fixed_field("008", 15, 3).alias("pub_country"),
        fixed_field("008", 35, 3).alias("language"),
        all_subfields("041", "a").alias("languages"),
        F.transform(all_subfields("043", "a"), strip_trailing_dashes).alias("geo_areas"),
        joined(call_number, "a", "b").alias("lc_call_number"),
        F.nullif(
            F.regexp_extract(first_subfield(call_number, "a"), r"^[A-Z]+", 0), F.lit("")
        ).alias("lc_class"),
        joined(publication, "a", "b", "c").alias("publication"),
        subject_headings("650").alias("subjects_topical"),
        subject_headings("651").alias("subjects_geographic"),
        first_subfield(first_datafield("520"), "a").alias("summary"),
        tidy(first_subfield(first_datafield("010"), "a")).alias("lccn"),
        F.transform(
            all_subfields("020", "a"), lambda isbn: F.regexp_extract(isbn, r"^[0-9Xx]+", 0)
        ).alias("isbns"),
        F.regexp_extract(F.get(oclc, 0), r"\d+", 0).alias("oclc"),
        "harvest_countries",
        "ingested_at",
    )


def strip_trailing_dashes(code: Column) -> Column:
    """Geographic area codes are padded to 7 characters: "s-ag---" -> "s-ag"."""
    return F.regexp_replace(code, "-+$", "")


def to_silver(bronze: DataFrame) -> DataFrame:
    """Bronze rows (any batch of them) -> one canonical silver row per record."""
    return flatten(latest_per_record(bronze))
