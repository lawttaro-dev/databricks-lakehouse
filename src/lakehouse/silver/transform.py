"""Pure silver transformations: DataFrame -> DataFrame, no I/O.

Field choice and MARC positions: docs/02-data-source-loc-catalog.md ("Data available per record").
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.catalog import TableDoc
from lakehouse.silver.marc import (
    all_subfields,
    first_datafield,
    first_subfield,
    fixed_field,
    joined,
    subject_headings,
    tidy,
)

SUBJECT = (
    "Array of {heading ($a), topics ($x), places ($z), periods ($y), forms ($v), vocabulary}; "
    "vocabulary is lcsh (2nd indicator 0) or the code in $2 (e.g. bisacsh)."
)
SILVER_DOC = TableDoc(
    name="loc_records",
    schema="silver",
    description=(
        "Library of Congress catalog records, one row per record (MARC 001): the latest "
        "version by 005, flattened into canonical columns. Upserted from bronze.loc_records "
        "with MERGE (ADR-012). Trailing ISBD punctuation is removed from text fields."
    ),
    columns={
        "record_id": "MARC 001: LoC record control number. Unique.",
        "record_modified": "MARC 005: when LoC last changed the record. Drives updates.",
        "title": "245 $a $b: title and subtitle.",
        "responsibility": "245 $c: statement of responsibility as printed (authors, editors).",
        "author": "100 $a (person) or 110 $a (organization): main entry.",
        "author_dates": "100 $d: life dates of the main author, to tell homonyms apart.",
        "date_type": "008/06: s single date, r reprint, m range, etc. (MARC date type).",
        "pub_year": "008/07-10: publication year. Null when partly unknown (e.g. 19uu).",
        "date2": "008/11-14: original year of a reprint or end year of a range.",
        "pub_country": "008/15-17: MARC country code of publication (ag Argentina, xx unknown).",
        "language": "008/35-37: main language, MARC code (spa, eng, por).",
        "languages": "041 $a: every language of the work (original and translations).",
        "geo_areas": "043 $a: geographic areas the work is about (s-ag Argentina, s-cl Chile).",
        "lc_call_number": "050 $a $b: LC call number.",
        "lc_class": "Leading letters of the LC call number; the first letter is the discipline.",
        "publication": "264 (2nd indicator 1) or 260, $a $b $c: place, publisher, date.",
        "subjects_topical": f"650 topical subjects. {SUBJECT}",
        "subjects_geographic": f"651 geographic subjects. {SUBJECT}",
        "summary": "520 $a: summary or abstract. Often absent.",
        "lccn": "010 $a: Library of Congress Control Number.",
        "isbns": "020 $a: valid ISBNs (cancelled or invalid ones in $z are left out).",
        "oclc": "035 $a (OCoLC) number: OCLC WorldCat id, to link with other catalogs.",
        "harvest_countries": (
            "Every country query that returned the record (AR, MX, BR, CL), sorted. "
            "More than one for works about several countries."
        ),
        "ingested_at": "When the stored version was written to bronze.",
    },
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
    """Bronze MARC arrays -> canonical silver columns (described in `SILVER_DOC`)."""
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
