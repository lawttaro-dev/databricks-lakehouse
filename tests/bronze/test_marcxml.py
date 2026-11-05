from pathlib import Path

from pyspark.sql import functions as F

from lakehouse.bronze.marcxml import read_pages

LANDING = str(Path(__file__).parent / "data" / "landing")


def test_reads_one_row_per_marc_record_and_skips_markers(spark):
    pages = read_pages(spark, LANDING)

    # AR 1950: 3, CL 2026: 28, MX 1900: 1. The _SUCCESS file is not parsed.
    assert pages.count() == 32


def test_keeps_tags_and_blank_indicators_as_strings(spark):
    pages = read_pages(spark, LANDING)

    tags = pages.select(F.explode("controlfield._tag").alias("tag")).distinct()
    assert {row.tag for row in tags.collect()} >= {"001", "005", "008"}

    f245 = (
        pages.select(F.explode("datafield").alias("f"))
        .where("f._tag = '245' and f.subfield[0]._VALUE = 'The voice of Peron.'")
        .first()
        .f
    )
    assert (f245._ind1, f245._ind2) == ("1", "4")
