"""Column expressions over the bronze MARC arrays (`controlfield`, `datafield`).

Everything here builds Spark expressions with higher-order functions (`filter`,
`transform`), so the work runs inside the JVM, with no Python per row.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

TRAILING_PUNCTUATION = r"[\s/:;,.=]+$"  # ISBD punctuation that MARC leaves at field ends


def tidy(value: Column) -> Column:
    """Strip trailing ISBD punctuation and surrounding spaces; empty becomes null."""
    return F.nullif(F.trim(F.regexp_replace(value, TRAILING_PUNCTUATION, "")), F.lit(""))


def controlfield(tag: str) -> Column:
    """Value of the first controlfield with this tag, null if absent.

    `F.get` instead of `[0]`: under ANSI mode (Spark 4 default) indexing an empty
    array raises an error instead of returning null.
    """
    return F.get(F.filter("controlfield", lambda field: field["_tag"] == tag), 0)["_VALUE"]


def fixed_field(tag: str, start: int, length: int) -> Column:
    """Positions of a fixed-length controlfield (0-based `start`, as in MARC docs)."""
    return F.nullif(F.trim(F.substring(controlfield(tag), start + 1, length)), F.lit(""))


def datafields(tag: str, ind2: str | None = None) -> Column:
    """All datafields with this tag (optionally with this second indicator)."""

    def matches(field: Column) -> Column:
        condition = field["_tag"] == tag
        return condition & (field["_ind2"] == ind2) if ind2 is not None else condition

    return F.filter("datafield", matches)


def first_datafield(tag: str, ind2: str | None = None) -> Column:
    return F.get(datafields(tag, ind2), 0)


def subfields(field: Column, *codes: str) -> Column:
    """Values of the subfields with these codes, in record order."""
    selected = F.filter(field["subfield"], lambda sub: sub["_code"].isin(*codes))
    return F.transform(selected, lambda sub: sub["_VALUE"])


def first_subfield(field: Column, code: str) -> Column:
    return F.get(subfields(field, code), 0)


def joined(field: Column, *codes: str) -> Column:
    """Subfields joined with spaces, trailing punctuation removed."""
    return tidy(F.array_join(subfields(field, *codes), " "))


def all_subfields(tag: str, code: str) -> Column:
    """One subfield code across every repetition of a field (e.g. every 020 $a)."""
    return F.flatten(F.transform(datafields(tag), lambda field: subfields(field, code)))


def subject_headings(tag: str) -> Column:
    """650/651 as structs, keeping the subdivisions apart.

    Vocabulary: second indicator 0 is LCSH, 7 means "named in $2" (e.g. bisacsh).
    """

    def tidied(field: Column, code: str) -> Column:
        return F.transform(subfields(field, code), tidy)

    return F.transform(
        datafields(tag),
        lambda field: F.struct(
            tidy(first_subfield(field, "a")).alias("heading"),
            tidied(field, "x").alias("topics"),
            tidied(field, "z").alias("places"),
            tidied(field, "y").alias("periods"),
            tidied(field, "v").alias("forms"),
            F.when(field["_ind2"] == "0", F.lit("lcsh"))
            .otherwise(first_subfield(field, "2"))
            .alias("vocabulary"),
        ),
    )
