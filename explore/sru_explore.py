#!/usr/bin/env python3
"""Explore the Library of Congress catalog through its SRU endpoint (lx2.loc.gov).

Answers "what was written about a country, each year" using:
  - counts: number of catalog records per publication year whose subject matches the terms
  - sample: download MARCXML records for one year, flatten them and summarize by
            LC classification, publication country, language, geographic area and subject

Endpoint notes (verified by hand, Nov 2026):
  - Supported indexes: dc.subject, dc.date (equality only, no ranges), dc.creator, dc.title,
    cql.anywhere, bath.isbn, bath.lccn. Not supported: dc.publisher, dc.language.
  - Record schemas: marcxml (full record), dc (simplified). mods returns server errors.
  - Pages of marcxml records fail intermittently with "First record position out of range"
    (e.g. size=101 failed while 130 and 150 worked). Retrying with a smaller page works.
  - Rate limit is not documented; this script throttles and caps requests anyway.

Usage:
    python3 sru_explore.py counts --terms Argentina Argentine --from-year 1940 --to-year 1960
    python3 sru_explore.py sample --terms Argentina Argentine --year 1950 --limit 250
"""

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

BASE_URL = "http://lx2.loc.gov:210/lcdb"
USER_AGENT = "sru-explore/0.1 (personal data exploration)"
MIN_SECONDS_BETWEEN_REQUESTS = 3.0
MAX_PAGE_SIZE = 100
FLAKY_DIAGNOSTIC = "First record position out of range"
TRANSIENT_RETRIES = 3

NS = {
    "zs": "http://www.loc.gov/zing/srw/",
    "diag": "http://www.loc.gov/zing/srw/diagnostic/",
    "marc": "http://www.loc.gov/MARC21/slim",
}

# Top-level LC Classification classes, used to label the first letter of field 050
LC_CLASSES = {
    "A": "General works", "B": "Philosophy, psychology, religion", "C": "Auxiliary sciences of history",
    "D": "World history (except Americas)", "E": "History of the Americas (US)", "F": "History of the Americas (local, Latin America)",
    "G": "Geography, anthropology, recreation", "H": "Social sciences", "J": "Political science", "K": "Law",
    "L": "Education", "M": "Music", "N": "Fine arts", "P": "Language and literature", "Q": "Science",
    "R": "Medicine", "S": "Agriculture", "T": "Technology", "U": "Military science", "V": "Naval science",
    "Z": "Bibliography, library science",
}


class SruError(Exception):
    """Raised on SRU diagnostics, blocks or unexpected responses."""


class SruClient:
    def __init__(self, max_requests: int):
        self.max_requests = max_requests
        self.requests_made = 0
        self.last_request_at = 0.0

    def search(self, query: str, start: int = 1, size: int = 0, schema: str = "marcxml") -> ET.Element:
        if self.requests_made >= self.max_requests:
            raise SruError(f"Request budget exhausted ({self.max_requests}). Raise --max-requests if needed.")

        wait = MIN_SECONDS_BETWEEN_REQUESTS - (time.monotonic() - self.last_request_at)
        if wait > 0:
            time.sleep(wait)

        params = {
            "version": "1.1",
            "operation": "searchRetrieve",
            "query": query,
            "startRecord": start,
            "maximumRecords": size,
            "recordSchema": schema,
        }
        url = BASE_URL + "?" + urllib.parse.urlencode(params)
        print(f"[{self.requests_made + 1}/{self.max_requests}] {query} (start={start}, size={size})", file=sys.stderr)
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        self.last_request_at = time.monotonic()
        self.requests_made += 1

        for attempt in range(TRANSIENT_RETRIES + 1):
            try:
                with urllib.request.urlopen(request, timeout=90) as response:
                    content_type = response.headers.get("Content-Type", "")
                    body = response.read()
                break
            except urllib.error.HTTPError as e:
                if e.code < 500 or attempt == TRANSIENT_RETRIES:
                    raise SruError(f"HTTP {e.code} for {url}") from e
                error = f"HTTP {e.code}"
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt == TRANSIENT_RETRIES:
                    raise SruError(f"Network error for {url}: {e}") from e
                error = repr(e)
            backoff = 10 * 2**attempt
            print(f"  transient error ({error}), retrying in {backoff}s", file=sys.stderr)
            time.sleep(backoff)

        if "xml" not in content_type:
            raise SruError(f"Expected XML, got {content_type!r}: {body[:120]!r}")

        root = ET.fromstring(body)
        diagnostic = root.find(".//diag:message", NS)
        if diagnostic is not None:
            raise SruError(f"SRU diagnostic for {query!r}: {diagnostic.text}")
        return root


def build_query(terms: list[str], year: int | None = None) -> str:
    subject = " or ".join(f'dc.subject="{t}"' for t in terms)
    if len(terms) > 1:
        subject = f"({subject})"
    return f"{subject} and dc.date={year}" if year is not None else subject


def number_of_records(root: ET.Element) -> int:
    node = root.find("zs:numberOfRecords", NS)
    return int(node.text) if node is not None else 0


def flatten_marc(record: ET.Element) -> dict:
    """Turn one MARCXML record into a flat dict with the fields useful for the analysis."""
    control = {cf.get("tag"): cf.text or "" for cf in record.findall("marc:controlfield", NS)}
    fixed = control.get("008", "").ljust(40)

    def subfields(tag: str, codes: str | None = None) -> list[str]:
        values = []
        for field in record.findall(f"marc:datafield[@tag='{tag}']", NS):
            parts = [sf.text or "" for sf in field.findall("marc:subfield", NS) if codes is None or sf.get("code") in codes]
            if parts:
                values.append(" -- ".join(p.strip(" .,:;/") for p in parts))
        return values

    lc_call = subfields("050", "a")
    lc_letter = lc_call[0][:1].upper() if lc_call else ""
    return {
        "id": control.get("001"),
        "record_modified": control.get("005"),
        "pub_year": fixed[7:11].strip(),
        "pub_country": fixed[15:18].strip(),
        "language": fixed[35:38].strip(),
        "geo_areas": [
            (sf.text or "").strip("-")
            for sf in record.findall("marc:datafield[@tag='043']/marc:subfield[@code='a']", NS)
        ],
        "lc_call_number": lc_call[0] if lc_call else None,
        "lc_class": lc_letter,
        "lc_class_label": LC_CLASSES.get(lc_letter),
        "author": (subfields("100", "a") or subfields("110", "a") or [None])[0],
        "title": (subfields("245", "ab") or [None])[0],
        "publication": (subfields("260", "abc") or subfields("264", "abc") or [None])[0],
        "subjects_topical": subfields("650"),
        "subjects_geographic": subfields("651"),
    }


def cmd_counts(client: SruClient, args: argparse.Namespace) -> None:
    out_path = args.out_dir / f"counts_{'_'.join(args.terms)}_{args.from_year}_{args.to_year}.csv"
    rows = []
    total = number_of_records(client.search(build_query(args.terms)))
    print(f"\nAll years: {total} records for subject in {args.terms}\n")
    for year in range(args.from_year, args.to_year + 1, args.step):
        count = number_of_records(client.search(build_query(args.terms, year)))
        rows.append({"year": year, "records": count})
        print(f"  {year}: {count:>6} {'#' * min(count // max(args.bar_scale, 1), 80)}")

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["year", "records"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nSaved {out_path}")


def fetch_page_with_retry(client: SruClient, query: str, start: int, size: int, attempts: int = 4) -> list[ET.Element]:
    """Fetch one page of MARC records, shrinking the page on the server's intermittent error."""
    for attempt in range(attempts):
        try:
            return client.search(query, start=start, size=size).findall(".//marc:record", NS)
        except SruError as e:
            if FLAKY_DIAGNOSTIC not in str(e) or attempt == attempts - 1:
                raise
            size = max(size * 2 // 3, 1)
            print(f"  flaky page, retrying with size={size}", file=sys.stderr)
    return []


def cmd_sample(client: SruClient, args: argparse.Namespace) -> None:
    query = build_query(args.terms, args.year)
    total = number_of_records(client.search(query))
    records, start = [], 1
    while len(records) < min(args.limit, total):
        size = min(MAX_PAGE_SIZE, args.limit - len(records), total - start + 1)
        page = fetch_page_with_retry(client, query, start, size)
        if not page:
            break
        records.extend(flatten_marc(r) for r in page)
        start += len(page)

    out_path = args.out_dir / f"sample_{'_'.join(args.terms)}_{args.year}.jsonl"
    with out_path.open("w") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"\n{len(records)} of {total} records for {query!r}\n")
    summaries = {
        "LC class (topic)": Counter(f"{r['lc_class'] or '?'} {r['lc_class_label'] or ''}" for r in records),
        "Publication country (008)": Counter(r["pub_country"] or "?" for r in records),
        "Language (008)": Counter(r["language"] or "?" for r in records),
        "Geographic area (043)": Counter(g for r in records for g in r["geo_areas"] or ["(none)"]),
        "Topical subject, main term (650$a)": Counter(s.split(" -- ")[0] for r in records for s in r["subjects_topical"]),
    }
    for name, counter in summaries.items():
        print(f"{name}:")
        for value, count in counter.most_common(args.top):
            print(f"  {count:>5}  {value}")
        print()
    print(f"Saved {out_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-requests", type=int, default=60)
    parser.add_argument("--out-dir", type=Path, default=Path(__file__).parent / "sru_out")
    sub = parser.add_subparsers(dest="command", required=True)

    counts = sub.add_parser("counts", help="records per publication year")
    counts.add_argument("--terms", nargs="+", required=True, help='subject terms OR-ed together, e.g. Argentina Argentine')
    counts.add_argument("--from-year", type=int, required=True)
    counts.add_argument("--to-year", type=int, required=True)
    counts.add_argument("--step", type=int, default=1)
    counts.add_argument("--bar-scale", type=int, default=20, help="records per # in the bar chart")

    sample = sub.add_parser("sample", help="download and summarize MARC records for one year")
    sample.add_argument("--terms", nargs="+", required=True)
    sample.add_argument("--year", type=int, required=True)
    sample.add_argument("--limit", type=int, default=250)
    sample.add_argument("--top", type=int, default=10)

    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    client = SruClient(args.max_requests)
    try:
        {"counts": cmd_counts, "sample": cmd_sample}[args.command](client, args)
    except SruError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        return 2
    print(f"{client.requests_made} requests made.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
