#!/usr/bin/env python3
"""Probe access to the Handbook of Latin American Studies (HLAS) collection via the loc.gov JSON API.

API docs: https://www.loc.gov/apis/json-and-yaml/
Rate limit for the JSON API: 20 requests/minute; exceeding it blocks your IP for 1 hour.
This script throttles itself, caps the total number of requests and stops on the first
429 / CAPTCHA / Cloudflare challenge. Stdlib only, no installs needed.

Usage:
    python3 hlas_probe.py                      # overview: total items, facets, sample records
    python3 hlas_probe.py --country brazil     # also count items per decade for one country
    python3 hlas_probe.py --max-requests 5     # tighter request budget
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_URL = "https://www.loc.gov/collections/handbook-of-latin-american-studies/"
USER_AGENT = "hlas-probe/0.1 (personal data exploration)"
MIN_SECONDS_BETWEEN_REQUESTS = 4.0  # ~15 req/min, below the documented 20/min


class BlockedError(Exception):
    """Raised when loc.gov answers with a rate-limit, CAPTCHA or bot challenge."""


class LocClient:
    def __init__(self, max_requests: int, out_dir: Path):
        self.max_requests = max_requests
        self.requests_made = 0
        self.last_request_at = 0.0
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def get(self, params: dict, save_as: str) -> dict:
        if self.requests_made >= self.max_requests:
            raise RuntimeError(f"Request budget exhausted ({self.max_requests}). Raise --max-requests if needed.")

        wait = MIN_SECONDS_BETWEEN_REQUESTS - (time.monotonic() - self.last_request_at)
        if wait > 0:
            time.sleep(wait)

        url = BASE_URL + "?" + urllib.parse.urlencode({"fo": "json", **params})
        print(f"[{self.requests_made + 1}/{self.max_requests}] GET {url}")
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        self.last_request_at = time.monotonic()
        self.requests_made += 1

        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                content_type = response.headers.get("Content-Type", "")
                body = response.read()
        except urllib.error.HTTPError as e:
            snippet = e.read(300).decode("utf-8", "replace")
            if e.code in (403, 429) or "Just a moment" in snippet:
                raise BlockedError(f"HTTP {e.code}: {snippet[:120]!r}") from e
            raise

        if "json" not in content_type:
            # loc.gov serves an HTML CAPTCHA page under load instead of a 429
            raise BlockedError(f"Expected JSON, got {content_type!r}: {body[:120]!r}")

        data = json.loads(body)
        (self.out_dir / f"{save_as}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))
        return data


def print_overview(data: dict) -> None:
    pagination = data.get("pagination", {})
    print(f"\nTotal items in collection: {pagination.get('of')}  (pages of {pagination.get('perpage')}: {pagination.get('total')})")

    print("\nTop facet values:")
    for facet in data.get("facets", []):
        values = ", ".join(f"{f['title']} ({f['count']})" for f in facet.get("filters", [])[:8])
        print(f"  {facet.get('type')}: {values}")

    print("\nSample records:")
    for item in data.get("results", []):
        print("  ---")
        for field in ("id", "title", "date", "location", "subject", "language", "original_format", "partof"):
            value = item.get(field)
            if isinstance(value, list):
                value = value[:6]
            print(f"  {field:16}: {value}")


def count_by_decade(client: LocClient, country: str, start: int, end: int) -> None:
    print(f"\nItems per decade for location={country!r}:")
    for decade in range(start, end, 10):
        data = client.get(
            {"fa": f"location:{country}", "dates": f"{decade}/{decade + 9}", "c": 1, "at": "pagination"},
            save_as=f"count_{country}_{decade}s",
        )
        print(f"  {decade}s: {data.get('pagination', {}).get('of', 0)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--country", help="location facet value to count per decade, e.g. brazil, mexico, argentina")
    parser.add_argument("--from-decade", type=int, default=1930)
    parser.add_argument("--to-decade", type=int, default=2030)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--max-requests", type=int, default=15)
    parser.add_argument("--out-dir", type=Path, default=Path(__file__).parent / "hlas_raw")
    args = parser.parse_args()

    client = LocClient(args.max_requests, args.out_dir)
    try:
        overview = client.get({"c": args.sample_size, "at": "pagination,facets,results"}, save_as="overview")
        print_overview(overview)
        if args.country:
            count_by_decade(client, args.country, args.from_decade, args.to_decade)
    except BlockedError as e:
        print(f"\nBLOCKED by loc.gov: {e}")
        print("Do not retry right away: wait at least 1 hour. If it persists, it is bot protection rather than the rate limit.")
        return 2

    print(f"\nDone. {client.requests_made} requests made. Raw responses saved to {args.out_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
