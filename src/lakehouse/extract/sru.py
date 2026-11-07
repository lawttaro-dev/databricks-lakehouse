"""Client for SRU (Search/Retrieve via URL), the search protocol used by library catalogs.

Server quirks handled here (observed on lx2.loc.gov, see docs/02-data-source-loc-catalog.md):
  - errors come as HTTP 200 with an SRU diagnostic in the XML body
  - intermittent "First record position out of range" on valid pages: retry with a smaller page
  - intermittent disconnects: retry with exponential backoff
  - undocumented rate limit: throttle every request and cap requests per run
"""

import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass

NS = {
    "zs": "http://www.loc.gov/zing/srw/",
    "diag": "http://www.loc.gov/zing/srw/diagnostic/",
    "marc": "http://www.loc.gov/MARC21/slim",
}
USER_AGENT = "lakehouse-extract/0.1 (bibliographic catalog research)"
FLAKY_DIAGNOSTIC = "First record position out of range"


class SruError(Exception):
    """SRU diagnostic, permanent HTTP error or unexpected response."""


class BudgetExhaustedError(SruError):
    """The per-run request budget is spent. Not a failure: the next run resumes."""


class TransientError(Exception):
    """Network error or HTTP 5xx: worth retrying."""


@dataclass(frozen=True)
class SruPage:
    raw: bytes  # the XML response exactly as received
    total_records: int
    records_in_page: int


Transport = Callable[[str], tuple[str, bytes]]  # url -> (content type, body)


def http_get(url: str) -> tuple[str, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return response.headers.get("Content-Type", ""), response.read()
    except urllib.error.HTTPError as e:
        if e.code >= 500:
            raise TransientError(f"HTTP {e.code}") from e
        raise SruError(f"HTTP {e.code} for {url}") from e
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        raise TransientError(repr(e)) from e


def build_query(terms: tuple[str, ...], year: int) -> str:
    """CQL query for records about any of the subject terms, published in `year`."""
    subject = " or ".join(f'dc.subject="{term}"' for term in terms)
    if len(terms) > 1:
        subject = f"({subject})"
    return f"{subject} and dc.date={year}"


class SruClient:
    def __init__(
        self,
        endpoint: str,
        record_schema: str,
        throttle_seconds: float,
        max_requests: int,
        transport: Transport = http_get,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        retries: int = 3,
        backoff_seconds: float = 10,
    ):
        self.endpoint = endpoint
        self.record_schema = record_schema
        self.throttle_seconds = throttle_seconds
        self.max_requests = max_requests
        self.requests_made = 0
        self._transport = transport
        self._sleep = sleep
        self._clock = clock
        self._retries = retries
        self._backoff_seconds = backoff_seconds
        self._last_request_at: float | None = None

    def search(self, query: str, start: int = 1, size: int = 0) -> SruPage:
        """One searchRetrieve request. size=0 returns only the total count."""
        url = (
            self.endpoint
            + "?"
            + urllib.parse.urlencode(
                {
                    "version": "1.1",
                    "operation": "searchRetrieve",
                    "query": query,
                    "startRecord": start,
                    "maximumRecords": size,
                    "recordSchema": self.record_schema,
                }
            )
        )
        content_type, body = self._get_with_retries(url)
        if "xml" not in content_type:
            raise SruError(f"Expected XML, got {content_type!r}: {body[:120]!r}")

        root = ET.fromstring(body)
        diagnostic = root.find(".//diag:message", NS)
        if diagnostic is not None:
            raise SruError(f"SRU diagnostic for {query!r}: {diagnostic.text}")
        total = root.find("zs:numberOfRecords", NS)
        return SruPage(
            raw=body,
            total_records=int(total.text) if total is not None else 0,
            records_in_page=len(root.findall(".//marc:record", NS)),
        )

    def fetch_page(self, query: str, start: int, size: int, attempts: int = 6) -> SruPage:
        """Fetch one page of records, retrying the server's intermittent error.

        Each retry waits longer (10 s, 20 s, ...) and asks for a smaller page. The error can
        repeat several times in a row (2026-11-07: four in a row, then fine).
        """
        for attempt in range(attempts):
            try:
                return self.search(query, start=start, size=size)
            except BudgetExhaustedError:
                raise
            except SruError as e:
                if FLAKY_DIAGNOSTIC not in str(e) or attempt == attempts - 1:
                    raise
                self._sleep(self._backoff_seconds * (attempt + 1))
                size = max(size * 2 // 3, 1)
        raise AssertionError("unreachable")

    def _get_with_retries(self, url: str) -> tuple[str, bytes]:
        for attempt in range(self._retries + 1):
            self._throttle()
            try:
                return self._transport(url)
            except TransientError:
                if attempt == self._retries:
                    raise
                self._sleep(self._backoff_seconds * 2**attempt)
        raise AssertionError("unreachable")

    def _throttle(self) -> None:
        if self.requests_made >= self.max_requests:
            raise BudgetExhaustedError(f"request budget of {self.max_requests} spent")
        if self._last_request_at is not None:
            wait = self.throttle_seconds - (self._clock() - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = self._clock()
        self.requests_made += 1
