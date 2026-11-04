"""In-memory SRU server for tests: answers searchRetrieve URLs like lx2.loc.gov does."""

import urllib.parse

from lakehouse.extract.sru import TransientError

RESPONSE = """<?xml version="1.0"?>
<zs:searchRetrieveResponse xmlns:zs="http://www.loc.gov/zing/srw/">
  <zs:numberOfRecords>{total}</zs:numberOfRecords>
  <zs:records>{records}</zs:records>
</zs:searchRetrieveResponse>"""

RECORD = """<zs:record><zs:recordData>
  <record xmlns="http://www.loc.gov/MARC21/slim">
    <controlfield tag="001">{id}</controlfield>
  </record>
</zs:recordData></zs:record>"""

DIAGNOSTIC = """<?xml version="1.0"?>
<zs:searchRetrieveResponse xmlns:zs="http://www.loc.gov/zing/srw/">
  <zs:diagnostics><diag:diagnostic xmlns:diag="http://www.loc.gov/zing/srw/diagnostic/">
    <diag:message>{message}</diag:message>
  </diag:diagnostic></zs:diagnostics>
</zs:searchRetrieveResponse>"""


class FakeSruServer:
    """Callable transport. `totals` maps a CQL query to its number of records."""

    def __init__(self, totals: dict[str, int]):
        self.totals = totals
        self.calls: list[dict[str, str]] = []
        self.failures: list[Exception | str] = []  # consumed one per call, before answering

    def __call__(self, url: str) -> tuple[str, bytes]:
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        self.calls.append(params)
        if self.failures:
            failure = self.failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return "text/xml", DIAGNOSTIC.format(message=failure).encode()

        total = self.totals.get(params["query"], 0)
        start, size = int(params["startRecord"]), int(params["maximumRecords"])
        ids = range(start, min(start + size, total + 1))
        records = "".join(RECORD.format(id=i) for i in ids)
        return "text/xml", RESPONSE.format(total=total, records=records).encode()


def transient(message: str = "connection reset") -> TransientError:
    return TransientError(message)
