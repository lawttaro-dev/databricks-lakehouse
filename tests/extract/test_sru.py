import pytest

from lakehouse.extract.sru import (
    FLAKY_DIAGNOSTIC,
    BudgetExhaustedError,
    SruClient,
    SruError,
    TransientError,
    build_query,
)
from tests.extract.fake_sru import FakeSruServer, transient

QUERY = '(dc.subject="Argentina" or dc.subject="Argentine") and dc.date=1950'


def make_client(server, max_requests=100, sleeps=None):
    return SruClient(
        endpoint="http://sru.test/db",
        record_schema="marcxml",
        throttle_seconds=3,
        max_requests=max_requests,
        transport=server,
        sleep=(sleeps.append if sleeps is not None else lambda _: None),
        clock=lambda: 0.0,
    )


def test_build_query_ors_terms_and_filters_year():
    assert build_query(("Argentina", "Argentine"), 1950) == QUERY
    assert build_query(("Chile",), 2000) == 'dc.subject="Chile" and dc.date=2000'


def test_search_returns_raw_body_and_counts():
    client = make_client(FakeSruServer({QUERY: 250}))

    page = client.search(QUERY, start=1, size=100)

    assert page.total_records == 250
    assert page.records_in_page == 100
    assert page.raw.startswith(b"<?xml")


def test_diagnostic_in_http_200_raises():
    server = FakeSruServer({})
    server.failures.append("Unsupported index")

    with pytest.raises(SruError, match="Unsupported index"):
        make_client(server).search(QUERY)


def test_transient_errors_are_retried_with_exponential_backoff():
    server = FakeSruServer({QUERY: 5})
    server.failures += [transient(), transient()]
    sleeps = []

    page = make_client(server, sleeps=sleeps).search(QUERY, size=5)

    assert page.records_in_page == 5
    backoffs = [s for s in sleeps if s >= 10]
    assert backoffs == [10, 20]


def test_transient_errors_give_up_after_retries():
    server = FakeSruServer({})
    server.failures += [transient()] * 4

    with pytest.raises(TransientError):
        make_client(server).search(QUERY)


def test_throttle_waits_between_requests():
    sleeps = []
    client = make_client(FakeSruServer({}), sleeps=sleeps)

    client.search(QUERY)
    client.search(QUERY)

    assert sleeps == [3]  # the fake clock never advances, so the full throttle applies


def test_budget_is_enforced():
    client = make_client(FakeSruServer({}), max_requests=1)
    client.search(QUERY)

    with pytest.raises(BudgetExhaustedError):
        client.search(QUERY)


def test_fetch_page_shrinks_page_on_flaky_diagnostic():
    server = FakeSruServer({QUERY: 250})
    server.failures.append(FLAKY_DIAGNOSTIC)

    page = make_client(server).fetch_page(QUERY, start=1, size=100)

    assert [c["maximumRecords"] for c in server.calls] == ["100", "66"]
    assert page.records_in_page == 66
