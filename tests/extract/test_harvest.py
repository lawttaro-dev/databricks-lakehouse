import json
from pathlib import Path

from lakehouse.extract.config import SruSourceConfig
from lakehouse.extract.harvest import Partition, harvest, plan_partitions
from lakehouse.extract.sru import SruClient, build_query
from lakehouse.extract.storage import LocalStorage
from tests.extract.fake_sru import FakeSruServer

AR = ("Argentina", "Argentine")
CL = ("Chile", "Chilean")


def make_client(server, max_requests=100):
    return SruClient(
        "http://sru.test/db", "marcxml", 0, max_requests, transport=server, sleep=lambda _: None
    )


def test_plan_partitions_newest_year_first():
    partitions = plan_partitions({"AR": AR, "CL": CL}, range(2023, 2025))

    assert [(p.country, p.year) for p in partitions] == [
        ("AR", 2024),
        ("CL", 2024),
        ("AR", 2023),
        ("CL", 2023),
    ]


def test_harvest_writes_pages_and_success_marker(tmp_path):
    server = FakeSruServer({build_query(AR, 1950): 250})
    storage = LocalStorage(tmp_path)

    report = harvest(make_client(server), storage, [Partition("AR", AR, 1950)], "b1", page_size=100)

    folder = tmp_path / "batch=b1/country=AR/year=1950"
    assert sorted(p.name for p in folder.iterdir()) == [
        "_SUCCESS",
        "page-000001.xml",
        "page-000101.xml",
        "page-000201.xml",
    ]
    assert json.loads((folder / "_SUCCESS").read_text())["total_records"] == 250
    assert report.records == 250
    assert len(server.calls) == 4  # 1 count + 3 pages


def test_empty_partition_is_marked_complete(tmp_path):
    report = harvest(
        make_client(FakeSruServer({})),
        LocalStorage(tmp_path),
        [Partition("CL", CL, 1901)],
        "b1",
        page_size=100,
    )

    assert (tmp_path / "batch=b1/country=CL/year=1901/_SUCCESS").exists()
    assert report.completed == [Partition("CL", CL, 1901)]


def test_completed_partitions_are_skipped_on_rerun(tmp_path):
    server = FakeSruServer({build_query(AR, 1950): 10})
    storage = LocalStorage(tmp_path)
    partitions = [Partition("AR", AR, 1950)]
    harvest(make_client(server), storage, partitions, "b1", page_size=100)
    calls_after_first_run = len(server.calls)

    report = harvest(make_client(server), storage, partitions, "b1", page_size=100)

    assert report.skipped == partitions
    assert len(server.calls) == calls_after_first_run


def test_budget_exhaustion_leaves_partition_pending_and_resumable(tmp_path):
    server = FakeSruServer({build_query(AR, 1950): 250, build_query(CL, 1950): 5})
    storage = LocalStorage(tmp_path)
    partitions = [Partition("AR", AR, 1950), Partition("CL", CL, 1950)]

    first = harvest(make_client(server, max_requests=2), storage, partitions, "b1", page_size=100)

    assert first.pending == partitions
    assert not (tmp_path / "batch=b1/country=AR/year=1950/_SUCCESS").exists()

    second = harvest(make_client(server), storage, partitions, "b1", page_size=100)

    assert second.completed == partitions
    assert second.records == 255


def test_source_config_loads_repo_yaml():
    config = SruSourceConfig.load(Path("sources/loc_sru.yml"))

    assert config.countries["AR"] == AR
    assert config.page_size == 100
    assert config.volume_path("dev") == "/Volumes/dev/landing/loc_sru"
