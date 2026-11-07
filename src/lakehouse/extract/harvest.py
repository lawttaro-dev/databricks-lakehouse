"""Harvest SRU records into landing storage, one (country, year) partition at a time.

Landing layout, read later by Auto Loader:

    <volume>/batch=<batch>/country=<CC>/year=<YYYY>/page-<start>.xml
    <volume>/batch=<batch>/country=<CC>/year=<YYYY>/_SUCCESS

A partition is complete only when its _SUCCESS marker exists. Runs skip complete partitions,
so a long backfill can stop at any point (request budget, CI timeout, Ctrl+C) and resume by
running again with the same batch. A partition that fails is reported and skipped; the run goes
on with the others.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from lakehouse.extract.sru import BudgetExhaustedError, SruClient, SruError, build_query
from lakehouse.extract.storage import LandingStorage


@dataclass(frozen=True)
class Partition:
    country: str
    terms: tuple[str, ...]
    year: int

    def prefix(self, batch: str) -> str:
        return f"batch={batch}/country={self.country}/year={self.year}"


@dataclass
class HarvestReport:
    completed: list[Partition] = field(default_factory=list)
    skipped: list[Partition] = field(default_factory=list)
    pending: list[Partition] = field(default_factory=list)
    failed: list[Partition] = field(default_factory=list)
    records: int = 0


def plan_partitions(countries: dict[str, tuple[str, ...]], years: range) -> list[Partition]:
    """Most recent years first, so a partial run lands the most useful data."""
    return [
        Partition(country, terms, year)
        for year in reversed(years)
        for country, terms in countries.items()
    ]


def harvest(
    client: SruClient,
    storage: LandingStorage,
    partitions: list[Partition],
    batch: str,
    page_size: int,
    log: Callable[[str], None] = print,
) -> HarvestReport:
    report = HarvestReport()
    for index, partition in enumerate(partitions):
        if storage.exists(f"{partition.prefix(batch)}/_SUCCESS"):
            report.skipped.append(partition)
            continue
        try:
            records = harvest_partition(client, storage, partition, batch, page_size)
        except BudgetExhaustedError:
            report.pending = partitions[index:]
            log(f"request budget spent; {len(report.pending)} partitions pending")
            break
        except SruError as e:
            # No _SUCCESS is written, so the next run retries this partition
            report.failed.append(partition)
            log(f"{partition.country} {partition.year}: FAILED, will retry next run ({e})")
            continue
        report.completed.append(partition)
        report.records += records
        log(f"{partition.country} {partition.year}: {records} records")
    return report


def harvest_partition(
    client: SruClient, storage: LandingStorage, partition: Partition, batch: str, page_size: int
) -> int:
    query = build_query(partition.terms, partition.year)
    total = client.search(query).total_records
    prefix = partition.prefix(batch)

    start = 1
    while start <= total:
        page = client.fetch_page(query, start, min(page_size, total - start + 1))
        if page.records_in_page == 0:
            break
        storage.write(f"{prefix}/page-{start:06d}.xml", page.raw)
        start += page.records_in_page

    marker = {"query": query, "total_records": total, "harvested_at": datetime.now(UTC).isoformat()}
    storage.write(f"{prefix}/_SUCCESS", json.dumps(marker).encode())
    return total
