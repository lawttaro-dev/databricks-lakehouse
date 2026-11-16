"""Command line entry point: lakehouse-extract.

Examples:
    # Local dry run into a folder, a few requests
    uv run lakehouse-extract --mode window --window-years 1 --countries AR \\
        --local-dir /tmp/landing --max-requests 10

    # Daily rolling window into the dev Volume
    uv run lakehouse-extract --mode window --catalog dev

    # Full-history backfill, resumable: run again with the same arguments to continue
    uv run lakehouse-extract --mode backfill --catalog dev --max-requests 1500

Exit code 0 also when partitions failed, the server went down or the budget ran out: the next
run resumes (details in the output and, in GitHub Actions, the step summary). Non-zero means
something to fix (configuration, credentials, a bug).
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path

from lakehouse.extract.config import SruSourceConfig
from lakehouse.extract.harvest import HarvestReport, Partition, harvest, plan_partitions
from lakehouse.extract.sru import SruClient
from lakehouse.extract.storage import LandingStorage, LocalStorage, VolumeStorage

DEFAULT_SOURCE = Path("sources/loc_sru.yml")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Harvest a source into its landing Volume.",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--mode", choices=["backfill", "window"], required=True)
    parser.add_argument("--from-year", type=int, help="backfill start (default: from config)")
    parser.add_argument("--to-year", type=int, help="last year (default: current year)")
    parser.add_argument("--window-years", type=int, help="window size (default: from config)")
    parser.add_argument("--countries", nargs="+", help="subset of country codes from config")
    parser.add_argument("--batch", help="landing batch name; reuse it to resume a backfill")
    parser.add_argument("--max-requests", type=int, default=300)

    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--catalog", help="environment catalog: dev, qa or prod")
    target.add_argument("--local-dir", type=Path, help="write to a local folder instead")
    parser.add_argument("--profile", help="Databricks CLI profile (with --catalog)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    # Show progress as it happens when output goes to a file (nohup ... > backfill.log)
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args(argv)
    config = SruSourceConfig.load(args.source)

    to_year = args.to_year or date.today().year
    if args.mode == "backfill":
        from_year = args.from_year or config.backfill_from_year
        batch = args.batch or f"backfill-{from_year}-{to_year}"
    else:
        from_year = to_year - (args.window_years or config.window_years) + 1
        batch = args.batch or f"window-{date.today().isoformat()}"

    countries = config.countries
    if args.countries:
        unknown = set(args.countries) - set(countries)
        if unknown:
            sys.exit(f"Unknown countries {sorted(unknown)}; configured: {sorted(countries)}")
        countries = {code: countries[code] for code in args.countries}

    storage: LandingStorage
    if args.local_dir:
        storage = LocalStorage(args.local_dir)
    else:
        from databricks.sdk import WorkspaceClient

        storage = VolumeStorage(
            WorkspaceClient(profile=args.profile), config.volume_path(args.catalog)
        )

    client = SruClient(
        config.endpoint, config.record_schema, config.throttle_seconds, args.max_requests
    )
    partitions = plan_partitions(countries, range(from_year, to_year + 1))
    print(f"{config.source} {args.mode} {from_year}-{to_year} batch={batch}")
    print(f"{len(partitions)} partitions, budget {args.max_requests} requests")

    report = harvest(client, storage, partitions, batch, config.page_size)
    print(
        f"done: {len(report.completed)} completed, {len(report.skipped)} skipped, "
        f"{len(report.pending)} pending, {len(report.failed)} failed, {report.records} records, "
        f"{client.requests_made} requests"
    )
    if report.failed:
        failed = ", ".join(f"{p.country} {p.year}" for p in report.failed)
        print(f"failed partitions (next run retries them): {failed}")

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as out:
            target = args.catalog or str(args.local_dir)
            out.write(step_summary(f"{config.source} {args.mode} → {target}", report))
    # Source flakiness (failed partitions, server down, budget spent) is expected and resumes on
    # the next run, so it is not a failure. Anything else raises and exits non-zero.
    return 0


def step_summary(title: str, report: HarvestReport) -> str:
    """Markdown for the GitHub Actions run page."""

    def names(partitions: list[Partition]) -> str:
        return ", ".join(f"{p.country} {p.year}" for p in partitions) or "-"

    lines = [
        f"### {title}",
        "",
        "| Completed | Skipped | Pending | Failed | Records |",
        "|---|---|---|---|---|",
        f"| {len(report.completed)} | {len(report.skipped)} | {len(report.pending)} "
        f"| {len(report.failed)} | {report.records} |",
        "",
    ]
    if report.stopped:
        lines.append(f"Stopped early: {report.stopped}. The next run resumes.")
    if report.failed:
        lines.append(f"Failed (retried next run): {names(report.failed)}")
    return "\n".join(lines) + "\n\n"


if __name__ == "__main__":
    sys.exit(main())
