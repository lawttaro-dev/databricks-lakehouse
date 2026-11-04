"""Source configuration, loaded from sources/<source>.yml."""

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class SruSourceConfig:
    source: str
    endpoint: str
    record_schema: str
    volume: str  # "<schema>.<volume>" inside the environment catalog
    throttle_seconds: float
    page_size: int
    backfill_from_year: int
    window_years: int
    countries: dict[str, tuple[str, ...]]

    @classmethod
    def load(cls, path: Path) -> "SruSourceConfig":
        raw = yaml.safe_load(path.read_text())
        if raw.get("protocol") != "sru":
            raise ValueError(f"{path}: expected protocol 'sru', got {raw.get('protocol')!r}")
        return cls(
            source=raw["source"],
            endpoint=raw["endpoint"],
            record_schema=raw["record_schema"],
            volume=raw["volume"],
            throttle_seconds=float(raw["throttle_seconds"]),
            page_size=int(raw["page_size"]),
            backfill_from_year=int(raw["backfill"]["from_year"]),
            window_years=int(raw["window"]["years"]),
            countries={code: tuple(terms) for code, terms in raw["countries"].items()},
        )

    def volume_path(self, catalog: str) -> str:
        schema, volume = self.volume.split(".")
        return f"/Volumes/{catalog}/{schema}/{volume}"
