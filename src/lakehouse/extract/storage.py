"""Where landed files go: a Unity Catalog Volume, or a local folder for tests and dry runs."""

import io
from pathlib import Path
from typing import Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


class LandingStorage(Protocol):
    def exists(self, path: str) -> bool: ...

    def write(self, path: str, data: bytes) -> None: ...


class LocalStorage:
    def __init__(self, root: Path):
        self.root = root

    def exists(self, path: str) -> bool:
        return (self.root / path).exists()

    def write(self, path: str, data: bytes) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)


class VolumeStorage:
    """Files in a Unity Catalog Volume, through the workspace Files API."""

    def __init__(self, client: WorkspaceClient, volume_path: str):
        self.client = client
        self.volume_path = volume_path.rstrip("/")

    def exists(self, path: str) -> bool:
        try:
            self.client.files.get_metadata(f"{self.volume_path}/{path}")
            return True
        except NotFound:
            return False

    def write(self, path: str, data: bytes) -> None:
        self.client.files.upload(f"{self.volume_path}/{path}", io.BytesIO(data), overwrite=True)
