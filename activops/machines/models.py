from __future__ import annotations

from typing import TypedDict


class FileChange(TypedDict):
    file: str
    timestamp: str  # ISO "YYYY-mm-dd HH:MM:SS" (ou ISO 8601 selon ta source)


class ProcessInfo(TypedDict):
    tty: str
    cmd: str


class PersistentProcess(TypedDict):
    process: str
    start_time: str  # ISO 8601


class MachineContext(TypedDict):
    hostname: str
    timestamp: str  # ISO 8601 avec TZ
    persistent_apps: list[PersistentProcess]
    modified_files: list[FileChange]
