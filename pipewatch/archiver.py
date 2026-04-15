"""Archive old audit/snapshot records to compressed files for long-term storage."""
from __future__ import annotations

import gzip
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List


@dataclass
class ArchiveConfig:
    archive_dir: str = ".pipewatch/archive"
    source_dir: str = ".pipewatch/snapshots"
    max_age_days: int = 30
    compress: bool = True


@dataclass
class ArchiveResult:
    archived: int
    skipped: int
    archive_path: str

    def __str__(self) -> str:
        return (
            f"Archived {self.archived} file(s), skipped {self.skipped} "
            f"-> {self.archive_path}"
        )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _age_days(path: Path) -> float:
    mtime = path.stat().st_mtime
    age = _utcnow().timestamp() - mtime
    return age / 86400.0


def archive_old_files(config: ArchiveConfig) -> ArchiveResult:
    """Move files older than max_age_days from source_dir into a compressed archive."""
    source = Path(config.source_dir)
    archive_dir = Path(config.archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)

    if not source.exists():
        return ArchiveResult(archived=0, skipped=0, archive_path=str(archive_dir))

    timestamp = _utcnow().strftime("%Y%m%dT%H%M%SZ")
    bundle_name = f"archive_{timestamp}.jsonl"
    if config.compress:
        bundle_name += ".gz"
    bundle_path = archive_dir / bundle_name

    archived = 0
    skipped = 0
    candidates: List[Path] = sorted(source.glob("*.json"))

    open_fn = gzip.open if config.compress else open
    mode = "wt" if config.compress else "w"

    with open_fn(bundle_path, mode) as fh:  # type: ignore[call-overload]
        for fpath in candidates:
            if _age_days(fpath) >= config.max_age_days:
                data = fpath.read_text(encoding="utf-8")
                fh.write(data.rstrip("\n") + "\n")
                fpath.unlink()
                archived += 1
            else:
                skipped += 1

    if archived == 0:
        bundle_path.unlink(missing_ok=True)

    return ArchiveResult(archived=archived, skipped=skipped, archive_path=str(bundle_path))


def list_archives(config: ArchiveConfig) -> List[str]:
    """Return sorted list of archive bundle filenames."""
    archive_dir = Path(config.archive_dir)
    if not archive_dir.exists():
        return []
    return sorted(p.name for p in archive_dir.glob("archive_*"))
