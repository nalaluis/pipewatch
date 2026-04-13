"""Compactor: prune and compact historical snapshot/audit data older than a retention window."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import List

from pipewatch.snapshot import _snapshot_path, list_snapshots


@dataclass
class CompactorConfig:
    retention_seconds: float = 86400.0  # 24 hours
    dry_run: bool = False


@dataclass
class CompactionResult:
    removed: List[str] = field(default_factory=list)
    kept: List[str] = field(default_factory=list)
    dry_run: bool = False

    @property
    def removed_count(self) -> int:
        return len(self.removed)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    def __str__(self) -> str:
        prefix = "[dry-run] " if self.dry_run else ""
        return (
            f"{prefix}Compaction complete: "
            f"{self.removed_count} removed, {self.kept_count} kept"
        )


def compact_snapshots(
    cfg: CompactorConfig,
    base_dir: str = ".pipewatch_snapshots",
) -> CompactionResult:
    """Remove snapshot files older than retention_seconds."""
    result = CompactionResult(dry_run=cfg.dry_run)
    now = time.time()
    cutoff = now - cfg.retention_seconds

    names = list_snapshots(base_dir=base_dir)
    for name in names:
        path = _snapshot_path(name, base_dir=base_dir)
        try:
            mtime = os.path.getmtime(path)
        except FileNotFoundError:
            continue
        if mtime < cutoff:
            result.removed.append(name)
            if not cfg.dry_run:
                os.remove(path)
        else:
            result.kept.append(name)
    return result


def format_compaction(result: CompactionResult) -> str:
    lines = [str(result)]
    if result.removed:
        lines.append("  Removed:")
        for name in result.removed:
            lines.append(f"    - {name}")
    if result.kept:
        lines.append("  Kept:")
        for name in result.kept:
            lines.append(f"    - {name}")
    return "\n".join(lines)
