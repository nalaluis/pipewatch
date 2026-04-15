"""Load archiver configuration from YAML."""
from __future__ import annotations

from pathlib import Path

import yaml

from pipewatch.archiver import ArchiveConfig


def load_archiver_config(path: str = "pipewatch-archiver.yaml") -> ArchiveConfig:
    """Parse archiver config from *path*, returning defaults if the file is absent."""
    p = Path(path)
    if not p.exists():
        return ArchiveConfig()

    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    section = raw.get("archiver", raw)  # support both top-level and nested

    return ArchiveConfig(
        archive_dir=section.get("archive_dir", ArchiveConfig.archive_dir),
        source_dir=section.get("source_dir", ArchiveConfig.source_dir),
        max_age_days=int(section.get("max_age_days", ArchiveConfig.max_age_days)),
        compress=bool(section.get("compress", ArchiveConfig.compress)),
    )
