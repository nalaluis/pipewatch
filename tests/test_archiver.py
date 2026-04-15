"""Tests for pipewatch.archiver and pipewatch.archiver_config."""
from __future__ import annotations

import gzip
import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.archiver import (
    ArchiveConfig,
    ArchiveResult,
    archive_old_files,
    list_archives,
)
from pipewatch.archiver_config import load_archiver_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_json(directory: Path, name: str, age_days: float) -> Path:
    """Write a dummy JSON file and backdate its mtime."""
    directory.mkdir(parents=True, exist_ok=True)
    p = directory / name
    p.write_text(json.dumps({"pipeline": name}), encoding="utf-8")
    past_ts = time.time() - age_days * 86400
    import os
    os.utime(p, (past_ts, past_ts))
    return p


# ---------------------------------------------------------------------------
# archive_old_files
# ---------------------------------------------------------------------------

def test_archive_old_files_returns_zero_when_source_missing(tmp_path):
    cfg = ArchiveConfig(
        archive_dir=str(tmp_path / "archive"),
        source_dir=str(tmp_path / "nonexistent"),
        max_age_days=7,
    )
    result = archive_old_files(cfg)
    assert result.archived == 0
    assert result.skipped == 0


def test_archive_old_files_moves_old_files(tmp_path):
    src = tmp_path / "snapshots"
    arc = tmp_path / "archive"
    _write_json(src, "old.json", age_days=40)
    _write_json(src, "fresh.json", age_days=1)

    cfg = ArchiveConfig(
        archive_dir=str(arc),
        source_dir=str(src),
        max_age_days=30,
        compress=False,
    )
    result = archive_old_files(cfg)

    assert result.archived == 1
    assert result.skipped == 1
    assert not (src / "old.json").exists(), "old file should be removed from source"
    assert (src / "fresh.json").exists(), "fresh file should remain"


def test_archive_old_files_compresses_bundle(tmp_path):
    src = tmp_path / "snapshots"
    arc = tmp_path / "archive"
    _write_json(src, "stale.json", age_days=60)

    cfg = ArchiveConfig(
        archive_dir=str(arc),
        source_dir=str(src),
        max_age_days=30,
        compress=True,
    )
    result = archive_old_files(cfg)

    assert result.archived == 1
    bundle = Path(result.archive_path)
    assert bundle.suffix == ".gz"
    with gzip.open(bundle, "rt") as fh:
        lines = fh.readlines()
    assert len(lines) == 1


def test_archive_old_files_no_bundle_when_nothing_archived(tmp_path):
    src = tmp_path / "snapshots"
    arc = tmp_path / "archive"
    _write_json(src, "fresh.json", age_days=1)

    cfg = ArchiveConfig(
        archive_dir=str(arc),
        source_dir=str(src),
        max_age_days=30,
        compress=False,
    )
    result = archive_old_files(cfg)

    assert result.archived == 0
    assert not list(arc.glob("archive_*")), "no bundle should be created"


def test_archive_result_str():
    r = ArchiveResult(archived=3, skipped=1, archive_path="/tmp/archive_x.jsonl.gz")
    s = str(r)
    assert "3" in s
    assert "1" in s


# ---------------------------------------------------------------------------
# list_archives
# ---------------------------------------------------------------------------

def test_list_archives_empty_when_dir_missing(tmp_path):
    cfg = ArchiveConfig(archive_dir=str(tmp_path / "nope"))
    assert list_archives(cfg) == []


def test_list_archives_returns_sorted_names(tmp_path):
    arc = tmp_path / "archive"
    arc.mkdir()
    (arc / "archive_20240201T000000Z.jsonl").write_text("x")
    (arc / "archive_20240101T000000Z.jsonl").write_text("x")

    cfg = ArchiveConfig(archive_dir=str(arc))
    names = list_archives(cfg)
    assert names == sorted(names)
    assert len(names) == 2


# ---------------------------------------------------------------------------
# load_archiver_config
# ---------------------------------------------------------------------------

def test_load_archiver_config_missing_file_returns_defaults(tmp_path):
    cfg = load_archiver_config(str(tmp_path / "missing.yaml"))
    assert cfg.max_age_days == 30
    assert cfg.compress is True


def test_load_archiver_config_parses_yaml(tmp_path):
    p = tmp_path / "cfg.yaml"
    p.write_text(
        "archiver:\n  max_age_days: 14\n  compress: false\n  archive_dir: /tmp/arc\n",
        encoding="utf-8",
    )
    cfg = load_archiver_config(str(p))
    assert cfg.max_age_days == 14
    assert cfg.compress is False
    assert cfg.archive_dir == "/tmp/arc"


def test_load_archiver_config_empty_yaml_returns_defaults(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("", encoding="utf-8")
    cfg = load_archiver_config(str(p))
    assert cfg.max_age_days == 30
