"""Tests for pipewatch.compactor and pipewatch.compactor_config."""

from __future__ import annotations

import os
import tempfile
import time

import pytest

from pipewatch.compactor import (
    CompactorConfig,
    CompactionResult,
    compact_snapshots,
    format_compaction,
)
from pipewatch.compactor_config import load_compactor_config
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.snapshot import save_snapshot


def _make_metric(name: str = "pipe") -> PipelineMetric:
    return PipelineMetric(
        pipeline=name,
        status=PipelineStatus.HEALTHY,
        failure_rate=0.0,
        throughput=100.0,
        error_count=0,
        record_count=100,
    )


def test_compact_snapshots_removes_old_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        metric = _make_metric("old_pipe")
        save_snapshot(metric, base_dir=tmpdir)
        # Force the file to appear old
        path = os.path.join(tmpdir, "old_pipe.json")
        old_time = time.time() - 200
        os.utime(path, (old_time, old_time))

        cfg = CompactorConfig(retention_seconds=100.0, dry_run=False)
        result = compact_snapshots(cfg, base_dir=tmpdir)

        assert "old_pipe" in result.removed
        assert not os.path.exists(path)


def test_compact_snapshots_keeps_fresh_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        metric = _make_metric("fresh_pipe")
        save_snapshot(metric, base_dir=tmpdir)

        cfg = CompactorConfig(retention_seconds=3600.0, dry_run=False)
        result = compact_snapshots(cfg, base_dir=tmpdir)

        assert "fresh_pipe" in result.kept
        assert "fresh_pipe" not in result.removed


def test_compact_snapshots_dry_run_does_not_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        metric = _make_metric("stale_pipe")
        save_snapshot(metric, base_dir=tmpdir)
        path = os.path.join(tmpdir, "stale_pipe.json")
        old_time = time.time() - 500
        os.utime(path, (old_time, old_time))

        cfg = CompactorConfig(retention_seconds=100.0, dry_run=True)
        result = compact_snapshots(cfg, base_dir=tmpdir)

        assert "stale_pipe" in result.removed
        assert os.path.exists(path), "dry_run should not delete the file"
        assert result.dry_run is True


def test_compaction_result_counts():
    result = CompactionResult(removed=["a", "b"], kept=["c"])
    assert result.removed_count == 2
    assert result.kept_count == 1


def test_compaction_result_str_dry_run():
    result = CompactionResult(removed=["x"], kept=[], dry_run=True)
    assert "[dry-run]" in str(result)


def test_compaction_result_str_normal():
    result = CompactionResult(removed=[], kept=["y"], dry_run=False)
    assert "[dry-run]" not in str(result)


def test_format_compaction_includes_names():
    result = CompactionResult(removed=["old"], kept=["new"])
    text = format_compaction(result)
    assert "old" in text
    assert "new" in text
    assert "Removed" in text
    assert "Kept" in text


def test_load_compactor_config_missing_returns_defaults(tmp_path):
    cfg = load_compactor_config(path=str(tmp_path / "nonexistent.yaml"))
    assert cfg.retention_seconds == 86400.0
    assert cfg.dry_run is False


def test_load_compactor_config_parses_yaml(tmp_path):
    p = tmp_path / "compactor.yaml"
    p.write_text("retention_seconds: 3600\ndry_run: true\n")
    cfg = load_compactor_config(path=str(p))
    assert cfg.retention_seconds == 3600.0
    assert cfg.dry_run is True


def test_load_compactor_config_partial_yaml(tmp_path):
    p = tmp_path / "compactor.yaml"
    p.write_text("retention_seconds: 7200\n")
    cfg = load_compactor_config(path=str(p))
    assert cfg.retention_seconds == 7200.0
    assert cfg.dry_run is False
