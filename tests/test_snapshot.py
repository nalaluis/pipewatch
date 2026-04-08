"""Tests for pipewatch.snapshot persistence helpers."""

import os
import tempfile

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.snapshot import (
    save_snapshot,
    load_snapshot,
    list_snapshots,
    _snapshot_path,
)


def make_metric(name="pipe_a", processed=1000, failed=10, duration=60.0, status=PipelineStatus.OK):
    return PipelineMetric(
        pipeline_name=name,
        records_processed=processed,
        records_failed=failed,
        duration_seconds=duration,
        status=status,
    )


def test_save_creates_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        metric = make_metric()
        path = save_snapshot(metric, directory=tmpdir)
        assert os.path.exists(path)


def test_load_returns_none_for_missing_pipeline():
    with tempfile.TemporaryDirectory() as tmpdir:
        result = load_snapshot("nonexistent", directory=tmpdir)
        assert result is None


def test_roundtrip_preserves_fields():
    with tempfile.TemporaryDirectory() as tmpdir:
        metric = make_metric(name="etl_orders", processed=500, failed=5, duration=30.0)
        save_snapshot(metric, directory=tmpdir)
        loaded = load_snapshot("etl_orders", directory=tmpdir)
        assert loaded is not None
        assert loaded.pipeline_name == metric.pipeline_name
        assert loaded.records_processed == metric.records_processed
        assert loaded.records_failed == metric.records_failed
        assert loaded.duration_seconds == metric.duration_seconds
        assert loaded.status == metric.status


def test_list_snapshots_returns_saved_names():
    with tempfile.TemporaryDirectory() as tmpdir:
        save_snapshot(make_metric(name="pipe_a"), directory=tmpdir)
        save_snapshot(make_metric(name="pipe_b"), directory=tmpdir)
        names = list_snapshots(directory=tmpdir)
        assert "pipe_a" in names
        assert "pipe_b" in names


def test_list_snapshots_empty_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        names = list_snapshots(directory=tmpdir)
        assert names == []


def test_list_snapshots_missing_dir():
    names = list_snapshots(directory="/tmp/__pipewatch_no_such_dir_xyz")
    assert names == []


def test_overwrite_updates_snapshot():
    with tempfile.TemporaryDirectory() as tmpdir:
        save_snapshot(make_metric(name="pipe_x", processed=100), directory=tmpdir)
        save_snapshot(make_metric(name="pipe_x", processed=999), directory=tmpdir)
        loaded = load_snapshot("pipe_x", directory=tmpdir)
        assert loaded.records_processed == 999
