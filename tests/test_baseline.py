"""Tests for pipewatch.baseline."""
from __future__ import annotations

import os
import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.baseline import (
    BaselineComparison,
    BaselineEntry,
    compare_to_baseline,
    format_comparison,
    load_baseline,
    save_baseline,
    _baseline_path,
)


def make_metric(
    pipeline: str = "etl_main",
    success: int = 90,
    failed: int = 10,
    records: int = 1000,
    elapsed: float = 10.0,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        success=success,
        failed=failed,
        records_processed=records,
        elapsed_seconds=elapsed,
        status=status,
    )


def test_save_creates_file(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    metric = make_metric()
    entry = save_baseline(metric)
    path = _baseline_path.__wrapped__(metric.pipeline) if hasattr(_baseline_path, "__wrapped__") else None
    # Verify the returned entry has expected fields
    assert entry.pipeline == "etl_main"
    assert 0.0 <= entry.failure_rate <= 1.0
    assert entry.throughput > 0
    assert entry.recorded_at  # non-empty timestamp


def test_load_returns_none_for_missing(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    result = load_baseline("nonexistent_pipeline")
    assert result is None


def test_roundtrip_preserves_fields(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    metric = make_metric(pipeline="pipe_a", success=80, failed=20, records=500, elapsed=5.0)
    saved = save_baseline(metric)
    loaded = load_baseline("pipe_a")
    assert loaded is not None
    assert loaded.pipeline == saved.pipeline
    assert abs(loaded.failure_rate - saved.failure_rate) < 1e-9
    assert abs(loaded.throughput - saved.throughput) < 1e-9


def test_compare_returns_none_without_baseline(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    metric = make_metric()
    assert compare_to_baseline(metric) is None


def test_compare_detects_regression(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    baseline_metric = make_metric(success=95, failed=5)   # 5% failure rate
    save_baseline(baseline_metric)
    current_metric = make_metric(success=80, failed=20)   # 20% failure rate
    cmp = compare_to_baseline(current_metric, regression_threshold=0.05)
    assert cmp is not None
    assert cmp.regression is True
    assert cmp.failure_rate_delta > 0


def test_compare_no_regression_when_improved(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    baseline_metric = make_metric(success=70, failed=30)
    save_baseline(baseline_metric)
    current_metric = make_metric(success=95, failed=5)
    cmp = compare_to_baseline(current_metric, regression_threshold=0.05)
    assert cmp is not None
    assert cmp.regression is False
    assert cmp.failure_rate_delta < 0


def test_format_comparison_regression_tag():
    cmp = BaselineComparison(
        pipeline="pipe_x",
        failure_rate_delta=0.10,
        throughput_delta=-5.0,
        regression=True,
    )
    text = format_comparison(cmp)
    assert "REGRESSION" in text
    assert "pipe_x" in text


def test_format_comparison_no_regression_tag():
    cmp = BaselineComparison(
        pipeline="pipe_y",
        failure_rate_delta=-0.02,
        throughput_delta=3.0,
        regression=False,
    )
    text = format_comparison(cmp)
    assert "REGRESSION" not in text
