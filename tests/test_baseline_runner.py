"""Tests for pipewatch.baseline_runner."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult, HealthThresholds
from pipewatch.alerts import Alert, AlertLevel
from pipewatch.reporter import Report
from pipewatch.baseline import save_baseline
from pipewatch.baseline_runner import (
    capture_baselines,
    format_baseline_report,
    regressions_in_report,
    run_baseline_check,
)


def _make_metric(pipeline="p1", success=90, failed=10, records=1000, elapsed=10.0):
    return PipelineMetric(
        pipeline=pipeline,
        success=success,
        failed=failed,
        records_processed=records,
        elapsed_seconds=elapsed,
        status=PipelineStatus.HEALTHY,
    )


def _make_result(metric: PipelineMetric) -> HealthResult:
    thresholds = HealthThresholds()
    return HealthResult(
        metric=metric,
        healthy=True,
        violations=[],
        thresholds=thresholds,
    )


def _make_report(metrics):
    results = [_make_result(m) for m in metrics]
    return Report(
        generated_at="2024-01-01T00:00:00Z",
        results=results,
        alerts=[],
        summary={},
    )


def test_run_baseline_check_no_baseline(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    report = _make_report([_make_metric()])
    comparisons = run_baseline_check(report)
    assert comparisons == []


def test_capture_baselines_saves_all(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    metrics = [_make_metric("p1"), _make_metric("p2")]
    report = _make_report(metrics)
    saved = capture_baselines(report)
    assert set(saved) == {"p1", "p2"}


def test_run_baseline_check_returns_comparisons(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    metric = _make_metric(success=90, failed=10)
    save_baseline(metric)
    report = _make_report([_make_metric(success=70, failed=30)])
    comparisons = run_baseline_check(report)
    assert len(comparisons) == 1
    assert comparisons[0].pipeline == "p1"


def test_regressions_in_report_filters_correctly(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    # Save a good baseline
    save_baseline(_make_metric("pipe_ok", success=95, failed=5))
    save_baseline(_make_metric("pipe_bad", success=95, failed=5))

    metrics = [
        _make_metric("pipe_ok", success=95, failed=5),    # no regression
        _make_metric("pipe_bad", success=60, failed=40),  # regression
    ]
    report = _make_report(metrics)
    regressions = regressions_in_report(report, regression_threshold=0.05)
    assert len(regressions) == 1
    assert regressions[0].pipeline == "pipe_bad"


def test_format_baseline_report_empty():
    text = format_baseline_report([])
    assert "No baseline" in text


def test_format_baseline_report_non_empty(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.baseline._BASELINE_DIR", str(tmp_path / "baselines"))
    metric = _make_metric(success=90, failed=10)
    save_baseline(metric)
    report = _make_report([_make_metric(success=70, failed=30)])
    comparisons = run_baseline_check(report)
    text = format_baseline_report(comparisons)
    assert "Baseline Comparison" in text
    assert "p1" in text
