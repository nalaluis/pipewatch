"""Unit tests for pipeline health evaluation."""

import pytest

from pipewatch.health import HealthThresholds, evaluate_health
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(**kwargs) -> PipelineMetric:
    defaults = dict(
        pipeline_id="test_pipe",
        rows_processed=1000,
        rows_failed=0,
        duration_seconds=60.0,
    )
    defaults.update(kwargs)
    return PipelineMetric(**defaults)


DEFAULT_THRESHOLDS = HealthThresholds()


def test_healthy_pipeline():
    metric = make_metric(rows_processed=1000, rows_failed=5, duration_seconds=30)
    result = evaluate_health(metric, DEFAULT_THRESHOLDS)
    assert result.status == PipelineStatus.OK
    assert result.reasons == []


def test_critical_failure_rate():
    metric = make_metric(rows_processed=900, rows_failed=100)  # 10% failure
    result = evaluate_health(metric, DEFAULT_THRESHOLDS)
    assert result.status == PipelineStatus.CRITICAL
    assert any("Failure rate" in r for r in result.reasons)


def test_warning_failure_rate():
    metric = make_metric(rows_processed=980, rows_failed=20)  # 2% failure
    result = evaluate_health(metric, DEFAULT_THRESHOLDS)
    assert result.status == PipelineStatus.WARNING


def test_critical_low_throughput():
    metric = make_metric(rows_processed=50, rows_failed=0, duration_seconds=100)  # 0.5 rows/s
    result = evaluate_health(metric, DEFAULT_THRESHOLDS)
    assert result.status == PipelineStatus.CRITICAL
    assert any("Throughput" in r for r in result.reasons)


def test_critical_duration_exceeded():
    metric = make_metric(rows_processed=5000, rows_failed=0, duration_seconds=7200)
    result = evaluate_health(metric, DEFAULT_THRESHOLDS)
    assert result.status == PipelineStatus.CRITICAL
    assert any("Duration" in r for r in result.reasons)


def test_custom_thresholds():
    thresholds = HealthThresholds(max_failure_rate=0.20, min_throughput=1.0)
    metric = make_metric(rows_processed=900, rows_failed=100, duration_seconds=10)
    result = evaluate_health(metric, thresholds)
    # 10% failure < 20% max — should not be critical on failure rate alone
    assert result.status != PipelineStatus.CRITICAL or "Duration" in str(result.reasons)


def test_failure_rate_zero_rows():
    metric = make_metric(rows_processed=0, rows_failed=0, duration_seconds=1)
    assert metric.failure_rate == 0.0


def test_throughput_zero_duration():
    metric = make_metric(rows_processed=100, rows_failed=0, duration_seconds=0)
    assert metric.throughput == 0.0


def test_multiple_issues_reported():
    """When multiple thresholds are violated, all reasons should be reported."""
    # High failure rate AND slow duration — expect both reasons to appear
    metric = make_metric(rows_processed=900, rows_failed=100, duration_seconds=7200)
    result = evaluate_health(metric, DEFAULT_THRESHOLDS)
    assert result.status == PipelineStatus.CRITICAL
    reason_text = " ".join(result.reasons)
    assert "Failure rate" in reason_text
    assert "Duration" in reason_text
