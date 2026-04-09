"""Tests for pipewatch.aggregator."""

import pytest
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult, HealthThresholds
from pipewatch.aggregator import aggregate, format_summary, PipelineSummary


def _make_result(
    name: str,
    failure_rate: float,
    throughput: float,
    status: PipelineStatus,
) -> HealthResult:
    metric = PipelineMetric(
        pipeline_name=name,
        total=100,
        failed=int(failure_rate * 100),
        processed=int(throughput),
        elapsed_seconds=1.0,
    )
    thresholds = HealthThresholds()
    return HealthResult(metric=metric, status=status, violations=[])


def test_aggregate_empty_returns_default():
    summary = aggregate([])
    assert summary.total == 0
    assert summary.healthy == 0
    assert summary.avg_failure_rate == 0.0


def test_aggregate_counts_statuses():
    results = [
        _make_result("a", 0.0, 100.0, PipelineStatus.HEALTHY),
        _make_result("b", 0.3, 50.0, PipelineStatus.WARNING),
        _make_result("c", 0.6, 10.0, PipelineStatus.CRITICAL),
    ]
    summary = aggregate(results)
    assert summary.total == 3
    assert summary.healthy == 1
    assert summary.warning == 1
    assert summary.critical == 1


def test_aggregate_avg_failure_rate():
    results = [
        _make_result("a", 0.1, 100.0, PipelineStatus.HEALTHY),
        _make_result("b", 0.3, 100.0, PipelineStatus.WARNING),
    ]
    summary = aggregate(results)
    assert abs(summary.avg_failure_rate - 0.2) < 0.01


def test_aggregate_avg_throughput():
    results = [
        _make_result("a", 0.0, 80.0, PipelineStatus.HEALTHY),
        _make_result("b", 0.0, 120.0, PipelineStatus.HEALTHY),
    ]
    summary = aggregate(results)
    assert abs(summary.avg_throughput - 100.0) < 0.1


def test_aggregate_worst_pipeline():
    results = [
        _make_result("low", 0.05, 100.0, PipelineStatus.HEALTHY),
        _make_result("high", 0.75, 10.0, PipelineStatus.CRITICAL),
        _make_result("mid", 0.20, 50.0, PipelineStatus.WARNING),
    ]
    summary = aggregate(results)
    assert summary.worst_pipeline == "high"
    assert abs(summary.worst_failure_rate - 0.75) < 0.01


def test_format_summary_contains_key_fields():
    results = [
        _make_result("pipe1", 0.1, 200.0, PipelineStatus.WARNING),
    ]
    summary = aggregate(results)
    output = format_summary(summary)
    assert "Total pipelines" in output
    assert "pipe1" in output
    assert "Warning" in output
