"""Tests for pipewatch.diff metric comparison helpers."""

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.diff import compute_diff, format_diff, MetricDiff


def make_metric(name="pipe", processed=1000, failed=10, duration=100.0, status=PipelineStatus.OK):
    return PipelineMetric(
        pipeline_name=name,
        records_processed=processed,
        records_failed=failed,
        duration_seconds=duration,
        status=status,
    )


def test_compute_diff_returns_none_without_previous():
    current = make_metric()
    assert compute_diff(current, None) is None


def test_compute_diff_correct_deltas():
    previous = make_metric(processed=800, failed=8)
    current = make_metric(processed=1000, failed=20)
    diff = compute_diff(current, previous)
    assert diff is not None
    assert diff.records_processed_delta == 200
    assert diff.records_failed_delta == 12


def test_compute_diff_failure_rate_delta():
    previous = make_metric(processed=1000, failed=10)   # 1%
    current = make_metric(processed=1000, failed=50)    # 5%
    diff = compute_diff(current, previous)
    assert diff.failure_rate_delta == pytest.approx(0.04, abs=1e-4)


def test_compute_diff_status_changed_flag():
    previous = make_metric(status=PipelineStatus.OK)
    current = make_metric(status=PipelineStatus.CRITICAL)
    diff = compute_diff(current, previous)
    assert diff.status_changed is True
    assert diff.previous_status == "ok"
    assert diff.current_status == "critical"


def test_compute_diff_no_status_change():
    previous = make_metric(status=PipelineStatus.OK)
    current = make_metric(status=PipelineStatus.OK)
    diff = compute_diff(current, previous)
    assert diff.status_changed is False


def test_format_diff_contains_pipeline_name():
    previous = make_metric(name="orders_etl")
    current = make_metric(name="orders_etl")
    diff = compute_diff(current, previous)
    output = format_diff(diff)
    assert "orders_etl" in output


def test_format_diff_shows_status_change():
    previous = make_metric(status=PipelineStatus.WARNING)
    current = make_metric(status=PipelineStatus.CRITICAL)
    diff = compute_diff(current, previous)
    output = format_diff(diff)
    assert "warning" in output
    assert "critical" in output
