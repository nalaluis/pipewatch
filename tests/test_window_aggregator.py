"""Tests for pipewatch.window_aggregator."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.window_aggregator import (
    WindowConfig,
    clear_window,
    compute_window_stats,
    record_metric,
)


def make_metric(pipeline: str, failure_rate: float = 0.0, throughput: float = 100.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=PipelineStatus.HEALTHY,
        failure_rate=failure_rate,
        throughput=throughput,
        total=1000,
        failed=int(failure_rate * 1000),
    )


@pytest.fixture(autouse=True)
def reset_state():
    clear_window()
    yield
    clear_window()


def test_compute_window_stats_returns_none_for_empty():
    result = compute_window_stats("pipe-a")
    assert result is None


def test_record_and_compute_single_metric():
    m = make_metric("pipe-a", failure_rate=0.1, throughput=50.0)
    record_metric(m)
    stats = compute_window_stats("pipe-a")
    assert stats is not None
    assert stats.count == 1
    assert stats.avg_failure_rate == pytest.approx(0.1)
    assert stats.avg_throughput == pytest.approx(50.0)


def test_compute_averages_across_multiple_metrics():
    record_metric(make_metric("pipe-b", failure_rate=0.2, throughput=80.0))
    record_metric(make_metric("pipe-b", failure_rate=0.4, throughput=120.0))
    stats = compute_window_stats("pipe-b")
    assert stats is not None
    assert stats.count == 2
    assert stats.avg_failure_rate == pytest.approx(0.3)
    assert stats.avg_throughput == pytest.approx(100.0)


def test_compute_min_max_values():
    record_metric(make_metric("pipe-c", failure_rate=0.1, throughput=10.0))
    record_metric(make_metric("pipe-c", failure_rate=0.5, throughput=90.0))
    stats = compute_window_stats("pipe-c")
    assert stats.min_failure_rate == pytest.approx(0.1)
    assert stats.max_failure_rate == pytest.approx(0.5)
    assert stats.min_throughput == pytest.approx(10.0)
    assert stats.max_throughput == pytest.approx(90.0)


def test_prune_removes_old_entries():
    cfg = WindowConfig(window_seconds=1)
    old_time = time.time() - 10
    m = make_metric("pipe-d", failure_rate=0.9)
    record_metric(m, cfg)
    # Manually backdate the entry
    from pipewatch import window_aggregator
    window_aggregator._windows["pipe-d"][0].recorded_at = old_time
    stats = compute_window_stats("pipe-d", cfg)
    assert stats is None


def test_max_entries_respected():
    cfg = WindowConfig(window_seconds=3600, max_entries=3)
    for i in range(5):
        record_metric(make_metric("pipe-e", failure_rate=float(i) / 10), cfg)
    stats = compute_window_stats("pipe-e", cfg)
    assert stats.count == 3


def test_clear_window_single_pipeline():
    record_metric(make_metric("pipe-f"))
    record_metric(make_metric("pipe-g"))
    clear_window("pipe-f")
    assert compute_window_stats("pipe-f") is None
    assert compute_window_stats("pipe-g") is not None


def test_window_stats_str_format():
    record_metric(make_metric("pipe-h", failure_rate=0.05, throughput=200.0))
    stats = compute_window_stats("pipe-h")
    text = str(stats)
    assert "pipe-h" in text
    assert "avg_failure_rate" in text
    assert "avg_throughput" in text
