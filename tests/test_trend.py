"""Tests for pipewatch.trend."""
import pytest
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.history import HistoryEntry
from pipewatch.trend import compute_trend, format_trend, TrendSummary


def _make_entry(pipeline: str, failure_rate: float, throughput: float,
                status: PipelineStatus = PipelineStatus.OK) -> HistoryEntry:
    metric = PipelineMetric(
        pipeline=pipeline,
        success=0,
        failure=0,
        failure_rate=failure_rate,
        throughput=throughput,
        status=status,
    )
    return HistoryEntry(current=metric, previous=None, diff=None)


def test_compute_trend_returns_none_for_empty():
    assert compute_trend([]) is None


def test_compute_trend_single_entry():
    entry = _make_entry("pipe-a", 0.1, 50.0)
    summary = compute_trend([entry])
    assert summary is not None
    assert summary.pipeline == "pipe-a"
    assert summary.sample_count == 1
    assert summary.failure_rate_trend == "stable"
    assert summary.throughput_trend == "stable"


def test_compute_trend_averages():
    entries = [
        _make_entry("p", 0.2, 100.0),
        _make_entry("p", 0.4, 200.0),
    ]
    summary = compute_trend(entries)
    assert summary.avg_failure_rate == pytest.approx(0.3, abs=1e-4)
    assert summary.avg_throughput == pytest.approx(150.0, abs=1e-4)


def test_failure_rate_degrading():
    entries = [
        _make_entry("p", 0.05, 100.0),
        _make_entry("p", 0.30, 100.0),
    ]
    summary = compute_trend(entries)
    assert summary.failure_rate_trend == "degrading"


def test_failure_rate_improving():
    entries = [
        _make_entry("p", 0.40, 100.0),
        _make_entry("p", 0.05, 100.0),
    ]
    summary = compute_trend(entries)
    assert summary.failure_rate_trend == "improving"


def test_throughput_degrading():
    entries = [
        _make_entry("p", 0.0, 200.0),
        _make_entry("p", 0.0, 50.0),
    ]
    summary = compute_trend(entries)
    assert summary.throughput_trend == "degrading"


def test_consecutive_critical_count():
    entries = [
        _make_entry("p", 0.5, 10.0, PipelineStatus.OK),
        _make_entry("p", 0.5, 10.0, PipelineStatus.CRITICAL),
        _make_entry("p", 0.5, 10.0, PipelineStatus.CRITICAL),
    ]
    summary = compute_trend(entries)
    assert summary.consecutive_critical == 2


def test_format_trend_returns_string():
    entry = _make_entry("pipe-x", 0.1, 75.0)
    summary = compute_trend([entry])
    text = format_trend(summary)
    assert "pipe-x" in text
    assert "avg_failure_rate" in text
    assert "avg_throughput" in text
    assert "consecutive_critical" in text
