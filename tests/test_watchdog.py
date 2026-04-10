"""Tests for pipewatch.watchdog."""

from __future__ import annotations

import time

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.watchdog import (
    WatchdogConfig,
    WatchdogState,
    StalePipeline,
    check_stale,
    update_watchdog,
    format_stale_report,
)


def make_metric(name: str = "pipe-a") -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=PipelineStatus.HEALTHY,
        failure_rate=0.0,
        throughput=100.0,
        error_count=0,
        record_count=100,
    )


def test_watchdog_state_initially_none():
    state = WatchdogState()
    assert state.last_seen("pipe-a") is None


def test_watchdog_state_records_time():
    state = WatchdogState()
    before = time.time()
    state.record("pipe-a")
    after = time.time()
    ts = state.last_seen("pipe-a")
    assert ts is not None
    assert before <= ts <= after


def test_watchdog_state_reset_clears_entry():
    state = WatchdogState()
    state.record("pipe-a")
    state.reset("pipe-a")
    assert state.last_seen("pipe-a") is None


def test_check_stale_returns_empty_when_all_fresh():
    state = WatchdogState()
    cfg = WatchdogConfig(stale_after_seconds=300)
    now = time.time()
    state.record("pipe-a", ts=now - 10)
    result = check_stale(state, cfg, now=now)
    assert result == []


def test_check_stale_detects_stale_pipeline():
    state = WatchdogState()
    cfg = WatchdogConfig(stale_after_seconds=60)
    now = time.time()
    state.record("pipe-a", ts=now - 120)
    result = check_stale(state, cfg, now=now)
    assert len(result) == 1
    assert result[0].name == "pipe-a"
    assert result[0].seconds_since >= 120


def test_check_stale_disabled_returns_empty():
    state = WatchdogState()
    cfg = WatchdogConfig(stale_after_seconds=10, enabled=False)
    now = time.time()
    state.record("pipe-a", ts=now - 9999)
    result = check_stale(state, cfg, now=now)
    assert result == []


def test_check_stale_boundary_exact_threshold():
    state = WatchdogState()
    cfg = WatchdogConfig(stale_after_seconds=60)
    now = time.time()
    state.record("pipe-a", ts=now - 60)
    result = check_stale(state, cfg, now=now)
    assert len(result) == 1


def test_update_watchdog_records_metric_name():
    state = WatchdogState()
    metric = make_metric("my-pipeline")
    update_watchdog(state, metric)
    assert state.last_seen("my-pipeline") is not None


def test_format_stale_report_no_stale():
    report = format_stale_report([])
    assert "on time" in report


def test_format_stale_report_with_stale():
    stale = [StalePipeline(name="pipe-x", last_seen=0.0, seconds_since=400.0)]
    report = format_stale_report(stale)
    assert "pipe-x" in report
    assert "STALE" in report
