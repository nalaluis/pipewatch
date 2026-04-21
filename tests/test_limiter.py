"""Tests for pipewatch.limiter."""
from __future__ import annotations

import time
import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.limiter import (
    LimiterConfig,
    LimiterWindow,
    apply_limiter,
    record_alert,
    reset_limiter,
    should_limit,
)


def make_alert(pipeline: str = "pipe", level: AlertLevel = AlertLevel.CRITICAL) -> Alert:
    return Alert(pipeline=pipeline, level=level, message="test alert", metric="failure_rate")


@pytest.fixture(autouse=True)
def clean_state():
    reset_limiter()
    yield
    reset_limiter()


def test_limiter_window_starts_empty():
    win = LimiterWindow()
    assert win.count() == 0


def test_limiter_window_records_and_counts():
    win = LimiterWindow()
    win.record()
    win.record()
    assert win.count() == 2


def test_limiter_window_prune_removes_old_entries():
    win = LimiterWindow()
    old = time.time() - 400
    win.timestamps = [old, old, time.time()]
    win.prune(window_seconds=300)
    assert win.count() == 1


def test_should_limit_returns_false_when_under_threshold():
    cfg = LimiterConfig(max_alerts_per_window=3, window_seconds=60)
    alert = make_alert()
    for _ in range(2):
        record_alert(alert)
    assert not should_limit(alert, cfg)


def test_should_limit_returns_true_at_threshold():
    cfg = LimiterConfig(max_alerts_per_window=3, window_seconds=60)
    alert = make_alert()
    for _ in range(3):
        record_alert(alert)
    assert should_limit(alert, cfg)


def test_should_limit_skips_below_min_level():
    cfg = LimiterConfig(max_alerts_per_window=1, min_level=AlertLevel.CRITICAL)
    alert = make_alert(level=AlertLevel.WARNING)
    for _ in range(10):
        record_alert(alert)
    assert not should_limit(alert, cfg)


def test_apply_limiter_allows_up_to_max():
    cfg = LimiterConfig(max_alerts_per_window=3, window_seconds=60)
    alerts = [make_alert() for _ in range(5)]
    result = apply_limiter(alerts, cfg)
    assert len(result) == 3


def test_apply_limiter_returns_all_when_under_limit():
    cfg = LimiterConfig(max_alerts_per_window=10, window_seconds=60)
    alerts = [make_alert() for _ in range(4)]
    result = apply_limiter(alerts, cfg)
    assert len(result) == 4


def test_apply_limiter_different_pipelines_tracked_separately():
    cfg = LimiterConfig(max_alerts_per_window=2, window_seconds=60)
    alerts = [
        make_alert(pipeline="a"),
        make_alert(pipeline="a"),
        make_alert(pipeline="b"),
        make_alert(pipeline="b"),
        make_alert(pipeline="a"),  # should be limited
    ]
    result = apply_limiter(alerts, cfg)
    pipelines = [a.pipeline for a in result]
    assert pipelines.count("a") == 2
    assert pipelines.count("b") == 2


def test_reset_limiter_clears_state():
    cfg = LimiterConfig(max_alerts_per_window=1, window_seconds=60)
    alert = make_alert()
    apply_limiter([alert], cfg)
    reset_limiter()
    result = apply_limiter([alert], cfg)
    assert len(result) == 1
