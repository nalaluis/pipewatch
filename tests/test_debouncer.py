"""Tests for pipewatch.debouncer."""
from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.debouncer import (
    DebouncerConfig,
    DebouncerEntry,
    _state,
    apply_debounce,
    record,
    resolve,
    should_debounce,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_alert(
    pipeline: str = "pipe_a",
    metric: str = "failure_rate",
    level: AlertLevel = AlertLevel.WARNING,
) -> Alert:
    return Alert(pipeline=pipeline, metric=metric, level=level, message="test")


@pytest.fixture(autouse=True)
def clean_state():
    _state.clear()
    yield
    _state.clear()


# ---------------------------------------------------------------------------
# record()
# ---------------------------------------------------------------------------

def test_record_creates_entry():
    a = make_alert()
    entry = record(a, now=1000.0)
    assert isinstance(entry, DebouncerEntry)
    assert entry.first_seen == 1000.0
    assert entry.count == 1


def test_record_increments_count_on_repeat():
    a = make_alert()
    record(a, now=1000.0)
    entry = record(a, now=1005.0)
    assert entry.count == 2
    assert entry.first_seen == 1000.0  # first_seen unchanged


def test_record_updates_level():
    a = make_alert(level=AlertLevel.WARNING)
    record(a, now=1000.0)
    a2 = make_alert(level=AlertLevel.CRITICAL)
    entry = record(a2, now=1010.0)
    assert entry.level == AlertLevel.CRITICAL


# ---------------------------------------------------------------------------
# resolve()
# ---------------------------------------------------------------------------

def test_resolve_removes_entry():
    a = make_alert()
    record(a, now=1000.0)
    resolve(a)
    assert f"{a.pipeline}:{a.metric}" not in _state


def test_resolve_missing_key_is_safe():
    resolve(make_alert())  # must not raise


# ---------------------------------------------------------------------------
# should_debounce()
# ---------------------------------------------------------------------------

def test_should_debounce_true_on_first_sighting():
    cfg = DebouncerConfig(min_duration_seconds=30.0)
    a = make_alert()
    assert should_debounce(a, cfg, now=1000.0) is True


def test_should_debounce_true_before_window_expires():
    cfg = DebouncerConfig(min_duration_seconds=30.0)
    a = make_alert()
    record(a, now=1000.0)
    assert should_debounce(a, cfg, now=1010.0) is True


def test_should_debounce_false_after_window_expires():
    cfg = DebouncerConfig(min_duration_seconds=30.0)
    a = make_alert()
    record(a, now=1000.0)
    assert should_debounce(a, cfg, now=1031.0) is False


def test_should_debounce_false_when_disabled():
    cfg = DebouncerConfig(min_duration_seconds=30.0, enabled=False)
    a = make_alert()
    # No prior record — but disabled means pass-through
    assert should_debounce(a, cfg, now=1000.0) is False


# ---------------------------------------------------------------------------
# apply_debounce()
# ---------------------------------------------------------------------------

def test_apply_debounce_blocks_new_alerts():
    cfg = DebouncerConfig(min_duration_seconds=30.0)
    alerts = [make_alert()]
    result = apply_debounce(alerts, cfg, now=1000.0)
    assert result == []


def test_apply_debounce_passes_after_window():
    cfg = DebouncerConfig(min_duration_seconds=30.0)
    a = make_alert()
    apply_debounce([a], cfg, now=1000.0)  # first sighting
    result = apply_debounce([a], cfg, now=1031.0)
    assert a in result


def test_apply_debounce_disabled_passes_all():
    cfg = DebouncerConfig(min_duration_seconds=30.0, enabled=False)
    alerts = [make_alert(), make_alert(pipeline="pipe_b")]
    result = apply_debounce(alerts, cfg, now=1000.0)
    assert len(result) == 2


def test_apply_debounce_multiple_alerts_independent_windows():
    cfg = DebouncerConfig(min_duration_seconds=20 = make_alert(pipeline="pipe_a")
    a2 = make_alert(pipeline="pipe_b")
    apply_debounce([a1], cfg, now=1000.0)  # a1 first seen at t=1000
    apply_debounce([a2], cfg, now=1010.0)  # a2 first seen at t=1010
    result = apply_debounce([a1, a2], cfg, now=1025.0)
    # a1 age=25 >= 20 → passes; a2 age=15 < 20 → blocked
    assert a1 in result
    assert a2 not in result
