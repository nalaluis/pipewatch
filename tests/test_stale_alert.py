"""Tests for pipewatch.stale_alert module."""

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.stale_alert import (
    StaleAlertConfig,
    StaleAlertEntry,
    StaleAlertState,
)


def make_alert(
    pipeline: str = "pipe_a",
    metric: str = "failure_rate",
    level: AlertLevel = AlertLevel.WARNING,
    message: str = "high failure rate",
) -> Alert:
    return Alert(pipeline=pipeline, metric=metric, level=level, message=message)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_record_creates_entry():
    state = StaleAlertState()
    alert = make_alert()
    entry = state.record(alert)
    assert entry.count == 1
    assert entry.alert_key == "pipe_a:failure_rate:warning"


def test_record_increments_existing_entry():
    state = StaleAlertState()
    alert = make_alert()
    state.record(alert)
    entry = state.record(alert)
    assert entry.count == 2


def test_resolve_removes_entry():
    state = StaleAlertState()
    alert = make_alert()
    state.record(alert)
    state.resolve(alert)
    assert state._entries == {}


def test_entry_is_not_stale_when_fresh():
    now = _now()
    entry = StaleAlertEntry(alert_key="k", first_seen=now, last_seen=now)
    assert not entry.is_stale(ttl=300, now=now)


def test_entry_is_stale_after_ttl():
    past = _now() - timedelta(seconds=400)
    entry = StaleAlertEntry(alert_key="k", first_seen=past, last_seen=past)
    assert entry.is_stale(ttl=300)


def test_stale_alerts_returns_empty_when_no_alerts_recorded():
    state = StaleAlertState()
    alert = make_alert()
    cfg = StaleAlertConfig(ttl_seconds=60)
    result = state.stale_alerts([alert], cfg)
    assert result == []


def test_stale_alerts_flags_old_alert():
    state = StaleAlertState()
    alert = make_alert()
    past = _now() - timedelta(seconds=400)
    state.record(alert, now=past)
    cfg = StaleAlertConfig(ttl_seconds=300)
    result = state.stale_alerts([alert], cfg)
    assert alert in result


def test_stale_alerts_skips_below_min_level():
    state = StaleAlertState()
    alert = make_alert(level=AlertLevel.WARNING)
    past = _now() - timedelta(seconds=400)
    state.record(alert, now=past)
    cfg = StaleAlertConfig(ttl_seconds=60, min_level=AlertLevel.CRITICAL)
    result = state.stale_alerts([alert], cfg)
    assert result == []


def test_reset_clears_all_entries():
    state = StaleAlertState()
    state.record(make_alert(pipeline="a"))
    state.record(make_alert(pipeline="b"))
    state.reset()
    assert state._entries == {}
