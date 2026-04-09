"""Tests for pipewatch.deduplicator."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.deduplicator import (
    DeduplicationConfig,
    DeduplicationState,
    filter_duplicates,
    should_deduplicate,
)


def make_alert(
    pipeline: str = "etl",
    level: AlertLevel = AlertLevel.WARNING,
    metric: str = "failure_rate",
    message: str = "high failure rate",
) -> Alert:
    return Alert(pipeline=pipeline, level=level, metric=metric, message=message)


def test_deduplication_state_initially_none():
    state = DeduplicationState()
    alert = make_alert()
    assert state.seconds_since(alert) is None


def test_deduplication_state_records_time():
    state = DeduplicationState()
    alert = make_alert()
    state.record(alert)
    elapsed = state.seconds_since(alert)
    assert elapsed is not None
    assert elapsed < 1.0


def test_deduplication_state_reset_clears_entry():
    state = DeduplicationState()
    alert = make_alert()
    state.record(alert)
    state.reset(alert)
    assert state.seconds_since(alert) is None


def test_should_deduplicate_returns_false_when_never_fired():
    state = DeduplicationState()
    config = DeduplicationConfig(window_seconds=300)
    alert = make_alert()
    assert should_deduplicate(alert, state, config) is False


def test_should_deduplicate_returns_true_within_window():
    state = DeduplicationState()
    config = DeduplicationConfig(window_seconds=300)
    alert = make_alert()
    state.record(alert)
    assert should_deduplicate(alert, state, config) is True


def test_should_deduplicate_returns_false_after_window():
    state = DeduplicationState()
    config = DeduplicationConfig(window_seconds=1)
    alert = make_alert()
    state.record(alert)
    with patch("pipewatch.deduplicator.time.monotonic", return_value=time.monotonic() + 5):
        assert should_deduplicate(alert, state, config) is False


def test_should_deduplicate_disabled_always_returns_false():
    state = DeduplicationState()
    config = DeduplicationConfig(window_seconds=300, enabled=False)
    alert = make_alert()
    state.record(alert)
    assert should_deduplicate(alert, state, config) is False


def test_filter_duplicates_passes_first_occurrence():
    state = DeduplicationState()
    config = DeduplicationConfig(window_seconds=300)
    alerts = [make_alert()]
    result = filter_duplicates(alerts, state, config)
    assert len(result) == 1


def test_filter_duplicates_suppresses_second_occurrence():
    state = DeduplicationState()
    config = DeduplicationConfig(window_seconds=300)
    alert = make_alert()
    state.record(alert)
    result = filter_duplicates([alert], state, config)
    assert result == []


def test_filter_duplicates_distinct_alerts_both_pass():
    state = DeduplicationState()
    config = DeduplicationConfig(window_seconds=300)
    a1 = make_alert(pipeline="pipe_a")
    a2 = make_alert(pipeline="pipe_b")
    result = filter_duplicates([a1, a2], state, config)
    assert len(result) == 2
