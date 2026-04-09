"""Tests for pipewatch.suppressor."""

from unittest.mock import patch

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.suppressor import (
    SuppressionRule,
    SuppressionState,
    filter_alerts,
    should_suppress,
)


def make_alert(pipeline: str = "etl", level: AlertLevel = AlertLevel.WARNING) -> Alert:
    return Alert(pipeline=pipeline, level=level, message="test alert", metric="failure_rate", value=0.5)


def test_suppression_state_initially_none():
    state = SuppressionState()
    assert state.seconds_since("etl", AlertLevel.WARNING) is None


def test_suppression_state_records_time():
    state = SuppressionState()
    state.record("etl", AlertLevel.WARNING)
    elapsed = state.seconds_since("etl", AlertLevel.WARNING)
    assert elapsed is not None
    assert elapsed < 1.0


def test_suppression_state_reset_clears_entry():
    state = SuppressionState()
    state.record("etl", AlertLevel.CRITICAL)
    state.reset("etl", AlertLevel.CRITICAL)
    assert state.seconds_since("etl", AlertLevel.CRITICAL) is None


def test_should_suppress_no_rules_returns_false():
    alert = make_alert()
    state = SuppressionState()
    assert should_suppress(alert, state, []) is False


def test_should_suppress_no_prior_record_returns_false():
    alert = make_alert(pipeline="etl", level=AlertLevel.WARNING)
    state = SuppressionState()
    rules = [SuppressionRule(pipeline="etl", level=AlertLevel.WARNING, cooldown_seconds=300)]
    assert should_suppress(alert, state, rules) is False


def test_should_suppress_within_cooldown_returns_true():
    alert = make_alert(pipeline="etl", level=AlertLevel.WARNING)
    state = SuppressionState()
    state.record("etl", AlertLevel.WARNING)
    rules = [SuppressionRule(pipeline="etl", level=AlertLevel.WARNING, cooldown_seconds=300)]
    assert should_suppress(alert, state, rules) is True


def test_should_suppress_after_cooldown_returns_false():
    alert = make_alert(pipeline="etl", level=AlertLevel.WARNING)
    state = SuppressionState()
    rules = [SuppressionRule(pipeline="etl", level=AlertLevel.WARNING, cooldown_seconds=5)]
    # Simulate time having passed beyond cooldown
    with patch("pipewatch.suppressor.time.monotonic", side_effect=[0.0, 10.0, 10.0]):
        state.record("etl", AlertLevel.WARNING)
        assert should_suppress(alert, state, rules) is False


def test_filter_alerts_allows_first_occurrence():
    alerts = [make_alert()]
    state = SuppressionState()
    rules = [SuppressionRule(pipeline="etl", level=AlertLevel.WARNING, cooldown_seconds=300)]
    result = filter_alerts(alerts, state, rules)
    assert len(result) == 1


def test_filter_alerts_suppresses_duplicate():
    alert = make_alert()
    state = SuppressionState()
    rules = [SuppressionRule(pipeline="etl", level=AlertLevel.WARNING, cooldown_seconds=300)]
    first = filter_alerts([alert], state, rules)
    second = filter_alerts([alert], state, rules)
    assert len(first) == 1
    assert len(second) == 0


def test_filter_alerts_different_pipelines_not_suppressed():
    alerts = [
        make_alert(pipeline="pipe_a"),
        make_alert(pipeline="pipe_b"),
    ]
    state = SuppressionState()
    rules = [
        SuppressionRule(pipeline="pipe_a", level=AlertLevel.WARNING, cooldown_seconds=300),
        SuppressionRule(pipeline="pipe_b", level=AlertLevel.WARNING, cooldown_seconds=300),
    ]
    result = filter_alerts(alerts, state, rules)
    assert len(result) == 2
