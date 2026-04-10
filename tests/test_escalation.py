"""Tests for pipewatch.escalation."""

from __future__ import annotations

from time import time
from unittest.mock import patch

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.escalation import (
    EscalationPolicy,
    EscalationState,
    escalate_alert,
    should_escalate,
)


def make_alert(level: AlertLevel = AlertLevel.WARNING) -> Alert:
    return Alert(
        pipeline="pipe-a",
        level=level,
        metric="failure_rate",
        message="failure_rate is 0.4 (threshold 0.2)",
    )


# ---------------------------------------------------------------------------
# EscalationState
# ---------------------------------------------------------------------------

def test_escalation_state_initially_none():
    state = EscalationState()
    assert state.get(make_alert()) is None


def test_escalation_state_records_entry():
    state = EscalationState()
    alert = make_alert()
    state.record(alert)
    assert state.get(alert) is not None


def test_escalation_state_reset_clears_entry():
    state = EscalationState()
    alert = make_alert()
    state.record(alert)
    state.reset(alert)
    assert state.get(alert) is None


def test_escalation_state_increment_increases_count():
    state = EscalationState()
    alert = make_alert()
    state.record(alert)
    state.increment(alert)
    assert state.get(alert).escalation_count == 1


def test_escalation_state_multiple_increments():
    """Verify that successive increments accumulate correctly."""
    state = EscalationState()
    alert = make_alert()
    state.record(alert)
    for _ in range(3):
        state.increment(alert)
    assert state.get(alert).escalation_count == 3


# ---------------------------------------------------------------------------
# should_escalate
# ---------------------------------------------------------------------------

def test_should_escalate_returns_false_without_entry():
    state = EscalationState()
    policy = EscalationPolicy(escalate_after_seconds=10.0)
    assert should_escalate(make_alert(), state, policy) is False


def test_should_escalate_returns_false_before_threshold():
    state = EscalationState()
    alert = make_alert()
    state.record(alert)
    policy = EscalationPolicy(escalate_after_seconds=600.0)
    assert should_escalate(alert, state, policy, now=time()) is False


def test_should_escalate_returns_true_after_threshold():
    state = EscalationState()
    alert = make_alert()
    state.record(alert)
    policy = EscalationPolicy(escalate_after_seconds=60.0)
    future = state.get(alert).first_seen + 120.0
    assert should_escalate(alert, state, policy, now=future) is True


def test_should_escalate_critical_alert_never_escalates():
    state = EscalationState()
    alert = make_alert(level=AlertLevel.CRITICAL)
    state.record(alert)
    policy = EscalationPolicy(escalate_after_seconds=0.0)
    assert should_escalate(alert, state, policy, now=time() + 9999) is False


def test_should_escalate_respects_max_escalations():
    state = EscalationState()
    alert = make_alert()
    state.record(alert)
    state.increment(alert)  # already escalated once
    policy = EscalationPolicy(escalate_after_seconds=0.0, max_escalations=1)
    assert should_escalate(alert, state, policy, now=time() + 9999)
