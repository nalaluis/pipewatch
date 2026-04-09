"""Tests for pipewatch.throttle and pipewatch.throttle_config."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.throttle import (
    ThrottleConfig,
    ThrottleState,
    apply_throttle,
    should_throttle,
)
from pipewatch.throttle_config import load_throttle_config


def make_alert(pipeline: str = "etl", level: AlertLevel = AlertLevel.WARNING) -> Alert:
    return Alert(pipeline=pipeline, level=level, message="test alert", metric="failure_rate", value=0.5)


# ---------------------------------------------------------------------------
# ThrottleState
# ---------------------------------------------------------------------------

def test_throttle_state_initially_none():
    state = ThrottleState()
    alert = make_alert()
    assert state.seconds_since(alert) is None


def test_throttle_state_records_time():
    state = ThrottleState()
    alert = make_alert()
    state.record(alert, now=1000.0)
    assert state.seconds_since(alert, now=1060.0) == pytest.approx(60.0)


def test_throttle_state_reset_clears_entry():
    state = ThrottleState()
    alert = make_alert()
    state.record(alert, now=1000.0)
    state.reset(alert)
    assert state.seconds_since(alert) is None


# ---------------------------------------------------------------------------
# should_throttle
# ---------------------------------------------------------------------------

def test_should_throttle_returns_false_when_never_fired():
    state = ThrottleState()
    config = ThrottleConfig(cooldown_seconds=300)
    assert should_throttle(make_alert(), state, config, now=1000.0) is False


def test_should_throttle_returns_true_within_cooldown():
    state = ThrottleState()
    config = ThrottleConfig(cooldown_seconds=300)
    alert = make_alert()
    state.record(alert, now=1000.0)
    assert should_throttle(alert, state, config, now=1100.0) is True


def test_should_throttle_returns_false_after_cooldown():
    state = ThrottleState()
    config = ThrottleConfig(cooldown_seconds=300)
    alert = make_alert()
    state.record(alert, now=1000.0)
    assert should_throttle(alert, state, config, now=1400.0) is False


# ---------------------------------------------------------------------------
# apply_throttle
# ---------------------------------------------------------------------------

def test_apply_throttle_passes_first_occurrence():
    state = ThrottleState()
    config = ThrottleConfig(cooldown_seconds=300)
    alerts = [make_alert()]
    result = apply_throttle(alerts, state, config, now=1000.0)
    assert len(result) == 1


def test_apply_throttle_suppresses_repeat_within_cooldown():
    state = ThrottleState()
    config = ThrottleConfig(cooldown_seconds=300)
    alert = make_alert()
    apply_throttle([alert], state, config, now=1000.0)
    result = apply_throttle([alert], state, config, now=1100.0)
    assert result == []


def test_apply_throttle_allows_after_cooldown():
    state = ThrottleState()
    config = ThrottleConfig(cooldown_seconds=300)
    alert = make_alert()
    apply_throttle([alert], state, config, now=1000.0)
    result = apply_throttle([alert], state, config, now=1400.0)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# load_throttle_config
# ---------------------------------------------------------------------------

def test_load_throttle_config_missing_file_returns_defaults(tmp_path):
    cfg = load_throttle_config(tmp_path / "nonexistent.yaml")
    assert cfg.cooldown_seconds == 300
    assert cfg.min_level == AlertLevel.WARNING


def test_load_throttle_config_parses_values(tmp_path):
    yaml_text = textwrap.dedent("""
        throttle:
          cooldown_seconds: 60
          min_level: critical
    """)
    p = tmp_path / "throttle.yaml"
    p.write_text(yaml_text)
    cfg = load_throttle_config(p)
    assert cfg.cooldown_seconds == 60
    assert cfg.min_level == AlertLevel.CRITICAL
