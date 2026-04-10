"""Tests for pipewatch.cooldown and pipewatch.cooldown_config."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.cooldown import (
    CooldownConfig,
    CooldownState,
    apply_cooldown,
    should_cooldown,
    window_for,
)
from pipewatch.cooldown_config import load_cooldown_config


def make_alert(pipeline: str = "test_pipe", reason: str = "failure_rate") -> Alert:
    return Alert(
        pipeline=pipeline,
        level=AlertLevel.WARNING,
        reason=reason,
        message=f"{pipeline} {reason} alert",
    )


# ---------------------------------------------------------------------------
# CooldownState
# ---------------------------------------------------------------------------

def test_cooldown_state_initially_none():
    state = CooldownState()
    assert state.last_alert_at("pipe::reason") is None
    assert state.seconds_since("pipe::reason") is None


def test_cooldown_state_records_time():
    state = CooldownState()
    before = time.monotonic()
    state.record("pipe::reason")
    after = time.monotonic()
    ts = state.last_alert_at("pipe::reason")
    assert ts is not None
    assert before <= ts <= after


def test_cooldown_state_reset_clears_entry():
    state = CooldownState()
    state.record("pipe::reason")
    state.reset("pipe::reason")
    assert state.last_alert_at("pipe::reason") is None


def test_seconds_since_increases_over_time():
    state = CooldownState()
    state.record("k")
    elapsed = state.seconds_since("k")
    assert elapsed is not None
    assert elapsed >= 0.0


# ---------------------------------------------------------------------------
# window_for
# ---------------------------------------------------------------------------

def test_window_for_returns_default():
    cfg = CooldownConfig(default_seconds=120.0)
    assert window_for("unknown", cfg) == 120.0


def test_window_for_returns_per_pipeline_override():
    cfg = CooldownConfig(default_seconds=120.0, per_pipeline={"orders_etl": 600.0})
    assert window_for("orders_etl", cfg) == 600.0


# ---------------------------------------------------------------------------
# should_cooldown / apply_cooldown
# ---------------------------------------------------------------------------

def test_should_cooldown_false_when_never_fired():
    state = CooldownState()
    cfg = CooldownConfig(default_seconds=300.0)
    alert = make_alert()
    assert should_cooldown(alert, state, cfg) is False


def test_should_cooldown_true_within_window():
    state = CooldownState()
    cfg = CooldownConfig(default_seconds=300.0)
    alert = make_alert()
    state.record("test_pipe::failure_rate")
    assert should_cooldown(alert, state, cfg) is True


def test_should_cooldown_false_after_window_expires():
    state = CooldownState()
    cfg = CooldownConfig(default_seconds=0.0)
    alert = make_alert()
    state.record("test_pipe::failure_rate")
    assert should_cooldown(alert, state, cfg) is False


def test_apply_cooldown_passes_first_alert():
    state = CooldownState()
    cfg = CooldownConfig(default_seconds=300.0)
    alerts = [make_alert()]
    result = apply_cooldown(alerts, state, cfg)
    assert len(result) == 1


def test_apply_cooldown_suppresses_duplicate_within_window():
    state = CooldownState()
    cfg = CooldownConfig(default_seconds=300.0)
    alert = make_alert()
    apply_cooldown([alert], state, cfg)  # first pass — records
    result = apply_cooldown([alert], state, cfg)  # second pass — suppressed
    assert result == []


def test_apply_cooldown_allows_different_pipelines():
    state = CooldownState()
    cfg = CooldownConfig(default_seconds=300.0)
    a1 = make_alert(pipeline="pipe_a")
    a2 = make_alert(pipeline="pipe_b")
    apply_cooldown([a1], state, cfg)
    result = apply_cooldown([a1, a2], state, cfg)
    assert len(result) == 1
    assert result[0].pipeline == "pipe_b"


# ---------------------------------------------------------------------------
# load_cooldown_config
# ---------------------------------------------------------------------------

def test_load_cooldown_config_missing_file_returns_defaults(tmp_path):
    cfg = load_cooldown_config(str(tmp_path / "no_such.yaml"))
    assert cfg.default_seconds == 300.0
    assert cfg.per_pipeline == {}


def test_load_cooldown_config_parses_yaml(tmp_path):
    yaml_content = "cooldown:\n  default_seconds: 60\n  per_pipeline:\n    orders_etl: 120\n"
    p = tmp_path / "cooldown.yaml"
    p.write_text(yaml_content)
    cfg = load_cooldown_config(str(p))
    assert cfg.default_seconds == 60.0
    assert cfg.per_pipeline["orders_etl"] == 120.0


def test_load_cooldown_config_empty_yaml_returns_defaults(tmp_path):
    p = tmp_path / "cooldown.yaml"
    p.write_text("")
    cfg = load_cooldown_config(str(p))
    assert cfg.default_seconds == 300.0
