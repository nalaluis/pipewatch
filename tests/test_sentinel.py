"""Tests for pipewatch.sentinel and pipewatch.sentinel_config."""
from __future__ import annotations

import time
import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.sentinel import (
    SentinelConfig,
    apply_sentinel,
    mark_triggered,
    record_hit,
    reset,
    should_trigger,
)
from pipewatch.sentinel_config import load_sentinel_config


def make_alert(pipeline: str = "pipe", level: AlertLevel = AlertLevel.CRITICAL) -> Alert:
    return Alert(pipeline=pipeline, level=level, metric="failure_rate", message="test")


@pytest.fixture(autouse=True)
def clean_state():
    reset()
    yield
    reset()


def test_record_hit_increments_count():
    cfg = SentinelConfig(threshold=3, window_seconds=60)
    now = time.time()
    record_hit("pipe", cfg, now)
    record_hit("pipe", cfg, now + 1)
    assert not should_trigger("pipe", cfg, now + 2)  # only 2 hits, threshold=3


def test_should_trigger_false_below_threshold():
    cfg = SentinelConfig(threshold=5, window_seconds=300)
    now = time.time()
    for _ in range(4):
        record_hit("pipe", cfg, now)
    assert should_trigger("pipe", cfg, now) is False


def test_should_trigger_true_at_threshold():
    cfg = SentinelConfig(threshold=3, window_seconds=300, cooldown_seconds=0)
    now = time.time()
    for _ in range(3):
        record_hit("pipe", cfg, now)
    assert should_trigger("pipe", cfg, now) is True


def test_should_trigger_false_during_cooldown():
    cfg = SentinelConfig(threshold=2, window_seconds=300, cooldown_seconds=120)
    now = time.time()
    for _ in range(3):
        record_hit("pipe", cfg, now)
    mark_triggered("pipe", now)
    assert should_trigger("pipe", cfg, now + 30) is False


def test_should_trigger_true_after_cooldown():
    cfg = SentinelConfig(threshold=2, window_seconds=300, cooldown_seconds=60)
    now = time.time()
    for _ in range(3):
        record_hit("pipe", cfg, now)
    mark_triggered("pipe", now)
    assert should_trigger("pipe", cfg, now + 61) is True


def test_hits_outside_window_pruned():
    cfg = SentinelConfig(threshold=2, window_seconds=60, cooldown_seconds=0)
    now = time.time()
    record_hit("pipe", cfg, now - 120)  # outside window
    record_hit("pipe", cfg, now - 90)   # outside window
    record_hit("pipe", cfg, now)         # inside
    assert should_trigger("pipe", cfg, now) is False


def test_reset_single_pipeline():
    cfg = SentinelConfig(threshold=1, window_seconds=300, cooldown_seconds=0)
    now = time.time()
    record_hit("pipe", cfg, now)
    reset("pipe")
    assert should_trigger("pipe", cfg, now) is False


def test_apply_sentinel_no_critical_alerts():
    cfg = SentinelConfig(threshold=2, window_seconds=300, cooldown_seconds=0)
    alerts = [make_alert(level=AlertLevel.WARNING)]
    result = apply_sentinel(alerts, cfg)
    assert result == []


def test_apply_sentinel_triggers_after_threshold():
    cfg = SentinelConfig(threshold=3, window_seconds=300, cooldown_seconds=0)
    now = time.time()
    # First two calls — no trigger yet
    apply_sentinel([make_alert()], cfg, now)
    apply_sentinel([make_alert()], cfg, now + 1)
    result = apply_sentinel([make_alert()], cfg, now + 2)
    assert len(result) == 1
    assert "SENTINEL" in result[0].message


def test_apply_sentinel_respects_cooldown():
    cfg = SentinelConfig(threshold=2, window_seconds=300, cooldown_seconds=120)
    now = time.time()
    apply_sentinel([make_alert()], cfg, now)
    apply_sentinel([make_alert()], cfg, now + 1)  # triggers
    # immediately after — still in cooldown
    result = apply_sentinel([make_alert()], cfg, now + 2)
    assert result == []


def test_load_sentinel_config_missing_returns_defaults(tmp_path):
    cfg = load_sentinel_config(str(tmp_path / "missing.yaml"))
    assert cfg.threshold == 3
    assert cfg.window_seconds == 300
    assert cfg.cooldown_seconds == 60


def test_load_sentinel_config_parses_yaml(tmp_path):
    p = tmp_path / "sentinel.yaml"
    p.write_text("sentinel:\n  threshold: 5\n  window_seconds: 120\n  cooldown_seconds: 30\n")
    cfg = load_sentinel_config(str(p))
    assert cfg.threshold == 5
    assert cfg.window_seconds == 120.0
    assert cfg.cooldown_seconds == 30.0
