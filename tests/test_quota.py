"""Tests for pipewatch.quota and pipewatch.quota_config."""

from __future__ import annotations

from time import time
from typing import List

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.quota import (
    QuotaConfig,
    QuotaState,
    QuotaWindow,
    apply_quota,
    is_quota_exceeded,
    record_alert,
    reset_state,
)


def make_alert(pipeline: str = "pipe-a", level: AlertLevel = AlertLevel.WARNING) -> Alert:
    return Alert(pipeline=pipeline, level=level, message=f"{pipeline} alert")


@pytest.fixture(autouse=True)
def clean_state():
    reset_state()
    yield
    reset_state()


# --- QuotaWindow ---

def test_quota_window_starts_empty():
    w = QuotaWindow()
    assert w.count() == 0


def test_quota_window_records_and_counts():
    w = QuotaWindow()
    w.record(1000.0)
    w.record(1001.0)
    assert w.count() == 2


def test_quota_window_prune_removes_old_entries():
    w = QuotaWindow()
    now = time()
    w.record(now - 7200)  # 2 hours ago
    w.record(now - 100)   # recent
    w.prune(window_seconds=3600, now=now)
    assert w.count() == 1


# --- QuotaState ---

def test_quota_state_count_returns_zero_initially():
    state = QuotaState()
    cfg = QuotaConfig(max_alerts=5, window_seconds=60)
    assert state.count("pipe-x", cfg.window_seconds) == 0


def test_quota_state_records_and_counts():
    state = QuotaState()
    now = time()
    state.record("pipe-x", now)
    state.record("pipe-x", now + 1)
    assert state.count("pipe-x", 3600, now + 2) == 2


def test_quota_state_reset_clears_pipeline():
    state = QuotaState()
    state.record("pipe-x")
    state.reset("pipe-x")
    assert state.count("pipe-x", 3600) == 0


# --- is_quota_exceeded / record_alert (module-level state) ---

def test_quota_not_exceeded_initially():
    cfg = QuotaConfig(max_alerts=3, window_seconds=60)
    assert not is_quota_exceeded("pipe-a", cfg)


def test_quota_exceeded_after_max_alerts():
    cfg = QuotaConfig(max_alerts=3, window_seconds=60)
    now = time()
    for _ in range(3):
        record_alert("pipe-a", now)
    assert is_quota_exceeded("pipe-a", cfg, now)


# --- apply_quota ---

def test_apply_quota_allows_alerts_under_limit():
    cfg = QuotaConfig(max_alerts=5, window_seconds=60)
    alerts = [make_alert("pipe-a") for _ in range(3)]
    result = apply_quota(alerts, cfg)
    assert len(result) == 3


def test_apply_quota_blocks_alerts_over_limit():
    cfg = QuotaConfig(max_alerts=2, window_seconds=60)
    alerts = [make_alert("pipe-a") for _ in range(5)]
    result = apply_quota(alerts, cfg)
    assert len(result) == 2


def test_apply_quota_tracks_per_pipeline():
    cfg = QuotaConfig(max_alerts=2, window_seconds=60)
    alerts = [
        make_alert("pipe-a"),
        make_alert("pipe-a"),
        make_alert("pipe-a"),  # should be blocked
        make_alert("pipe-b"),
        make_alert("pipe-b"),
    ]
    result = apply_quota(alerts, cfg)
    names = [a.pipeline for a in result]
    assert names.count("pipe-a") == 2
    assert names.count("pipe-b") == 2


def test_apply_quota_empty_list_returns_empty():
    cfg = QuotaConfig()
    assert apply_quota([], cfg) == []
