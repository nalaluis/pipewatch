"""Tests for pipewatch.heartbeat."""

from __future__ import annotations

import math
import time

import pytest

from pipewatch.heartbeat import (
    HeartbeatConfig,
    HeartbeatEntry,
    HeartbeatState,
    MissingHeartbeat,
)


@pytest.fixture()
def state() -> HeartbeatState:
    return HeartbeatState()


@pytest.fixture()
def cfg() -> HeartbeatConfig:
    return HeartbeatConfig(timeout_seconds=60.0, warning_seconds=30.0)


def test_record_stores_entry(state: HeartbeatState) -> None:
    state.record("pipe-a", now=1000.0)
    assert state.last_seen("pipe-a") == 1000.0


def test_last_seen_returns_none_for_unknown(state: HeartbeatState) -> None:
    assert state.last_seen("unknown") is None


def test_reset_removes_entry(state: HeartbeatState) -> None:
    state.record("pipe-a", now=1000.0)
    state.reset("pipe-a")
    assert state.last_seen("pipe-a") is None


def test_heartbeat_entry_age(cfg: HeartbeatConfig) -> None:
    entry = HeartbeatEntry(pipeline="p", last_seen=1000.0)
    assert entry.age_seconds(now=1010.0) == pytest.approx(10.0)


def test_heartbeat_entry_is_alive_within_timeout() -> None:
    entry = HeartbeatEntry(pipeline="p", last_seen=1000.0)
    assert entry.is_alive(timeout=60.0, now=1050.0) is True


def test_heartbeat_entry_is_not_alive_past_timeout() -> None:
    entry = HeartbeatEntry(pipeline="p", last_seen=1000.0)
    assert entry.is_alive(timeout=60.0, now=1070.0) is False


def test_check_missing_returns_empty_when_all_fresh(
    state: HeartbeatState, cfg: HeartbeatConfig
) -> None:
    state.record("pipe-a", now=1000.0)
    result = state.check_missing(["pipe-a"], cfg, now=1010.0)
    assert result == []


def test_check_missing_warning_only(
    state: HeartbeatState, cfg: HeartbeatConfig
) -> None:
    state.record("pipe-a", now=1000.0)
    result = state.check_missing(["pipe-a"], cfg, now=1040.0)
    assert len(result) == 1
    m = result[0]
    assert m.warning is True
    assert m.critical is False


def test_check_missing_critical(
    state: HeartbeatState, cfg: HeartbeatConfig
) -> None:
    state.record("pipe-a", now=1000.0)
    result = state.check_missing(["pipe-a"], cfg, now=1070.0)
    assert len(result) == 1
    m = result[0]
    assert m.critical is True


def test_check_missing_never_seen_is_critical(
    state: HeartbeatState, cfg: HeartbeatConfig
) -> None:
    result = state.check_missing(["ghost-pipe"], cfg)
    assert len(result) == 1
    assert result[0].critical is True
    assert math.isinf(result[0].age_seconds)


def test_missing_heartbeat_str_warning() -> None:
    m = MissingHeartbeat(pipeline="p", age_seconds=35.0, warning=True, critical=False)
    assert "WARNING" in str(m)
    assert "p" in str(m)


def test_missing_heartbeat_str_critical() -> None:
    m = MissingHeartbeat(pipeline="p", age_seconds=65.0, warning=True, critical=True)
    assert "CRITICAL" in str(m)
