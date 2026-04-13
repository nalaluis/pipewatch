"""Tests for pipewatch.eviction."""

from __future__ import annotations

import time
import pytest

from pipewatch.eviction import (
    EvictionConfig,
    EvictionResult,
    evict_by_age,
    evict_by_count,
    apply_eviction,
)


def _state(keys_and_ages: dict) -> dict:
    """Build a state dict from {key: age_seconds_ago} mapping."""
    now = time.time()
    return {k: now - age for k, age in keys_and_ages.items()}


# --- evict_by_age ---

def test_evict_by_age_removes_old_entries():
    state = _state({"pipe-a": 7200, "pipe-b": 100})
    cfg = EvictionConfig(max_age_seconds=3600.0)
    result = evict_by_age(state, cfg)
    assert "pipe-a" in result.evicted
    assert "pipe-b" in result.kept
    assert "pipe-a" not in state
    assert "pipe-b" in state


def test_evict_by_age_keeps_fresh_entries():
    state = _state({"pipe-a": 60, "pipe-b": 120})
    cfg = EvictionConfig(max_age_seconds=3600.0)
    result = evict_by_age(state, cfg)
    assert result.evicted_count == 0
    assert result.kept_count == 2


def test_evict_by_age_disabled_skips_eviction():
    state = _state({"pipe-a": 9999})
    cfg = EvictionConfig(max_age_seconds=3600.0, enabled=False)
    result = evict_by_age(state, cfg)
    assert result.evicted_count == 0
    assert "pipe-a" in state


def test_evict_by_age_empty_state():
    state = {}
    cfg = EvictionConfig()
    result = evict_by_age(state, cfg)
    assert result.evicted_count == 0
    assert result.kept_count == 0


# --- evict_by_count ---

def test_evict_by_count_removes_oldest_when_over_limit():
    now = time.time()
    state = {
        "pipe-a": now - 300,
        "pipe-b": now - 200,
        "pipe-c": now - 100,
    }
    cfg = EvictionConfig(max_entries=2)
    result = evict_by_count(state, cfg)
    assert "pipe-a" in result.evicted
    assert result.kept_count == 2
    assert "pipe-a" not in state


def test_evict_by_count_no_eviction_when_within_limit():
    now = time.time()
    state = {"pipe-a": now - 10, "pipe-b": now - 20}
    cfg = EvictionConfig(max_entries=5)
    result = evict_by_count(state, cfg)
    assert result.evicted_count == 0
    assert result.kept_count == 2


def test_evict_by_count_disabled_skips_eviction():
    now = time.time()
    state = {f"pipe-{i}": now - i for i in range(10)}
    cfg = EvictionConfig(max_entries=2, enabled=False)
    result = evict_by_count(state, cfg)
    assert result.evicted_count == 0


# --- apply_eviction ---

def test_apply_eviction_combines_both_policies():
    now = time.time()
    state = {
        "old-pipe": now - 9000,   # too old
        "pipe-b": now - 10,
        "pipe-c": now - 20,
        "pipe-d": now - 30,
    }
    cfg = EvictionConfig(max_age_seconds=3600.0, max_entries=2)
    result = apply_eviction(state, cfg, now=now)
    assert "old-pipe" in result.evicted
    assert result.kept_count <= 2


def test_eviction_result_str():
    r = EvictionResult(evicted=["a", "b"], kept=["c"])
    assert "evicted=2" in str(r)
    assert "kept=1" in str(r)
