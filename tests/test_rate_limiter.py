"""Tests for pipewatch.rate_limiter and pipewatch.rate_limiter_config."""
from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.rate_limiter import (
    RateLimiterConfig,
    RateLimiterState,
    record_run,
    seconds_since_last_run,
    should_run,
)
from pipewatch.rate_limiter_config import load_rate_limiter_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _state() -> RateLimiterState:
    return RateLimiterState()


def _cfg(**kwargs) -> RateLimiterConfig:
    return RateLimiterConfig(**kwargs)


# ---------------------------------------------------------------------------
# RateLimiterState
# ---------------------------------------------------------------------------

def test_state_initially_none():
    s = _state()
    assert s.last_run("pipe-a") is None


def test_state_records_time():
    s = _state()
    s.record("pipe-a", ts=100.0)
    assert s.last_run("pipe-a") == 100.0


def test_state_reset_clears_entry():
    s = _state()
    s.record("pipe-a", ts=100.0)
    s.reset("pipe-a")
    assert s.last_run("pipe-a") is None


def test_state_reset_all_clears_everything():
    s = _state()
    s.record("pipe-a", ts=1.0)
    s.record("pipe-b", ts=2.0)
    s.reset_all()
    assert s.last_run("pipe-a") is None
    assert s.last_run("pipe-b") is None


# ---------------------------------------------------------------------------
# seconds_since_last_run
# ---------------------------------------------------------------------------

def test_seconds_since_returns_none_when_never_run():
    s = _state()
    assert seconds_since_last_run(s, "pipe-a") is None


def test_seconds_since_returns_elapsed():
    s = _state()
    with patch("pipewatch.rate_limiter.time.monotonic", return_value=200.0):
        s.record("pipe-a", ts=190.0)
        elapsed = seconds_since_last_run(s, "pipe-a", per_pipeline=True)
    assert elapsed == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# should_run
# ---------------------------------------------------------------------------

def test_should_run_true_when_never_run():
    s = _state()
    cfg = _cfg(min_interval_seconds=30.0)
    assert should_run(s, cfg, "pipe-a") is True


def test_should_run_false_within_interval():
    s = _state()
    cfg = _cfg(min_interval_seconds=60.0)
    with patch("pipewatch.rate_limiter.time.monotonic", return_value=100.0):
        record_run(s, cfg, "pipe-a")
    with patch("pipewatch.rate_limiter.time.monotonic", return_value=130.0):
        assert should_run(s, cfg, "pipe-a") is False


def test_should_run_true_after_interval_elapsed():
    s = _state()
    cfg = _cfg(min_interval_seconds=60.0)
    with patch("pipewatch.rate_limiter.time.monotonic", return_value=100.0):
        record_run(s, cfg, "pipe-a")
    with patch("pipewatch.rate_limiter.time.monotonic", return_value=165.0):
        assert should_run(s, cfg, "pipe-a") is True


def test_global_key_shared_across_pipelines():
    s = _state()
    cfg = _cfg(min_interval_seconds=60.0, per_pipeline=False)
    with patch("pipewatch.rate_limiter.time.monotonic", return_value=100.0):
        record_run(s, cfg, "pipe-a")
    with patch("pipewatch.rate_limiter.time.monotonic", return_value=120.0):
        # pipe-b was never individually recorded but shares global clock
        assert should_run(s, cfg, "pipe-b") is False


# ---------------------------------------------------------------------------
# load_rate_limiter_config
# ---------------------------------------------------------------------------

def test_load_rate_limiter_config_defaults_when_missing(tmp_path):
    cfg = load_rate_limiter_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.min_interval_seconds == 60.0
    assert cfg.per_pipeline is True


def test_load_rate_limiter_config_parses_yaml(tmp_path):
    p = tmp_path / "rl.yaml"
    p.write_text(textwrap.dedent("""\
        rate_limiter:
          min_interval_seconds: 120
          per_pipeline: false
    """))
    cfg = load_rate_limiter_config(str(p))
    assert cfg.min_interval_seconds == 120.0
    assert cfg.per_pipeline is False


def test_load_rate_limiter_config_empty_yaml(tmp_path):
    p = tmp_path / "rl.yaml"
    p.write_text("")
    cfg = load_rate_limiter_config(str(p))
    assert cfg.min_interval_seconds == 60.0
