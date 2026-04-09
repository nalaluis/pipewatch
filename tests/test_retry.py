"""Tests for pipewatch.retry and pipewatch.retry_config."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.retry import RetryPolicy, RetryResult, make_retry_policy, with_retry
from pipewatch.retry_config import load_retry_config


# ---------------------------------------------------------------------------
# with_retry
# ---------------------------------------------------------------------------

def test_with_retry_succeeds_first_attempt():
    policy = make_retry_policy(max_attempts=3, delay_seconds=0)
    result = with_retry(lambda: 42, policy)
    assert result.succeeded is True
    assert result.value == 42
    assert result.attempts == 1
    assert result.last_error is None


def test_with_retry_retries_on_failure():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return "ok"

    policy = RetryPolicy(max_attempts=3, delay_seconds=0, backoff_factor=1.0)
    result = with_retry(flaky, policy)
    assert result.succeeded is True
    assert result.attempts == 3
    assert result.value == "ok"


def test_with_retry_exhausts_attempts():
    policy = RetryPolicy(max_attempts=2, delay_seconds=0, backoff_factor=1.0)
    result = with_retry(lambda: (_ for _ in ()).throw(RuntimeError("boom")), policy)
    assert result.succeeded is False
    assert result.attempts == 2
    assert "boom" in (result.last_error or "")


def test_with_retry_no_sleep_when_zero_delay():
    """Ensure time.sleep is not called when delay_seconds=0."""
    policy = RetryPolicy(max_attempts=3, delay_seconds=0, backoff_factor=1.0)
    calls = {"n": 0}

    def always_fail():
        calls["n"] += 1
        raise OSError("fail")

    with patch("pipewatch.retry.time.sleep") as mock_sleep:
        with_retry(always_fail, policy)

    mock_sleep.assert_not_called()


def test_with_retry_applies_backoff():
    policy = RetryPolicy(max_attempts=3, delay_seconds=1.0, backoff_factor=2.0)
    slept: list[float] = []

    def always_fail():
        raise ValueError("x")

    with patch("pipewatch.retry.time.sleep", side_effect=lambda s: slept.append(s)):
        with_retry(always_fail, policy)

    assert slept == [1.0, 2.0]


# ---------------------------------------------------------------------------
# make_retry_policy
# ---------------------------------------------------------------------------

def test_make_retry_policy_defaults():
    p = make_retry_policy()
    assert p.max_attempts == 3
    assert p.delay_seconds == 1.0
    assert p.backoff_factor == 2.0


# ---------------------------------------------------------------------------
# load_retry_config
# ---------------------------------------------------------------------------

def test_load_retry_config_missing_file_returns_defaults(tmp_path):
    policy = load_retry_config(tmp_path / "nonexistent.yaml")
    assert policy.max_attempts == 3
    assert policy.delay_seconds == 1.0


def test_load_retry_config_parses_yaml(tmp_path):
    cfg = tmp_path / "retry.yaml"
    cfg.write_text("retry:\n  max_attempts: 5\n  delay_seconds: 0.5\n  backoff_factor: 1.5\n")
    policy = load_retry_config(cfg)
    assert policy.max_attempts == 5
    assert policy.delay_seconds == 0.5
    assert policy.backoff_factor == 1.5


def test_load_retry_config_empty_yaml_returns_defaults(tmp_path):
    cfg = tmp_path / "retry.yaml"
    cfg.write_text("")
    policy = load_retry_config(cfg)
    assert policy.max_attempts == 3
