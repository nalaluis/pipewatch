"""Tests for pipewatch.flusher."""

from __future__ import annotations

import pytest

from pipewatch.flusher import (
    FlushResult,
    FlushTarget,
    clear_registry,
    flush_all,
    register_flush_target,
    registered_names,
)


@pytest.fixture(autouse=True)
def clean_registry():
    clear_registry()
    yield
    clear_registry()


# ---------------------------------------------------------------------------
# FlushResult helpers
# ---------------------------------------------------------------------------

def test_flush_result_success_count():
    r = FlushResult(flushed=["a", "b"], errors=[])
    assert r.success_count == 2


def test_flush_result_error_count():
    r = FlushResult(flushed=[], errors=["x: boom"])
    assert r.error_count == 1


def test_flush_result_str_format():
    r = FlushResult(flushed=["a"], errors=["b: err"])
    assert "flushed=1" in str(r)
    assert "errors=1" in str(r)


# ---------------------------------------------------------------------------
# register / registered_names
# ---------------------------------------------------------------------------

def test_register_adds_target():
    register_flush_target("store_a", lambda: None)
    assert "store_a" in registered_names()


def test_register_multiple_targets():
    register_flush_target("alpha", lambda: None)
    register_flush_target("beta", lambda: None)
    names = registered_names()
    assert "alpha" in names
    assert "beta" in names


def test_clear_registry_empties_list():
    register_flush_target("x", lambda: None)
    clear_registry()
    assert registered_names() == []


# ---------------------------------------------------------------------------
# flush_all — global registry
# ---------------------------------------------------------------------------

def test_flush_all_calls_flush_fn():
    called = []
    register_flush_target("s1", lambda: called.append("s1"))
    flush_all()
    assert called == ["s1"]


def test_flush_all_returns_flushed_names():
    register_flush_target("pipe_state", lambda: None)
    result = flush_all()
    assert "pipe_state" in result.flushed
    assert result.error_count == 0


def test_flush_all_captures_errors_without_raising():
    def bad():
        raise RuntimeError("disk full")

    register_flush_target("broken", bad)
    result = flush_all()
    assert result.error_count == 1
    assert "broken" in result.errors[0]
    assert "disk full" in result.errors[0]


def test_flush_all_continues_after_error():
    called = []

    def bad():
        raise ValueError("oops")

    register_flush_target("bad", bad)
    register_flush_target("good", lambda: called.append("good"))
    result = flush_all()
    assert "good" in result.flushed
    assert result.error_count == 1


# ---------------------------------------------------------------------------
# flush_all — explicit target list
# ---------------------------------------------------------------------------

def test_flush_all_with_explicit_targets_ignores_registry():
    registry_called = []
    explicit_called = []

    register_flush_target("registry", lambda: registry_called.append(1))
    targets = [FlushTarget(name="explicit", flush_fn=lambda: explicit_called.append(1))]
    result = flush_all(targets=targets)

    assert registry_called == []
    assert explicit_called == [1]
    assert "explicit" in result.flushed
