"""Tests for circuit breaker logic."""

import pytest
from unittest.mock import patch
from time import time

from pipewatch.circuit_breaker import (
    CircuitBreakerConfig,
    CircuitBreakerState,
    CircuitState,
    record_failure,
    record_success,
    is_open,
    reset_state,
    get_state,
)


@pytest.fixture(autouse=True)
def clean_state():
    """Reset circuit breaker state between tests."""
    from pipewatch import circuit_breaker
    circuit_breaker._states.clear()
    yield
    circuit_breaker._states.clear()


def cfg(**kwargs) -> CircuitBreakerConfig:
    defaults = dict(failure_threshold=3, recovery_timeout=60.0, success_threshold=2)
    defaults.update(kwargs)
    return CircuitBreakerConfig(**defaults)


def test_initial_state_is_closed():
    assert not is_open("pipe", cfg())
    state = get_state("pipe")
    assert state is not None
    assert state.state == CircuitState.CLOSED


def test_circuit_opens_after_threshold_failures():
    c = cfg(failure_threshold=3)
    record_failure("pipe", c)
    record_failure("pipe", c)
    assert not is_open("pipe", c)
    record_failure("pipe", c)
    assert is_open("pipe", c)


def test_success_resets_failure_count():
    c = cfg(failure_threshold=3)
    record_failure("pipe", c)
    record_failure("pipe", c)
    record_success("pipe", c)
    state = get_state("pipe")
    assert state.failure_count == 0
    assert state.state == CircuitState.CLOSED


def test_circuit_transitions_to_half_open_after_timeout():
    c = cfg(failure_threshold=1, recovery_timeout=1.0)
    record_failure("pipe", c)
    assert is_open("pipe", c)
    state = get_state("pipe")
    state.opened_at = time() - 2.0  # simulate timeout elapsed
    assert not is_open("pipe", c)
    assert state.state == CircuitState.HALF_OPEN


def test_half_open_closes_after_success_threshold():
    c = cfg(failure_threshold=1, recovery_timeout=1.0, success_threshold=2)
    record_failure("pipe", c)
    state = get_state("pipe")
    state.state = CircuitState.HALF_OPEN
    record_success("pipe", c)
    assert state.state == CircuitState.HALF_OPEN
    record_success("pipe", c)
    assert state.state == CircuitState.CLOSED


def test_half_open_reopens_on_failure():
    c = cfg(failure_threshold=1, recovery_timeout=60.0)
    record_failure("pipe", c)
    state = get_state("pipe")
    state.state = CircuitState.HALF_OPEN
    record_failure("pipe", c)
    assert state.state == CircuitState.OPEN


def test_reset_state_clears_circuit():
    c = cfg(failure_threshold=1)
    record_failure("pipe", c)
    assert is_open("pipe", c)
    reset_state("pipe")
    assert not is_open("pipe", c)
    state = get_state("pipe")
    assert state.state == CircuitState.CLOSED


def test_get_state_returns_none_for_unknown_pipeline():
    assert get_state("unknown-pipe") is None
