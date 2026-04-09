"""Circuit breaker for pipeline alert suppression after repeated failures."""

from dataclasses import dataclass, field
from enum import Enum
from time import time
from typing import Dict, Optional


class CircuitState(str, Enum):
    CLOSED = "closed"      # normal operation
    OPEN = "open"          # tripped, suppressing alerts
    HALF_OPEN = "half_open"  # testing recovery


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5       # consecutive failures before opening
    recovery_timeout: float = 300.0  # seconds before trying half-open
    success_threshold: int = 2       # successes in half-open before closing


@dataclass
class CircuitBreakerState:
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: Optional[float] = None

    def reset(self) -> None:
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.opened_at = None


_states: Dict[str, CircuitBreakerState] = {}


def _get_state(pipeline: str) -> CircuitBreakerState:
    if pipeline not in _states:
        _states[pipeline] = CircuitBreakerState()
    return _states[pipeline]


def record_failure(pipeline: str, config: CircuitBreakerConfig) -> CircuitBreakerState:
    state = _get_state(pipeline)
    if state.state == CircuitState.HALF_OPEN:
        state.state = CircuitState.OPEN
        state.opened_at = time()
        state.failure_count += 1
    elif state.state == CircuitState.CLOSED:
        state.failure_count += 1
        if state.failure_count >= config.failure_threshold:
            state.state = CircuitState.OPEN
            state.opened_at = time()
    return state


def record_success(pipeline: str, config: CircuitBreakerConfig) -> CircuitBreakerState:
    state = _get_state(pipeline)
    if state.state == CircuitState.HALF_OPEN:
        state.success_count += 1
        if state.success_count >= config.success_threshold:
            state.reset()
    elif state.state == CircuitState.CLOSED:
        state.failure_count = 0
    return state


def is_open(pipeline: str, config: CircuitBreakerConfig) -> bool:
    state = _get_state(pipeline)
    if state.state == CircuitState.CLOSED:
        return False
    if state.state == CircuitState.OPEN:
        if state.opened_at and (time() - state.opened_at) >= config.recovery_timeout:
            state.state = CircuitState.HALF_OPEN
            state.success_count = 0
            return False
        return True
    return False  # HALF_OPEN allows a probe


def get_state(pipeline: str) -> Optional[CircuitBreakerState]:
    return _states.get(pipeline)


def reset_state(pipeline: str) -> None:
    if pipeline in _states:
        _states[pipeline].reset()
