"""Retry policy configuration and execution for pipeline metric collection."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


@dataclass
class RetryPolicy:
    """Configuration for retry behaviour."""

    max_attempts: int = 3
    delay_seconds: float = 1.0
    backoff_factor: float = 2.0
    exceptions: tuple = field(default_factory=lambda: (Exception,))


@dataclass
class RetryResult:
    """Outcome of a retried call."""

    value: object
    attempts: int
    succeeded: bool
    last_error: Optional[str] = None


def with_retry(fn: Callable[[], T], policy: RetryPolicy) -> RetryResult:
    """Execute *fn* according to *policy*, returning a RetryResult.

    Raises the final exception only when all attempts are exhausted and
    the caller should propagate the failure; otherwise the RetryResult
    carries ``succeeded=False`` and the last error message.
    """
    delay = policy.delay_seconds
    last_error: Optional[str] = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            value = fn()
            return RetryResult(value=value, attempts=attempt, succeeded=True)
        except policy.exceptions as exc:  # type: ignore[misc]
            last_error = str(exc)
            if attempt < policy.max_attempts:
                time.sleep(delay)
                delay *= policy.backoff_factor

    return RetryResult(
        value=None,
        attempts=policy.max_attempts,
        succeeded=False,
        last_error=last_error,
    )


def make_retry_policy(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
) -> RetryPolicy:
    """Convenience factory used by config loaders."""
    return RetryPolicy(
        max_attempts=max_attempts,
        delay_seconds=delay_seconds,
        backoff_factor=backoff_factor,
    )
