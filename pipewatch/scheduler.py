"""Scheduler for periodic pipeline watch runs with configurable intervals."""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class SchedulerConfig:
    interval_seconds: int = 60
    max_runs: Optional[int] = None  # None = run forever
    jitter_seconds: int = 0


@dataclass
class SchedulerState:
    runs_completed: int = 0
    last_run_at: Optional[float] = None
    running: bool = False
    errors: list[str] = field(default_factory=list)


def make_scheduler_config(raw: dict) -> SchedulerConfig:
    """Build a SchedulerConfig from a raw config dict."""
    return SchedulerConfig(
        interval_seconds=int(raw.get("interval_seconds", 60)),
        max_runs=raw.get("max_runs"),
        jitter_seconds=int(raw.get("jitter_seconds", 0)),
    )


def run_scheduled(
    task: Callable[[], None],
    config: SchedulerConfig,
    state: Optional[SchedulerState] = None,
    stop_event: Optional[threading.Event] = None,
) -> SchedulerState:
    """Run *task* repeatedly according to *config*.

    Returns the final SchedulerState when the loop ends.
    """
    if state is None:
        state = SchedulerState()
    if stop_event is None:
        stop_event = threading.Event()

    state.running = True

    while not stop_event.is_set():
        try:
            task()
        except Exception as exc:  # noqa: BLE001
            state.errors.append(str(exc))

        state.runs_completed += 1
        state.last_run_at = time.time()

        if config.max_runs is not None and state.runs_completed >= config.max_runs:
            break

        sleep_duration = config.interval_seconds
        if config.jitter_seconds:
            import random
            sleep_duration += random.uniform(0, config.jitter_seconds)

        stop_event.wait(timeout=sleep_duration)

    state.running = False
    return state
