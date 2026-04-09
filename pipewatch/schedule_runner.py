"""High-level helper that wires the scheduler to the watcher."""

from __future__ import annotations

import threading
from typing import Optional

from pipewatch.scheduler import SchedulerConfig, SchedulerState, run_scheduled
from pipewatch.watcher import run_once
from pipewatch.reporter import emit_report
from pipewatch.alerts import emit_alerts


def _make_task(config_path: str, output_format: str) -> callable:
    """Return a zero-arg callable that runs one watcher cycle."""

    def _task() -> None:
        report = run_once(config_path)
        emit_report(report, fmt=output_format)
        emit_alerts(report.alerts)

    return _task


def start_scheduler(
    config_path: str,
    scheduler_config: SchedulerConfig,
    output_format: str = "text",
    stop_event: Optional[threading.Event] = None,
    daemon: bool = True,
) -> tuple[threading.Thread, SchedulerState]:
    """Start the scheduler in a background thread.

    Returns the thread and the shared SchedulerState so callers can
    inspect progress or stop the loop via *stop_event*.
    """
    state = SchedulerState()
    if stop_event is None:
        stop_event = threading.Event()

    task = _make_task(config_path, output_format)

    thread = threading.Thread(
        target=run_scheduled,
        args=(task, scheduler_config, state, stop_event),
        daemon=daemon,
        name="pipewatch-scheduler",
    )
    thread.start()
    return thread, state


def stop_scheduler(
    thread: threading.Thread,
    stop_event: threading.Event,
    timeout: float = 5.0,
) -> None:
    """Signal the scheduler to stop and wait for the thread to finish."""
    stop_event.set()
    thread.join(timeout=timeout)
