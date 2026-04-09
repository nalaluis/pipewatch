"""Tests for pipewatch.scheduler."""

import threading
import time

import pytest

from pipewatch.scheduler import (
    SchedulerConfig,
    SchedulerState,
    make_scheduler_config,
    run_scheduled,
)


# ---------------------------------------------------------------------------
# make_scheduler_config
# ---------------------------------------------------------------------------

def test_make_scheduler_config_defaults():
    cfg = make_scheduler_config({})
    assert cfg.interval_seconds == 60
    assert cfg.max_runs is None
    assert cfg.jitter_seconds == 0


def test_make_scheduler_config_custom():
    cfg = make_scheduler_config({"interval_seconds": 30, "max_runs": 5, "jitter_seconds": 2})
    assert cfg.interval_seconds == 30
    assert cfg.max_runs == 5
    assert cfg.jitter_seconds == 2


# ---------------------------------------------------------------------------
# run_scheduled
# ---------------------------------------------------------------------------

def test_run_scheduled_calls_task_max_runs_times():
    calls = []
    cfg = SchedulerConfig(interval_seconds=0, max_runs=3)
    run_scheduled(lambda: calls.append(1), cfg)
    assert len(calls) == 3


def test_run_scheduled_state_tracks_runs():
    cfg = SchedulerConfig(interval_seconds=0, max_runs=4)
    state = run_scheduled(lambda: None, cfg)
    assert state.runs_completed == 4
    assert state.running is False
    assert state.last_run_at is not None


def test_run_scheduled_stop_event_halts_loop():
    stop = threading.Event()
    calls = []

    def task():
        calls.append(1)
        if len(calls) >= 2:
            stop.set()

    cfg = SchedulerConfig(interval_seconds=0)
    state = run_scheduled(task, cfg, stop_event=stop)
    assert state.runs_completed >= 2
    assert state.running is False


def test_run_scheduled_captures_task_errors():
    def bad_task():
        raise ValueError("boom")

    cfg = SchedulerConfig(interval_seconds=0, max_runs=2)
    state = run_scheduled(bad_task, cfg)
    assert len(state.errors) == 2
    assert "boom" in state.errors[0]


def test_run_scheduled_accepts_external_state():
    existing_state = SchedulerState(runs_completed=10)
    cfg = SchedulerConfig(interval_seconds=0, max_runs=2)
    state = run_scheduled(lambda: None, cfg, state=existing_state)
    assert state.runs_completed == 12
