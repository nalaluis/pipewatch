"""Tests for pipewatch.schedule_runner."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.scheduler import SchedulerConfig
from pipewatch.schedule_runner import start_scheduler, stop_scheduler


MODULE = "pipewatch.schedule_runner"


def _make_report(has_critical: bool = False):
    alert = MagicMock()
    alert.level.name = "CRITICAL" if has_critical else "OK"
    report = MagicMock()
    report.alerts = [alert] if has_critical else []
    return report


@patch(f"{MODULE}.emit_alerts")
@patch(f"{MODULE}.emit_report")
@patch(f"{MODULE}.run_once", return_value=_make_report())
def test_start_scheduler_spawns_daemon_thread(mock_run, mock_emit, mock_alerts):
    cfg = SchedulerConfig(interval_seconds=0, max_runs=1)
    thread, state = start_scheduler("pipewatch.yaml", cfg)
    thread.join(timeout=3)
    assert not thread.is_alive()
    assert state.runs_completed == 1


@patch(f"{MODULE}.emit_alerts")
@patch(f"{MODULE}.emit_report")
@patch(f"{MODULE}.run_once", return_value=_make_report())
def test_start_scheduler_thread_is_daemon(mock_run, mock_emit, mock_alerts):
    cfg = SchedulerConfig(interval_seconds=0, max_runs=1)
    thread, _ = start_scheduler("pipewatch.yaml", cfg, daemon=True)
    assert thread.daemon is True
    thread.join(timeout=3)


@patch(f"{MODULE}.emit_alerts")
@patch(f"{MODULE}.emit_report")
@patch(f"{MODULE}.run_once", return_value=_make_report())
def test_stop_scheduler_signals_thread(mock_run, mock_emit, mock_alerts):
    cfg = SchedulerConfig(interval_seconds=60)  # long sleep
    stop_event = threading.Event()
    thread, state = start_scheduler("pipewatch.yaml", cfg, stop_event=stop_event)
    # Give it time to start one run
    time.sleep(0.05)
    stop_scheduler(thread, stop_event, timeout=3)
    assert not thread.is_alive()


@patch(f
@patch(f"{MODULE}.emit_report")
@patch(f"{MODULE}.run_once", return_value=_make_report())
def test_start_scheduler_calls_run_once_with_config_path(mock_run, mock_emit, mock_alerts):
    cfg = SchedulerConfig(interval_seconds=0, max_runs=2)
    thread, state = start_scheduler("custom.yaml", cfg)
    thread.join(timeout=3)
    assert mock_run.call_count == 2
    mock_run.assert_called_with("custom.yaml")
