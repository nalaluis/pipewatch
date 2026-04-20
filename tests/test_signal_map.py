"""Tests for pipewatch/signal_map.py and pipewatch/signal_map_config.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus
from pipewatch.signal_map import (
    SignalMap,
    build_signal_map,
    format_signal_map,
)
from pipewatch.signal_map_config import SignalMapConfig, load_signal_map_config


def make_result(pipeline: str, status: PipelineStatus = PipelineStatus.HEALTHY) -> HealthResult:
    return HealthResult(pipeline=pipeline, status=status, violations=[])


def make_alert(
    pipeline: str,
    level: AlertLevel = AlertLevel.WARNING,
    message: str = "something wrong",
) -> Alert:
    return Alert(pipeline=pipeline, level=level, message=message)


# --- build_signal_map ---

def test_build_signal_map_empty_inputs():
    sm = build_signal_map([], [])
    assert sm.entries == {}


def test_build_signal_map_healthy_no_alerts():
    results = [make_result("pipe_a")]
    sm = build_signal_map(results, [])
    entry = sm.get("pipe_a")
    assert entry is not None
    assert entry.level == AlertLevel.OK
    assert entry.alert_count == 0


def test_build_signal_map_warning_alert():
    results = [make_result("pipe_b")]
    alerts = [make_alert("pipe_b", AlertLevel.WARNING, "high failure rate")]
    sm = build_signal_map(results, alerts)
    entry = sm.get("pipe_b")
    assert entry.level == AlertLevel.WARNING
    assert entry.alert_count == 1
    assert "high failure rate" in entry.reasons


def test_build_signal_map_critical_overrides_warning():
    results = [make_result("pipe_c")]
    alerts = [
        make_alert("pipe_c", AlertLevel.WARNING, "warn"),
        make_alert("pipe_c", AlertLevel.CRITICAL, "critical issue"),
    ]
    sm = build_signal_map(results, alerts)
    assert sm.get("pipe_c").level == AlertLevel.CRITICAL


def test_build_signal_map_multiple_pipelines():
    results = [make_result("a"), make_result("b"), make_result("c")]
    alerts = [make_alert("b", AlertLevel.CRITICAL, "down")]
    sm = build_signal_map(results, alerts)
    assert sm.get("a").level == AlertLevel.OK
    assert sm.get("b").level == AlertLevel.CRITICAL
    assert sm.get("c").level == AlertLevel.OK


def test_critical_pipelines_filter():
    results = [make_result("x"), make_result("y")]
    alerts = [make_alert("x", AlertLevel.CRITICAL)]
    sm = build_signal_map(results, alerts)
    assert "x" in sm.critical_pipelines()
    assert "y" not in sm.critical_pipelines()


def test_warning_pipelines_filter():
    results = [make_result("x"), make_result("y")]
    alerts = [make_alert("y", AlertLevel.WARNING)]
    sm = build_signal_map(results, alerts)
    assert "y" in sm.warning_pipelines()
    assert "x" not in sm.warning_pipelines()


# --- format_signal_map ---

def test_format_signal_map_empty():
    sm = SignalMap()
    assert format_signal_map(sm) == "(no pipelines)"


def test_format_signal_map_contains_pipeline_name():
    results = [make_result("my_pipe")]
    sm = build_signal_map(results, [])
    output = format_signal_map(sm)
    assert "my_pipe" in output


# --- load_signal_map_config ---

def test_load_signal_map_config_missing_returns_defaults(tmp_path):
    cfg = load_signal_map_config(str(tmp_path / "nope.yaml"))
    assert cfg.show_ok is True
    assert cfg.max_reasons == 5
    assert cfg.output_format == "text"


def test_load_signal_map_config_parses_yaml(tmp_path):
    p = tmp_path / "cfg.yaml"
    p.write_text("signal_map:\n  show_ok: false\n  max_reasons: 3\n  output_format: json\n")
    cfg = load_signal_map_config(str(p))
    assert cfg.show_ok is False
    assert cfg.max_reasons == 3
    assert cfg.output_format == "json"


def test_load_signal_map_config_empty_yaml_returns_defaults(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("")
    cfg = load_signal_map_config(str(p))
    assert isinstance(cfg, SignalMapConfig)
    assert cfg.max_reasons == 5
