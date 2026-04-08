"""Tests for pipewatch.alerts module."""

import io
from datetime import datetime

import pytest

from pipewatch.alerts import Alert, AlertLevel, build_alerts, emit_alerts
from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


def make_result(status: PipelineStatus, violations=None) -> HealthResult:
    return HealthResult(
        status=status,
        violations=violations or [],
        failure_rate=0.0,
        throughput=100.0,
    )


def test_no_alerts_for_healthy_pipeline():
    result = make_result(PipelineStatus.HEALTHY)
    alerts = build_alerts("my_pipeline", result)
    assert alerts == []


def test_warning_alert_generated():
    result = make_result(
        PipelineStatus.WARNING,
        violations=["failure_rate 0.12 exceeds warning threshold 0.10"],
    )
    alerts = build_alerts("etl_job", result)
    assert len(alerts) == 1
    assert alerts[0].level == AlertLevel.WARNING
    assert alerts[0].pipeline_name == "etl_job"
    assert "failure_rate" in alerts[0].message


def test_critical_alert_generated():
    result = make_result(
        PipelineStatus.CRITICAL,
        violations=["failure_rate 0.55 exceeds critical threshold 0.50"],
    )
    alerts = build_alerts("etl_job", result)
    assert len(alerts) == 1
    assert alerts[0].level == AlertLevel.CRITICAL


def test_multiple_violations_produce_multiple_alerts():
    result = make_result(
        PipelineStatus.CRITICAL,
        violations=["failure_rate too high", "throughput too low"],
    )
    alerts = build_alerts("pipeline_x", result)
    assert len(alerts) == 2


def test_alert_format_contains_expected_fields():
    alert = Alert(
        pipeline_name="demo",
        level=AlertLevel.WARNING,
        message="something is off",
        timestamp=datetime(2024, 6, 1, 12, 0, 0),
        metric_name="failure_rate",
        metric_value=0.123,
    )
    formatted = alert.format()
    assert "2024-06-01 12:00:00" in formatted
    assert "WARNING" in formatted
    assert "demo" in formatted
    assert "failure_rate=0.1230" in formatted


def test_emit_alerts_writes_to_sink():
    result = make_result(
        PipelineStatus.WARNING,
        violations=["throughput dropped below warning threshold"],
    )
    alerts = build_alerts("sink_test", result)
    buf = io.StringIO()
    emit_alerts(alerts, sink=buf)
    output = buf.getvalue()
    assert "sink_test" in output
    assert "WARNING" in output
