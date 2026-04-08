"""Tests for pipewatch.reporter."""

from __future__ import annotations

import io
import json

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.reporter import build_report, emit_report, render_text


def make_metric(success: int = 90, failed: int = 10, duration: float = 10.0) -> PipelineMetric:
    return PipelineMetric(success_count=success, failure_count=failed, duration_seconds=duration)


def make_result(healthy: bool = True, status: PipelineStatus = PipelineStatus.OK, violations=None) -> HealthResult:
    return HealthResult(healthy=healthy, status=status, violations=violations or [])


def make_alert(level: AlertLevel = AlertLevel.WARNING, metric: str = "failure_rate", value: float = 0.15) -> Alert:
    return Alert(level=level, message=f"{metric} is {value:.0%}", metric=metric, value=value)


# ---------------------------------------------------------------------------
# build_report
# ---------------------------------------------------------------------------

def test_build_report_structure():
    metric = make_metric()
    result = make_result()
    report = build_report("orders", metric, result, [])

    assert report["pipeline"] == "orders"
    assert "timestamp" in report
    assert "metrics" in report
    assert report["healthy"] is True
    assert report["alerts"] == []
    assert report["violations"] == []


def test_build_report_includes_alerts():
    metric = make_metric(success=80, failed=20)
    result = make_result(healthy=False, status=PipelineStatus.CRITICAL, violations=["failure_rate too high"])
    alert = make_alert(level=AlertLevel.CRITICAL, value=0.20)
    report = build_report("payments", metric, result, [alert])

    assert len(report["alerts"]) == 1
    assert report["alerts"][0]["level"] == "critical"
    assert report["status"] == "critical"
    assert "failure_rate too high" in report["violations"]


# ---------------------------------------------------------------------------
# render_text
# ---------------------------------------------------------------------------

def test_render_text_healthy():
    report = build_report("etl", make_metric(), make_result(), [])
    text = render_text(report)
    assert "etl" in text
    assert "Healthy: True" in text
    assert "Alerts" not in text


def test_render_text_shows_alerts():
    alert = make_alert()
    report = build_report("etl", make_metric(), make_result(healthy=False, status=PipelineStatus.WARNING, violations=["x"]), [alert])
    text = render_text(report)
    assert "Alerts" in text
    assert "WARNING" in text


# ---------------------------------------------------------------------------
# emit_report
# ---------------------------------------------------------------------------

def test_emit_report_text_format():
    buf = io.StringIO()
    report = build_report("stream", make_metric(), make_result(), [])
    emit_report(report, fmt="text", output=buf)
    output = buf.getvalue()
    assert "stream" in output
    assert output.endswith("\n")


def test_emit_report_json_format():
    buf = io.StringIO()
    report = build_report("stream", make_metric(), make_result(), [])
    emit_report(report, fmt="json", output=buf)
    parsed = json.loads(buf.getvalue())
    assert parsed["pipeline"] == "stream"
    assert "metrics" in parsed
