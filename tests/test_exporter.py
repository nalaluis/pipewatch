"""Tests for pipewatch.exporter."""

from __future__ import annotations

import csv
import io
import json
from unittest.mock import MagicMock

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.exporter import export_csv, export_json, export_report
from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.reporter import Report


def _make_metric(name: str = "pipe", failed: int = 1, total: int = 10, processed: int = 100) -> PipelineMetric:
    return PipelineMetric(name=name, failed=failed, total=total, processed=processed, window_seconds=60)


def _make_result(name: str = "pipe", status: PipelineStatus = PipelineStatus.HEALTHY) -> HealthResult:
    return HealthResult(metric=_make_metric(name), status=status, violations=[])


def _make_alert(pipeline: str = "pipe", level: AlertLevel = AlertLevel.WARNING) -> Alert:
    return Alert(pipeline=pipeline, level=level, message="test alert")


def _make_report(results=None, alerts=None) -> Report:
    return Report(
        timestamp="2024-01-01T00:00:00",
        results=results or [],
        alerts=alerts or [],
        summary=None,
    )


def test_export_json_structure():
    report = _make_report(results=[_make_result()], alerts=[_make_alert()])
    raw = export_json(report)
    data = json.loads(raw)
    assert "timestamp" in data
    assert "pipelines" in data
    assert "alerts" in data
    assert data["pipelines"][0]["name"] == "pipe"


def test_export_json_alert_fields():
    alert = _make_alert(level=AlertLevel.CRITICAL)
    report = _make_report(alerts=[alert])
    data = json.loads(export_json(report))
    assert data["alerts"][0]["level"] == "critical"
    assert data["alerts"][0]["message"] == "test alert"


def test_export_json_empty_report():
    report = _make_report()
    data = json.loads(export_json(report))
    assert data["pipelines"] == []
    assert data["alerts"] == []


def test_export_csv_has_header_and_row():
    report = _make_report(results=[_make_result("etl")])
    raw = export_csv(report)
    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["name"] == "etl"
    assert rows[0]["status"] == "healthy"


def test_export_csv_violations_pipe_separated():
    result = HealthResult(
        metric=_make_metric(),
        status=PipelineStatus.CRITICAL,
        violations=["failure_rate too high", "throughput too low"],
    )
    report = _make_report(results=[result])
    raw = export_csv(report)
    reader = csv.DictReader(io.StringIO(raw))
    row = next(reader)
    assert "failure_rate too high" in row["violations"]
    assert "|" in row["violations"]


def test_export_report_dispatches_json():
    report = _make_report()
    result = export_report(report, fmt="json")
    assert result.startswith("{")


def test_export_report_dispatches_csv():
    report = _make_report(results=[_make_result()])
    result = export_report(report, fmt="csv")
    assert "name" in result


def test_export_report_raises_on_unknown_format():
    report = _make_report()
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_report(report, fmt="xml")  # type: ignore[arg-type]
