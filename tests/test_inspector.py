"""Tests for pipewatch.inspector and pipewatch.inspector_config."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult
from pipewatch.alerts import Alert, AlertLevel
from pipewatch.inspector import (
    InspectionReport,
    inspect_pipeline,
    format_inspection,
)
from pipewatch.inspector_config import InspectorConfig, load_inspector_config


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def make_metric(failure_rate: float = 0.0, throughput: float = 100.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline="test-pipe",
        success=int(throughput * (1 - failure_rate)),
        failure=int(throughput * failure_rate),
        duration_seconds=1.0,
    )


def make_health(status: PipelineStatus = PipelineStatus.HEALTHY) -> HealthResult:
    return HealthResult(status=status, violations=[])


def make_alert(level: AlertLevel = AlertLevel.WARNING, msg: str = "test alert") -> Alert:
    a = MagicMock(spec=Alert)
    a.level = level
    a.message = msg
    return a


# ---------------------------------------------------------------------------
# InspectionReport properties
# ---------------------------------------------------------------------------

def test_has_anomalies_false_when_empty():
    r = InspectionReport(
        pipeline="p", metric=make_metric(), health=make_health(), alerts=[]
    )
    assert r.has_anomalies is False


def test_has_anomalies_true_when_present():
    anomaly = MagicMock()
    r = InspectionReport(
        pipeline="p", metric=make_metric(), health=make_health(),
        alerts=[], anomalies=[anomaly]
    )
    assert r.has_anomalies is True


def test_critical_alerts_filters_correctly():
    alerts = [
        make_alert(AlertLevel.CRITICAL, "crit"),
        make_alert(AlertLevel.WARNING, "warn"),
    ]
    r = InspectionReport(pipeline="p", metric=make_metric(), health=make_health(), alerts=alerts)
    assert len(r.critical_alerts) == 1
    assert r.critical_alerts[0].message == "crit"


def test_warning_alerts_filters_correctly():
    alerts = [
        make_alert(AlertLevel.CRITICAL, "crit"),
        make_alert(AlertLevel.WARNING, "warn"),
    ]
    r = InspectionReport(pipeline="p", metric=make_metric(), health=make_health(), alerts=alerts)
    assert len(r.warning_alerts) == 1
    assert r.warning_alerts[0].message == "warn"


# ---------------------------------------------------------------------------
# inspect_pipeline
# ---------------------------------------------------------------------------

def test_inspect_pipeline_returns_inspection_report():
    metric = make_metric()
    health = make_health()
    report = inspect_pipeline("my-pipe", metric, health, [])
    assert isinstance(report, InspectionReport)
    assert report.pipeline == "my-pipe"


def test_inspect_pipeline_defaults_anomalies_to_empty():
    report = inspect_pipeline("p", make_metric(), make_health(), [])
    assert report.anomalies == []


def test_inspect_pipeline_passes_score_and_trend():
    score = MagicMock()
    trend = MagicMock()
    report = inspect_pipeline("p", make_metric(), make_health(), [], score=score, trend=trend)
    assert report.score is score
    assert report.trend is trend


# ---------------------------------------------------------------------------
# format_inspection
# ---------------------------------------------------------------------------

def test_format_inspection_contains_pipeline_name():
    report = inspect_pipeline("alpha", make_metric(), make_health(), [])
    text = format_inspection(report)
    assert "alpha" in text


def test_format_inspection_no_alerts_says_none():
    report = inspect_pipeline("p", make_metric(), make_health(), [])
    text = format_inspection(report)
    assert "none" in text.lower()


def test_format_inspection_lists_alerts():
    alerts = [make_alert(AlertLevel.CRITICAL, "disk full")]
    report = inspect_pipeline("p", make_metric(), make_health(), alerts)
    text = format_inspection(report)
    assert "disk full" in text
    assert "CRITICAL" in text


def test_format_inspection_includes_score_when_present():
    score = MagicMock()
    score.score = 87.5
    score.grade = "B"
    report = inspect_pipeline("p", make_metric(), make_health(), [], score=score)
    text = format_inspection(report)
    assert "87.5" in text
    assert "B" in text


# ---------------------------------------------------------------------------
# InspectorConfig / load_inspector_config
# ---------------------------------------------------------------------------

def test_inspector_config_defaults():
    cfg = InspectorConfig()
    assert cfg.include_score is True
    assert cfg.include_trend is True
    assert cfg.include_anomalies is True
    assert cfg.pipelines == []


def test_load_inspector_config_missing_file_returns_defaults(tmp_path):
    cfg = load_inspector_config(str(tmp_path / "nonexistent.yaml"))
    assert isinstance(cfg, InspectorConfig)
    assert cfg.include_score is True


def test_load_inspector_config_parses_yaml(tmp_path):
    p = tmp_path / "pipewatch-inspector.yaml"
    p.write_text(
        "inspector:\n"
        "  include_score: false\n"
        "  include_trend: true\n"
        "  include_anomalies: false\n"
        "  pipelines:\n"
        "    - pipe-a\n"
        "    - pipe-b\n"
    )
    cfg = load_inspector_config(str(p))
    assert cfg.include_score is False
    assert cfg.include_trend is True
    assert cfg.include_anomalies is False
    assert cfg.pipelines == ["pipe-a", "pipe-b"]
