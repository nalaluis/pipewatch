"""Tests for pipewatch.watcher orchestration logic."""

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.watcher import run_once, _collect_metric
from pipewatch.metrics import PipelineMetric, PipelineStatus


DEFAULT_CONFIG = {
    "thresholds": {"failure_rate_warning": 0.05, "failure_rate_critical": 0.2,
                   "throughput_warning": 10, "throughput_critical": 5},
    "pipelines": [
        {"id": "pipe-a", "_mock_total": 100, "_mock_failed": 1, "_mock_status": "ok"},
    ],
}


def _patch_load(config=None):
    return patch("pipewatch.watcher.load_config", return_value=config or DEFAULT_CONFIG)


def test_run_once_returns_report():
    with _patch_load():
        report = run_once("fake.yaml")
    assert "pipelines" in report
    assert "alerts" in report
    assert "generated_at" in report


def test_run_once_healthy_no_alerts():
    with _patch_load():
        report = run_once("fake.yaml")
    assert report["alerts"] == []


def test_run_once_critical_pipeline_produces_alert():
    config = {
        "thresholds": {"failure_rate_warning": 0.05, "failure_rate_critical": 0.1,
                       "throughput_warning": 10, "throughput_critical": 5},
        "pipelines": [
            {"id": "pipe-b", "_mock_total": 100, "_mock_failed": 50, "_mock_status": "ok"},
        ],
    }
    with _patch_load(config):
        report = run_once("fake.yaml")
    assert len(report["alerts"]) >= 1
    levels = {a["level"] for a in report["alerts"]}
    assert "critical" in levels


def test_collect_metric_returns_pipeline_metric():
    pipeline = {"id": "pipe-x", "_mock_total": 200, "_mock_failed": 4, "_mock_status": "ok"}
    metric = _collect_metric(pipeline)
    assert isinstance(metric, PipelineMetric)
    assert metric.pipeline_id == "pipe-x"
    assert metric.total == 200
    assert metric.failed == 4


def test_collect_metric_bad_status_returns_none():
    pipeline = {"id": "pipe-z", "_mock_total": 10, "_mock_failed": 0, "_mock_status": "invalid_status"}
    metric = _collect_metric(pipeline)
    assert metric is None


def test_run_once_skips_pipeline_with_no_metric():
    config = {
        "thresholds": {"failure_rate_warning": 0.05, "failure_rate_critical": 0.2,
                       "throughput_warning": 10, "throughput_critical": 5},
        "pipelines": [
            {"id": "bad-pipe", "_mock_total": 0, "_mock_failed": 0, "_mock_status": "broken"},
        ],
    }
    with _patch_load(config):
        report = run_once("fake.yaml")
    assert report["pipelines"] == []
