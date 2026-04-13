"""Tests for pipewatch.rollup and pipewatch.rollup_config."""

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from pipewatch.health import HealthResult, HealthThresholds
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.rollup import (
    RollupBucket,
    RollupReport,
    format_rollup,
    rollup,
    rollup_by_label,
)
from pipewatch.rollup_config import RollupConfig, load_rollup_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metric(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=name,
        status=status,
        failure_rate=failure_rate,
        throughput=throughput,
        processed=int(throughput),
        failed=int(failure_rate * throughput),
    )


def _make_result(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
    throughput: float = 100.0,
) -> HealthResult:
    metric = _make_metric(name, status, failure_rate, throughput)
    thresholds = HealthThresholds()
    return HealthResult(metric=metric, thresholds=thresholds, violations=[])


# ---------------------------------------------------------------------------
# rollup()
# ---------------------------------------------------------------------------

def test_rollup_empty_returns_zero_bucket():
    bucket = rollup([], window_label="1h")
    assert bucket.total == 0
    assert bucket.healthy == 0
    assert bucket.avg_failure_rate == 0.0


def test_rollup_counts_statuses():
    results = [
        _make_result(status=PipelineStatus.HEALTHY),
        _make_result(status=PipelineStatus.WARNING),
        _make_result(status=PipelineStatus.CRITICAL),
    ]
    bucket = rollup(results, window_label="6h")
    assert bucket.total == 3
    assert bucket.healthy == 1
    assert bucket.warning == 1
    assert bucket.critical == 1


def test_rollup_averages_failure_rate():
    results = [
        _make_result(failure_rate=0.2),
        _make_result(failure_rate=0.4),
    ]
    bucket = rollup(results, window_label="24h")
    assert abs(bucket.avg_failure_rate - 0.3) < 1e-6


def test_rollup_averages_throughput():
    results = [
        _make_result(throughput=50.0),
        _make_result(throughput=150.0),
    ]
    bucket = rollup(results, window_label="1h")
    assert abs(bucket.avg_throughput - 100.0) < 1e-6


def test_rollup_str_contains_label():
    bucket = rollup([_make_result()], window_label="my-window")
    assert "my-window" in str(bucket)


# ---------------------------------------------------------------------------
# rollup_by_label()
# ---------------------------------------------------------------------------

def test_rollup_by_label_creates_buckets_per_label():
    labeled = {
        "1h": [_make_result(status=PipelineStatus.HEALTHY)],
        "24h": [
            _make_result(status=PipelineStatus.CRITICAL),
            _make_result(status=PipelineStatus.WARNING),
        ],
    }
    report = rollup_by_label(labeled)
    assert set(report.labels()) == {"1h", "24h"}
    assert report.get("1h").total == 1
    assert report.get("24h").total == 2


def test_rollup_by_label_empty_input_returns_empty_report():
    report = rollup_by_label({})
    assert report.labels() == []


# ---------------------------------------------------------------------------
# format_rollup()
# ---------------------------------------------------------------------------

def test_format_rollup_no_buckets():
    report = RollupReport()
    text = format_rollup(report)
    assert "No rollup data" in text


def test_format_rollup_includes_all_labels():
    labeled = {
        "1h": [_make_result()],
        "6h": [_make_result()],
    }
    report = rollup_by_label(labeled)
    text = format_rollup(report)
    assert "1h" in text
    assert "6h" in text
    assert "Rollup Report" in text


# ---------------------------------------------------------------------------
# load_rollup_config()
# ---------------------------------------------------------------------------

def test_load_rollup_config_missing_file_returns_defaults(tmp_path):
    cfg = load_rollup_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.enabled is True
    assert "1h" in cfg.windows


def test_load_rollup_config_parses_yaml(tmp_path):
    p = tmp_path / "rollup.yaml"
    p.write_text("rollup:\n  enabled: false\n  windows:\n    - '12h'\n")
    cfg = load_rollup_config(str(p))
    assert cfg.enabled is False
    assert cfg.windows == ["12h"]


def test_load_rollup_config_empty_yaml_returns_defaults(tmp_path):
    p = tmp_path / "rollup.yaml"
    p.write_text("")
    cfg = load_rollup_config(str(p))
    assert cfg.enabled is True


def test_rollup_config_dataclass_defaults():
    cfg = RollupConfig()
    assert cfg.enabled is True
    assert isinstance(cfg.windows, list)
    assert len(cfg.windows) > 0
