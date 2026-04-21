"""Tests for pipewatch.capacity and pipewatch.capacity_config."""
from __future__ import annotations

import textwrap
import os
import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.capacity import (
    CapacityConfig,
    CapacityResult,
    check_capacity,
    check_all_capacity,
)
from pipewatch.capacity_config import load_capacity_config


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def make_metric(pipeline: str = "etl", throughput: float = 500.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=PipelineStatus.HEALTHY,
        failure_rate=0.0,
        throughput=throughput,
        processed=int(throughput * 60),
        failed=0,
    )


_CFG = CapacityConfig(max_throughput=1000.0, warn_pct=0.80, critical_pct=0.95, min_throughput=10.0)


# ---------------------------------------------------------------------------
# check_capacity
# ---------------------------------------------------------------------------

def test_check_capacity_ok_status():
    result = check_capacity(make_metric(throughput=500.0), _CFG)
    assert result is not None
    assert result.status == "ok"


def test_check_capacity_warning_at_80_pct():
    result = check_capacity(make_metric(throughput=800.0), _CFG)
    assert result.status == "warning"


def test_check_capacity_critical_at_95_pct():
    result = check_capacity(make_metric(throughput=950.0), _CFG)
    assert result.status == "critical"


def test_check_capacity_under_min_throughput():
    result = check_capacity(make_metric(throughput=5.0), _CFG)
    assert result.status == "under"


def test_check_capacity_disabled_returns_none():
    cfg = CapacityConfig(enabled=False)
    assert check_capacity(make_metric(), cfg) is None


def test_check_capacity_utilization_value():
    result = check_capacity(make_metric(throughput=600.0), _CFG)
    assert abs(result.utilization - 0.6) < 1e-9


def test_capacity_result_str_contains_pipeline():
    result = check_capacity(make_metric(pipeline="my-pipe", throughput=500.0), _CFG)
    assert "my-pipe" in str(result)


# ---------------------------------------------------------------------------
# check_all_capacity
# ---------------------------------------------------------------------------

def test_check_all_capacity_returns_list():
    metrics = [make_metric("a", 100), make_metric("b", 900)]
    results = check_all_capacity(metrics, _CFG)
    assert len(results) == 2


def test_check_all_capacity_disabled_returns_empty():
    cfg = CapacityConfig(enabled=False)
    results = check_all_capacity([make_metric()], cfg)
    assert results == []


def test_check_all_capacity_statuses_vary():
    metrics = [make_metric("low", 5.0), make_metric("high", 960.0), make_metric("mid", 400.0)]
    results = check_all_capacity(metrics, _CFG)
    statuses = {r.pipeline: r.status for r in results}
    assert statuses["low"] == "under"
    assert statuses["high"] == "critical"
    assert statuses["mid"] == "ok"


# ---------------------------------------------------------------------------
# load_capacity_config
# ---------------------------------------------------------------------------

def test_load_capacity_config_missing_file_returns_defaults(tmp_path):
    cfg = load_capacity_config(str(tmp_path / "no-file.yaml"))
    assert isinstance(cfg, CapacityConfig)
    assert cfg.max_throughput == 1000.0
    assert cfg.enabled is True


def test_load_capacity_config_parses_yaml(tmp_path):
    p = tmp_path / "cap.yaml"
    p.write_text(textwrap.dedent("""\
        capacity:
          max_throughput: 500.0
          warn_pct: 0.70
          critical_pct: 0.90
          min_throughput: 5.0
          enabled: true
    """))
    cfg = load_capacity_config(str(p))
    assert cfg.max_throughput == 500.0
    assert cfg.warn_pct == pytest.approx(0.70)
    assert cfg.critical_pct == pytest.approx(0.90)
    assert cfg.min_throughput == 5.0


def test_load_capacity_config_empty_yaml_returns_defaults(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("")
    cfg = load_capacity_config(str(p))
    assert cfg.max_throughput == 1000.0
