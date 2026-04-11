"""Tests for pipewatch.sla and pipewatch.sla_config."""
from __future__ import annotations

import textwrap
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult, HealthThresholds
from pipewatch import sla as sla_module
from pipewatch.sla import (
    SLAConfig,
    SLAViolation,
    SLAResult,
    check_sla,
    check_all_slas,
    format_sla_report,
)


def _make_metric(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
    throughput: float = 10.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=name,
        status=status,
        failure_rate=failure_rate,
        throughput=throughput,
        total=100,
        failed=int(failure_rate * 100),
    )


def _make_result(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
    throughput: float = 10.0,
) -> HealthResult:
    metric = _make_metric(name, status, failure_rate, throughput)
    return HealthResult(
        pipeline=name,
        metric=metric,
        healthy=status == PipelineStatus.HEALTHY,
        status=status,
        violations=[],
    )


@pytest.fixture(autouse=True)
def clear_downtime():
    sla_module._downtime_registry.clear()
    yield
    sla_module._downtime_registry.clear()


def test_check_sla_compliant_pipeline():
    result = _make_result(failure_rate=0.01, throughput=5.0)
    cfg = SLAConfig(max_failure_rate=0.05, min_throughput=1.0)
    sla_result = check_sla(result, cfg)
    assert sla_result.compliant is True
    assert sla_result.violations == []


def test_check_sla_failure_rate_violation():
    result = _make_result(failure_rate=0.10)
    cfg = SLAConfig(max_failure_rate=0.05)
    sla_result = check_sla(result, cfg)
    assert not sla_result.compliant
    assert any(v.rule == "max_failure_rate" for v in sla_result.violations)


def test_check_sla_throughput_violation():
    result = _make_result(throughput=0.5)
    cfg = SLAConfig(min_throughput=1.0)
    sla_result = check_sla(result, cfg)
    assert not sla_result.compliant
    assert any(v.rule == "min_throughput" for v in sla_result.violations)


def test_check_sla_downtime_violation():
    name = "slow_pipe"
    result = _make_result(name=name, status=PipelineStatus.CRITICAL)
    cfg = SLAConfig(max_downtime_seconds=60.0)

    past = datetime.now(timezone.utc) - timedelta(seconds=120)
    sla_module._downtime_registry[name] = past

    sla_result = check_sla(result, cfg)
    assert not sla_result.compliant
    assert any(v.rule == "max_downtime_seconds" for v in sla_result.violations)


def test_check_sla_downtime_not_violated_within_limit():
    name = "recovering_pipe"
    result = _make_result(name=name, status=PipelineStatus.CRITICAL)
    cfg = SLAConfig(max_downtime_seconds=300.0)

    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    sla_module._downtime_registry[name] = past

    sla_result = check_sla(result, cfg)
    # only downtime rule; no violation yet
    assert all(v.rule != "max_downtime_seconds" for v in sla_result.violations)


def test_healthy_pipeline_clears_downtime_registry():
    name = "healed_pipe"
    sla_module._downtime_registry[name] = datetime.now(timezone.utc)
    result = _make_result(name=name, status=PipelineStatus.HEALTHY)
    check_sla(result, SLAConfig())
    assert name not in sla_module._downtime_registry


def test_check_all_slas_returns_one_per_result():
    results = [
        _make_result("a"),
        _make_result("b", failure_rate=0.9),
    ]
    sla_results = check_all_slas(results)
    assert len(sla_results) == 2
    assert sla_results[0].compliant is True
    assert sla_results[1].compliant is False


def test_check_all_slas_uses_default_config():
    results = [_make_result("x", throughput=0.0)]
    sla_results = check_all_slas(results)
    assert not sla_results[0].compliant


def test_format_sla_report_contains_pipeline_name():
    sr = SLAResult(pipeline="my_pipe", compliant=True)
    report = format_sla_report([sr])
    assert "my_pipe" in report
    assert "OK" in report


def test_format_sla_report_shows_violation_details():
    v = SLAViolation(pipeline="bad_pipe", rule="max_failure_rate", detail="rate=0.9")
    sr = SLAResult(pipeline="bad_pipe", compliant=False, violations=[v])
    report = format_sla_report([sr])
    assert "VIOLATION" in report
    assert "max_failure_rate" in report
    assert "rate=0.9" in report


def test_sla_violation_str_format():
    v = SLAViolation(pipeline="p", rule="min_throughput", detail="low")
    s = str(v)
    assert "SLA violation" in s
    assert "min_throughput" in s
    assert "low" in s
