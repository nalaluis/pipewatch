"""Tests for pipewatch.filter."""

from __future__ import annotations

import pytest

from pipewatch.filter import (
    FilterCriteria,
    apply_filter,
    filter_critical,
    filter_unhealthy,
)
from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


def make_result(
    name: str,
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
) -> HealthResult:
    return HealthResult(
        pipeline_name=name,
        status=status,
        failure_rate=failure_rate,
        throughput=100.0,
        violations=[],
    )


RESULTS = [
    make_result("payments:daily_load", PipelineStatus.HEALTHY, 0.01),
    make_result("payments:hourly_sync", PipelineStatus.WARNING, 0.12),
    make_result("inventory:nightly", PipelineStatus.CRITICAL, 0.45),
    make_result("audit_log", PipelineStatus.HEALTHY, 0.0),
]


def test_apply_filter_no_criteria_returns_all():
    result = apply_filter(RESULTS, FilterCriteria())
    assert len(result) == len(RESULTS)


def test_filter_by_exact_name_glob():
    result = apply_filter(RESULTS, FilterCriteria(names=["audit_log"]))
    assert len(result) == 1
    assert result[0].pipeline_name == "audit_log"


def test_filter_by_wildcard_name():
    result = apply_filter(RESULTS, FilterCriteria(names=["payments:*"]))
    assert len(result) == 2
    names = {r.pipeline_name for r in result}
    assert "payments:daily_load" in names
    assert "payments:hourly_sync" in names


def test_filter_by_status():
    result = apply_filter(RESULTS, FilterCriteria(statuses=[PipelineStatus.WARNING]))
    assert len(result) == 1
    assert result[0].pipeline_name == "payments:hourly_sync"


def test_filter_by_multiple_statuses():
    result = apply_filter(
        RESULTS,
        FilterCriteria(statuses=[PipelineStatus.WARNING, PipelineStatus.CRITICAL]),
    )
    assert len(result) == 2


def test_filter_by_tag():
    result = apply_filter(RESULTS, FilterCriteria(tags=["payments"]))
    assert len(result) == 2


def test_filter_by_tag_no_match():
    result = apply_filter(RESULTS, FilterCriteria(tags=["unknown"]))
    assert result == []


def test_filter_by_min_failure_rate():
    result = apply_filter(RESULTS, FilterCriteria(min_failure_rate=0.10))
    assert all(r.failure_rate >= 0.10 for r in result)
    assert len(result) == 2


def test_filter_by_max_failure_rate():
    result = apply_filter(RESULTS, FilterCriteria(max_failure_rate=0.05))
    assert all(r.failure_rate <= 0.05 for r in result)


def test_filter_combined_criteria():
    result = apply_filter(
        RESULTS,
        FilterCriteria(tags=["payments"], statuses=[PipelineStatus.WARNING]),
    )
    assert len(result) == 1
    assert result[0].pipeline_name == "payments:hourly_sync"


def test_filter_critical_helper():
    result = filter_critical(RESULTS)
    assert len(result) == 1
    assert result[0].status == PipelineStatus.CRITICAL


def test_filter_unhealthy_helper():
    result = filter_unhealthy(RESULTS)
    assert len(result) == 2
    statuses = {r.status for r in result}
    assert PipelineStatus.HEALTHY not in statuses
