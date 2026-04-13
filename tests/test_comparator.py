"""Tests for pipewatch.comparator."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult
from pipewatch.reporter import Report
from pipewatch.comparator import (
    PipelineComparison,
    ComparisonReport,
    compare_reports,
    format_comparison,
)


def _make_metric(pipeline: str, failed: int, total: int, throughput: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        failed=failed,
        total=total,
        throughput=throughput,
    )


def _make_result(pipeline: str, failed: int, total: int, throughput: float, status: PipelineStatus):
    metric = _make_metric(pipeline, failed, total, throughput)
    return HealthResult(metric=metric, status=status, violations=[])


def _make_report(*results) -> Report:
    return Report(results=list(results), alerts=[], timestamp="2024-01-01T00:00:00Z")


# ---------------------------------------------------------------------------
# PipelineComparison properties
# ---------------------------------------------------------------------------

def test_is_regression_when_status_worsens():
    c = PipelineComparison(
        pipeline="p",
        prev_status=PipelineStatus.HEALTHY,
        curr_status=PipelineStatus.CRITICAL,
        prev_failure_rate=0.01,
        curr_failure_rate=0.5,
        prev_throughput=100.0,
        curr_throughput=50.0,
    )
    assert c.is_regression is True
    assert c.is_improvement is False


def test_is_improvement_when_status_improves():
    c = PipelineComparison(
        pipeline="p",
        prev_status=PipelineStatus.CRITICAL,
        curr_status=PipelineStatus.HEALTHY,
        prev_failure_rate=0.5,
        curr_failure_rate=0.01,
        prev_throughput=50.0,
        curr_throughput=100.0,
    )
    assert c.is_improvement is True
    assert c.is_regression is False


def test_neither_regression_nor_improvement_when_same_status():
    c = PipelineComparison(
        pipeline="p",
        prev_status=PipelineStatus.WARNING,
        curr_status=PipelineStatus.WARNING,
        prev_failure_rate=0.1,
        curr_failure_rate=0.12,
        prev_throughput=80.0,
        curr_throughput=78.0,
    )
    assert c.is_regression is False
    assert c.is_improvement is False


def test_failure_rate_delta():
    c = PipelineComparison(
        pipeline="p",
        prev_status=PipelineStatus.HEALTHY,
        curr_status=PipelineStatus.WARNING,
        prev_failure_rate=0.05,
        curr_failure_rate=0.15,
        prev_throughput=100.0,
        curr_throughput=100.0,
    )
    assert abs(c.failure_rate_delta - 0.10) < 1e-9


def test_throughput_delta():
    c = PipelineComparison(
        pipeline="p",
        prev_status=PipelineStatus.HEALTHY,
        curr_status=PipelineStatus.HEALTHY,
        prev_failure_rate=0.01,
        curr_failure_rate=0.01,
        prev_throughput=200.0,
        curr_throughput=150.0,
    )
    assert abs(c.throughput_delta - (-50.0)) < 1e-9


# ---------------------------------------------------------------------------
# compare_reports
# ---------------------------------------------------------------------------

def test_compare_reports_matches_shared_pipelines():
    prev = _make_report(
        _make_result("alpha", 1, 100, 90.0, PipelineStatus.HEALTHY),
        _make_result("beta", 10, 100, 70.0, PipelineStatus.WARNING),
    )
    curr = _make_report(
        _make_result("alpha", 5, 100, 80.0, PipelineStatus.WARNING),
        _make_result("beta", 2, 100, 90.0, PipelineStatus.HEALTHY),
    )
    report = compare_reports(prev, curr)
    assert len(report.comparisons) == 2
    names = {c.pipeline for c in report.comparisons}
    assert names == {"alpha", "beta"}


def test_compare_reports_skips_new_pipelines():
    prev = _make_report(_make_result("alpha", 1, 100, 90.0, PipelineStatus.HEALTHY))
    curr = _make_report(
        _make_result("alpha", 1, 100, 90.0, PipelineStatus.HEALTHY),
        _make_result("gamma", 0, 50, 50.0, PipelineStatus.HEALTHY),
    )
    report = compare_reports(prev, curr)
    assert len(report.comparisons) == 1
    assert report.comparisons[0].pipeline == "alpha"


def test_compare_reports_regressions_list():
    prev = _make_report(_make_result("p", 1, 100, 100.0, PipelineStatus.HEALTHY))
    curr = _make_report(_make_result("p", 20, 100, 80.0, PipelineStatus.CRITICAL))
    report = compare_reports(prev, curr)
    assert len(report.regressions) == 1
    assert len(report.improvements) == 0


def test_compare_reports_improvements_list():
    prev = _make_report(_make_result("p", 20, 100, 80.0, PipelineStatus.CRITICAL))
    curr = _make_report(_make_result("p", 1, 100, 100.0, PipelineStatus.HEALTHY))
    report = compare_reports(prev, curr)
    assert len(report.improvements) == 1
    assert len(report.regressions) == 0


def test_compare_reports_empty_when_no_overlap():
    prev = _make_report(_make_result("alpha", 1, 100, 90.0, PipelineStatus.HEALTHY))
    curr = _make_report(_make_result("beta", 1, 100, 90.0, PipelineStatus.HEALTHY))
    report = compare_reports(prev, curr)
    assert report.comparisons == []


# ---------------------------------------------------------------------------
# format_comparison
# ---------------------------------------------------------------------------

def test_format_comparison_no_comparisons():
    report = ComparisonReport(comparisons=[])
    text = format_comparison(report)
    assert "No comparable" in text


def test_format_comparison_includes_pipeline_name():
    prev = _make_report(_make_result("my-pipe", 5, 100, 90.0, PipelineStatus.WARNING))
    curr = _make_report(_make_result("my-pipe", 1, 100, 95.0, PipelineStatus.HEALTHY))
    report = compare_reports(prev, curr)
    text = format_comparison(report)
    assert "my-pipe" in text
    assert "Regressions" in text
    assert "Improvements" in text
