"""Tests for pipewatch.tracer."""
from __future__ import annotations

import time
import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch import tracer as tr
from pipewatch.tracer import (
    TraceSpan,
    TraceReport,
    start_trace,
    end_trace,
    get_report,
    reset,
)


def make_metric(
    pipeline: str = "etl",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        failure_rate=failure_rate,
        throughput=throughput,
    )


@pytest.fixture(autouse=True)
def clean_state():
    reset()
    yield
    reset()


def test_end_trace_without_start_returns_none():
    metric = make_metric()
    assert end_trace(metric) is None


def test_end_trace_after_start_returns_span():
    start_trace("etl")
    time.sleep(0.001)
    span = end_trace(make_metric(pipeline="etl"))
    assert isinstance(span, TraceSpan)
    assert span.pipeline == "etl"


def test_span_duration_is_positive():
    start_trace("etl")
    time.sleep(0.005)
    span = end_trace(make_metric())
    assert span is not None
    assert span.duration_ms > 0


def test_span_status_matches_metric():
    start_trace("etl")
    span = end_trace(make_metric(status=PipelineStatus.CRITICAL))
    assert span is not None
    assert span.status == PipelineStatus.CRITICAL


def test_span_str_contains_pipeline_name():
    start_trace("etl")
    span = end_trace(make_metric())
    assert span is not None
    assert "etl" in str(span)


def test_get_report_accumulates_spans():
    for name in ("a", "b", "c"):
        start_trace(name)
        end_trace(make_metric(pipeline=name))
    report = get_report()
    assert len(report.spans) == 3


def test_report_for_pipeline_filters_correctly():
    start_trace("x")
    end_trace(make_metric(pipeline="x"))
    start_trace("y")
    end_trace(make_metric(pipeline="y"))
    report = get_report()
    assert len(report.for_pipeline("x")) == 1
    assert report.for_pipeline("x")[0].pipeline == "x"


def test_report_slowest_returns_n_spans():
    for name in ("p1", "p2", "p3"):
        start_trace(name)
        end_trace(make_metric(pipeline=name))
    report = get_report()
    slowest = report.slowest(2)
    assert len(slowest) == 2


def test_report_pipeline_names_unique():
    for _ in range(3):
        start_trace("dup")
        end_trace(make_metric(pipeline="dup"))
    report = get_report()
    assert report.pipeline_names().count("dup") == 1


def test_reset_clears_report():
    start_trace("etl")
    end_trace(make_metric())
    reset()
    assert get_report().spans == []


def test_active_trace_cleared_after_end():
    start_trace("etl")
    end_trace(make_metric())
    # Second call without start should return None
    assert end_trace(make_metric()) is None
