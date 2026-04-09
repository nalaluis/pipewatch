"""Tests for pipewatch.pipeline_grouper."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import pytest

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.pipeline_grouper import (
    PipelineGroup,
    format_groups,
    group_by,
    group_by_status,
    group_by_tag,
)


def make_result(name: str, status: PipelineStatus, tags: dict | None = None) -> HealthResult:
    metric = PipelineMetric(
        pipeline=name,
        status=status,
        failure_rate=0.0,
        throughput=100.0,
        tags=tags or {},
    )
    return HealthResult(metric=metric, healthy=status == PipelineStatus.OK, violations=[])


def test_group_by_status_separates_correctly():
    results = [
        make_result("a", PipelineStatus.OK),
        make_result("b", PipelineStatus.WARNING),
        make_result("c", PipelineStatus.CRITICAL),
        make_result("d", PipelineStatus.OK),
    ]
    groups = group_by_status(results)
    assert groups["ok"].size == 2
    assert groups["warning"].size == 1
    assert groups["critical"].size == 1


def test_group_by_status_counts():
    results = [
        make_result("x", PipelineStatus.CRITICAL),
        make_result("y", PipelineStatus.CRITICAL),
    ]
    groups = group_by_status(results)
    assert groups["critical"].critical_count == 2
    assert groups["critical"].healthy_count == 0


def test_group_by_tag_known_key():
    results = [
        make_result("p1", PipelineStatus.OK, tags={"team": "alpha"}),
        make_result("p2", PipelineStatus.WARNING, tags={"team": "beta"}),
        make_result("p3", PipelineStatus.OK, tags={"team": "alpha"}),
    ]
    groups = group_by_tag(results, "team")
    assert groups["alpha"].size == 2
    assert groups["beta"].size == 1


def test_group_by_tag_missing_tag_goes_to_untagged():
    results = [
        make_result("p1", PipelineStatus.OK, tags={}),
        make_result("p2", PipelineStatus.OK, tags={"team": "alpha"}),
    ]
    groups = group_by_tag(results, "team")
    assert "__untagged__" in groups
    assert groups["__untagged__"].size == 1


def test_group_by_custom_key_fn():
    results = [
        make_result("ingest.orders", PipelineStatus.OK),
        make_result("ingest.users", PipelineStatus.WARNING),
        make_result("export.reports", PipelineStatus.OK),
    ]
    groups = group_by(results, key_fn=lambda r: r.metric.pipeline.split(".")[0])
    assert "ingest" in groups
    assert "export" in groups
    assert groups["ingest"].size == 2


def test_format_groups_output():
    results = [
        make_result("a", PipelineStatus.OK),
        make_result("b", PipelineStatus.CRITICAL),
    ]
    groups = group_by_status(results)
    output = format_groups(groups)
    assert "ok" in output
    assert "critical" in output
    assert "total=" in output


def test_format_groups_empty():
    assert format_groups({}) == "(no groups)"
