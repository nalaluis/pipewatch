"""Tests for pipewatch.mirror."""

from __future__ import annotations

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.mirror import (
    MirrorEntry,
    MirrorReport,
    build_mirror,
    format_mirror,
)


def make_metric(
    name: str,
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=name,
        status=status,
        failure_rate=failure_rate,
        throughput=throughput,
    )


def test_build_mirror_all_matching():
    left = [make_metric("pipe_a"), make_metric("pipe_b")]
    right = [make_metric("pipe_a"), make_metric("pipe_b")]
    report = build_mirror("prod", "staging", left, right)
    assert len(report.entries) == 2
    assert report.diverged == []
    assert report.missing_left == []
    assert report.missing_right == []


def test_build_mirror_detects_status_divergence():
    left = [make_metric("pipe_a", PipelineStatus.HEALTHY)]
    right = [make_metric("pipe_a", PipelineStatus.CRITICAL)]
    report = build_mirror("prod", "staging", left, right)
    assert len(report.diverged) == 1
    assert report.diverged[0].pipeline == "pipe_a"


def test_build_mirror_detects_missing_in_right():
    left = [make_metric("pipe_a"), make_metric("pipe_b")]
    right = [make_metric("pipe_a")]
    report = build_mirror("prod", "staging", left, right)
    assert len(report.missing_right) == 1
    assert report.missing_right[0].pipeline == "pipe_b"


def test_build_mirror_detects_missing_in_left():
    left = [make_metric("pipe_a")]
    right = [make_metric("pipe_a"), make_metric("pipe_c")]
    report = build_mirror("prod", "staging", left, right)
    assert len(report.missing_left) == 1
    assert report.missing_left[0].pipeline == "pipe_c"


def test_mirror_entry_only_in_left():
    entry = MirrorEntry(pipeline="p", left=make_metric("p"), right=None)
    assert entry.only_in_left is True
    assert entry.only_in_right is False


def test_mirror_entry_only_in_right():
    entry = MirrorEntry(pipeline="p", left=None, right=make_metric("p"))
    assert entry.only_in_right is True
    assert entry.only_in_left is False


def test_mirror_entry_failure_rate_delta():
    left = make_metric("p", failure_rate=0.1)
    right = make_metric("p", failure_rate=0.3)
    entry = MirrorEntry(pipeline="p", left=left, right=right)
    assert abs(entry.failure_rate_delta - 0.2) < 1e-9


def test_mirror_entry_failure_rate_delta_none_when_missing():
    entry = MirrorEntry(pipeline="p", left=make_metric("p"), right=None)
    assert entry.failure_rate_delta is None


def test_format_mirror_contains_env_names():
    left = [make_metric("pipe_a", PipelineStatus.HEALTHY)]
    right = [make_metric("pipe_a", PipelineStatus.CRITICAL)]
    report = build_mirror("prod", "staging", left, right)
    text = format_mirror(report)
    assert "prod" in text
    assert "staging" in text


def test_format_mirror_shows_diverged_pipeline():
    left = [make_metric("pipe_a", PipelineStatus.HEALTHY)]
    right = [make_metric("pipe_a", PipelineStatus.CRITICAL)]
    report = build_mirror("prod", "staging", left, right)
    text = format_mirror(report)
    assert "DIVERGED" in text
    assert "pipe_a" in text


def test_format_mirror_shows_left_only():
    left = [make_metric("only_left")]
    right: list = []
    report = build_mirror("prod", "staging", left, right)
    text = format_mirror(report)
    assert "LEFT ONLY" in text
    assert "only_left" in text


def test_format_mirror_shows_right_only():
    left: list = []
    right = [make_metric("only_right")]
    report = build_mirror("prod", "staging", left, right)
    text = format_mirror(report)
    assert "RIGHT ONLY" in text
    assert "only_right" in text
