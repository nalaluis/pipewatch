"""Tests for pipewatch.digest."""
from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus
from pipewatch.digest import build_digest, format_digest, Digest


def make_result(
    pipeline: str = "pipe-a",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.01,
    throughput: float = 100.0,
) -> HealthResult:
    return HealthResult(
        pipeline=pipeline,
        status=status,
        failure_rate=failure_rate,
        throughput=throughput,
        violations=[],
    )


def make_alert(
    pipeline: str = "pipe-a",
    level: AlertLevel = AlertLevel.CRITICAL,
    message: str = "failure rate too high",
) -> Alert:
    return Alert(pipeline=pipeline, level=level, message=message)


# ---------------------------------------------------------------------------


def test_build_digest_returns_digest_instance():
    results = [make_result()]
    digest = build_digest(results, [])
    assert isinstance(digest, Digest)


def test_build_digest_summary_counts():
    results = [
        make_result("p1", PipelineStatus.HEALTHY),
        make_result("p2", PipelineStatus.WARNING),
        make_result("p3", PipelineStatus.CRITICAL),
    ]
    digest = build_digest(results, [])
    assert digest.summary.total == 3
    assert digest.summary.healthy_count == 1
    assert digest.summary.warning_count == 1
    assert digest.summary.critical_count == 1


def test_build_digest_entries_match_results():
    results = [make_result("pipe-x"), make_result("pipe-y")]
    digest = build_digest(results, [])
    names = [e.pipeline for e in digest.entries]
    assert "pipe-x" in names
    assert "pipe-y" in names


def test_build_digest_alert_count_per_pipeline():
    results = [make_result("pipe-a")]
    alerts = [make_alert("pipe-a"), make_alert("pipe-a")]
    digest = build_digest(results, alerts)
    entry = digest.entries[0]
    assert entry.alert_count == 2


def test_build_digest_top_alerts_only_critical():
    results = [make_result("p")]
    alerts = [
        make_alert("p", AlertLevel.CRITICAL, "crit msg"),
        make_alert("p", AlertLevel.WARNING, "warn msg"),
    ]
    digest = build_digest(results, alerts)
    assert len(digest.top_alerts) == 1
    assert "crit msg" in digest.top_alerts[0]


def test_build_digest_top_alerts_capped_at_five():
    results = [make_result("p")]
    alerts = [make_alert("p", AlertLevel.CRITICAL, f"msg {i}") for i in range(10)]
    digest = build_digest(results, alerts)
    assert len(digest.top_alerts) == 5


def test_build_digest_window_seconds_stored():
    digest = build_digest([make_result()], [], window_seconds=600)
    assert digest.window_seconds == 600


def test_format_digest_contains_pipeline_name():
    results = [make_result("my-pipeline")]
    digest = build_digest(results, [])
    text = format_digest(digest)
    assert "my-pipeline" in text


def test_format_digest_contains_summary_line():
    results = [make_result()]
    digest = build_digest(results, [])
    text = format_digest(digest)
    assert "Pipelines:" in text


def test_format_digest_shows_top_alerts_section():
    results = [make_result("p")]
    alerts = [make_alert("p", AlertLevel.CRITICAL, "something broke")]
    digest = build_digest(results, alerts)
    text = format_digest(digest)
    assert "Top critical alerts" in text
    assert "something broke" in text
