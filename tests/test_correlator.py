"""Tests for pipewatch.correlator."""

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.correlator import (
    CorrelationGroup,
    correlate_by_level,
    correlate_by_metric,
    format_correlation,
)


def make_alert(pipeline: str, level: AlertLevel, message: str = "failure_rate too high") -> Alert:
    return Alert(pipeline=pipeline, level=level, message=message)


def test_correlate_by_level_empty():
    result = correlate_by_level([])
    assert result == {}


def test_correlate_by_level_groups_correctly():
    alerts = [
        make_alert("pipe-a", AlertLevel.CRITICAL),
        make_alert("pipe-b", AlertLevel.WARNING),
        make_alert("pipe-c", AlertLevel.CRITICAL),
    ]
    groups = correlate_by_level(alerts)
    assert "critical" in groups
    assert "warning" in groups
    assert groups["critical"].size == 2
    assert groups["warning"].size == 1


def test_correlate_by_level_pipeline_names():
    alerts = [
        make_alert("pipe-x", AlertLevel.CRITICAL),
        make_alert("pipe-y", AlertLevel.CRITICAL),
    ]
    groups = correlate_by_level(alerts)
    assert set(groups["critical"].pipeline_names) == {"pipe-x", "pipe-y"}


def test_correlation_group_max_level_critical():
    group = CorrelationGroup(key="test", alerts=[
        make_alert("a", AlertLevel.WARNING),
        make_alert("b", AlertLevel.CRITICAL),
    ])
    assert group.max_level == AlertLevel.CRITICAL


def test_correlation_group_max_level_none_when_empty():
    group = CorrelationGroup(key="empty")
    assert group.max_level is None


def test_correlate_by_metric_throughput():
    alerts = [
        make_alert("pipe-a", AlertLevel.WARNING, message="throughput below threshold"),
        make_alert("pipe-b", AlertLevel.CRITICAL, message="failure_rate too high"),
        make_alert("pipe-c", AlertLevel.WARNING, message="low throughput detected"),
    ]
    groups = correlate_by_metric(alerts)
    assert "throughput" in groups
    assert "failure_rate" in groups
    assert groups["throughput"].size == 2
    assert groups["failure_rate"].size == 1


def test_format_correlation_empty():
    result = format_correlation({})
    assert result == "No correlated alerts."


def test_format_correlation_includes_key_and_pipelines():
    alerts = [
        make_alert("pipe-a", AlertLevel.CRITICAL),
        make_alert("pipe-b", AlertLevel.CRITICAL),
    ]
    groups = correlate_by_level(alerts)
    output = format_correlation(groups)
    assert "CRITICAL" in output
    assert "pipe-a" in output
    assert "pipe-b" in output
