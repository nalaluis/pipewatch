"""Tests for pipewatch.remediation."""

from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.remediation import (
    RemediationAction,
    RemediationReport,
    build_remediation,
    format_remediation,
)


def make_alert(
    pipeline: str = "etl",
    level: AlertLevel = AlertLevel.WARNING,
    violation: str = "failure_rate",
    message: str = "failure rate too high",
) -> Alert:
    return Alert(pipeline=pipeline, level=level, violation=violation, message=message)


def test_build_remediation_no_alerts_returns_empty_report():
    report = build_remediation("etl", [])
    assert report.pipeline == "etl"
    assert not report.has_actions


def test_build_remediation_failure_rate_warning_produces_actions():
    alert = make_alert(violation="failure_rate", level=AlertLevel.WARNING)
    report = build_remediation("etl", [alert])
    assert report.has_actions
    titles = [a.title for a in report.actions]
    assert "Inspect recent failures" in titles


def test_build_remediation_failure_rate_critical_includes_circuit_breaker():
    alert = make_alert(violation="failure_rate", level=AlertLevel.CRITICAL)
    report = build_remediation("etl", [alert])
    titles = [a.title for a in report.actions]
    assert "Consider circuit breaker" in titles


def test_build_remediation_throughput_violation_produces_actions():
    alert = make_alert(violation="throughput", level=AlertLevel.WARNING)
    report = build_remediation("etl", [alert])
    titles = [a.title for a in report.actions]
    assert "Check upstream data sources" in titles
    assert "Scale worker capacity" in titles


def test_build_remediation_skips_alerts_for_other_pipelines():
    alert = make_alert(pipeline="other", violation="failure_rate")
    report = build_remediation("etl", [alert])
    assert not report.has_actions


def test_build_remediation_deduplicates_actions():
    a1 = make_alert(violation="failure_rate", level=AlertLevel.WARNING)
    a2 = make_alert(violation="failure_rate", level=AlertLevel.WARNING)
    report = build_remediation("etl", [a1, a2])
    titles = [a.title for a in report.actions]
    assert titles.count("Inspect recent failures") == 1


def test_sorted_actions_orders_by_priority():
    report = RemediationReport(
        pipeline="etl",
        actions=[
            RemediationAction(title="B", description="", priority=3),
            RemediationAction(title="A", description="", priority=1),
            RemediationAction(title="C", description="", priority=2),
        ],
    )
    sorted_titles = [a.title for a in report.sorted_actions()]
    assert sorted_titles == ["A", "C", "B"]


def test_format_remediation_no_actions():
    report = RemediationReport(pipeline="etl")
    text = format_remediation(report)
    assert "No remediation actions suggested" in text
    assert "etl" in text


def test_format_remediation_includes_action_titles():
    alert = make_alert(violation="failure_rate", level=AlertLevel.CRITICAL)
    report = build_remediation("etl", [alert])
    text = format_remediation(report)
    assert "Inspect recent failures" in text
    assert "Consider circuit breaker" in text


def test_remediation_action_str_with_command():
    action = RemediationAction(
        title="Check logs",
        description="Review error logs.",
        command="pipewatch logs --pipeline etl",
        priority=1,
    )
    text = str(action)
    assert "Check logs" in text
    assert "pipewatch logs" in text


def test_remediation_action_str_without_command():
    action = RemediationAction(title="Scale workers", description="Add more workers.", priority=2)
    text = str(action)
    assert "Scale workers" in text
    assert "$" not in text
