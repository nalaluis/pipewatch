"""Tests for pipewatch.runbook."""
from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.runbook import (
    RunbookEntry,
    RunbookSuggestion,
    format_runbook_report,
    suggest_runbooks,
)


def make_alert(
    pipeline: str = "etl",
    level: AlertLevel = AlertLevel.CRITICAL,
    reason: str = "failure_rate too high",
) -> Alert:
    return Alert(pipeline=pipeline, level=level, reason=reason)


def test_suggest_runbooks_empty_list():
    assert suggest_runbooks([]) == []


def test_suggest_runbooks_skips_ok_alerts():
    alert = make_alert(level=AlertLevel.OK, reason="failure_rate ok")
    assert suggest_runbooks([alert]) == []


def test_suggest_runbooks_returns_entry_for_failure_rate():
    alert = make_alert(reason="failure_rate exceeded threshold")
    suggestions = suggest_runbooks([alert])
    assert len(suggestions) == 1
    assert suggestions[0].alert is alert
    assert "Failure Rate" in suggestions[0].entry.title


def test_suggest_runbooks_returns_entry_for_throughput():
    alert = make_alert(reason="throughput below minimum")
    suggestions = suggest_runbooks([alert])
    assert len(suggestions) == 1
    assert "Throughput" in suggestions[0].entry.title


def test_suggest_runbooks_no_match_returns_no_suggestion():
    alert = make_alert(reason="unknown metric violation")
    suggestions = suggest_runbooks([alert])
    assert suggestions == []


def test_suggest_runbooks_multiple_alerts():
    alerts = [
        make_alert(pipeline="a", reason="failure_rate high"),
        make_alert(pipeline="b", reason="throughput low"),
        make_alert(pipeline="c", reason="unknown"),
    ]
    suggestions = suggest_runbooks(alerts)
    assert len(suggestions) == 2
    pipelines = [s.alert.pipeline for s in suggestions]
    assert "a" in pipelines
    assert "b" in pipelines


def test_runbook_entry_format_includes_title_and_steps():
    entry = RunbookEntry(title="Test", steps=["step one", "step two"])
    rendered = entry.format()
    assert "Test" in rendered
    assert "step one" in rendered
    assert "step two" in rendered


def test_runbook_entry_format_includes_reference():
    entry = RunbookEntry(title="T", steps=["s"], reference="http://example.com")
    assert "http://example.com" in entry.format()


def test_runbook_suggestion_format():
    alert = make_alert(pipeline="my_pipe", level=AlertLevel.WARNING, reason="throughput low")
    entry = RunbookEntry(title="Low Throughput", steps=["check resources"])
    suggestion = RunbookSuggestion(alert=alert, entry=entry)
    rendered = suggestion.format()
    assert "my_pipe" in rendered
    assert "WARNING" in rendered
    assert "Low Throughput" in rendered


def test_format_runbook_report_no_suggestions():
    assert format_runbook_report([]) == "No runbook suggestions."


def test_format_runbook_report_with_suggestions():
    alert = make_alert(reason="failure_rate critical")
    suggestions = suggest_runbooks([alert])
    report = format_runbook_report(suggestions)
    assert "Runbook Suggestions" in report
    assert "High Failure Rate" in report
