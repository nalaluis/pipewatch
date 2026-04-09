"""Tests for notification backends."""

import pytest
from pipewatch.notifier import (
    Notifier,
    NotifierConfig,
    NotifierType,
    StdoutNotifier,
    WebhookNotifier,
    create_notifier
)
from pipewatch.alerts import Alert, AlertLevel


def make_alert(level=AlertLevel.WARNING, pipeline="test_pipeline"):
    """Helper to create test alerts."""
    return Alert(
        pipeline=pipeline,
        level=level,
        message="Test alert",
        metric="failure_rate",
        value=0.5,
        threshold=0.3
    )


def test_notifier_should_notify_respects_min_level():
    config = NotifierConfig(
        type=NotifierType.STDOUT,
        min_level=AlertLevel.CRITICAL
    )
    notifier = StdoutNotifier(config)
    
    warning_alert = make_alert(level=AlertLevel.WARNING)
    critical_alert = make_alert(level=AlertLevel.CRITICAL)
    
    assert not notifier.should_notify(warning_alert)
    assert notifier.should_notify(critical_alert)


def test_notifier_disabled_should_not_notify():
    config = NotifierConfig(
        type=NotifierType.STDOUT,
        enabled=False
    )
    notifier = StdoutNotifier(config)
    
    alert = make_alert(level=AlertLevel.CRITICAL)
    assert not notifier.should_notify(alert)


def test_stdout_notifier_sends_alerts(capsys):
    config = NotifierConfig(
        type=NotifierType.STDOUT,
        min_level=AlertLevel.WARNING
    )
    notifier = StdoutNotifier(config)
    
    alerts = [
        make_alert(level=AlertLevel.WARNING, pipeline="pipeline1"),
        make_alert(level=AlertLevel.CRITICAL, pipeline="pipeline2")
    ]
    
    result = notifier.send(alerts)
    assert result is True
    
    captured = capsys.readouterr()
    assert "ALERTS" in captured.out
    assert "pipeline1" in captured.out
    assert "pipeline2" in captured.out


def test_webhook_notifier_requires_url(capsys):
    config = NotifierConfig(
        type=NotifierType.WEBHOOK,
        config={}
    )
    notifier = WebhookNotifier(config)
    
    alerts = [make_alert()]
    result = notifier.send(alerts)
    
    assert result is False


def test_webhook_notifier_with_url(capsys):
    config = NotifierConfig(
        type=NotifierType.WEBHOOK,
        config={"url": "https://example.com/webhook"}
    )
    notifier = WebhookNotifier(config)
    
    alerts = [make_alert()]
    result = notifier.send(alerts)
    
    assert result is True
    captured = capsys.readouterr()
    assert "https://example.com/webhook" in captured.out


def test_create_notifier_factory():
    stdout_config = NotifierConfig(type=NotifierType.STDOUT)
    stdout_notifier = create_notifier(stdout_config)
    assert isinstance(stdout_notifier, StdoutNotifier)
    
    webhook_config = NotifierConfig(type=NotifierType.WEBHOOK)
    webhook_notifier = create_notifier(webhook_config)
    assert isinstance(webhook_notifier, WebhookNotifier)
