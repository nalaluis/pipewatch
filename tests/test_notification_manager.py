"""Tests for notification manager."""

import pytest
from pipewatch.notification_manager import NotificationManager
from pipewatch.notifier import (
    Notifier,
    NotifierConfig,
    NotifierType,
    StdoutNotifier
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


def test_notification_manager_empty_alerts_returns_empty():
    manager = NotificationManager()
    results = manager.notify([])
    assert results == {}


def test_notification_manager_no_notifiers():
    manager = NotificationManager(notifiers=[])
    alerts = [make_alert()]
    results = manager.notify(alerts)
    assert results == {}


def test_notification_manager_single_notifier(capsys):
    config = NotifierConfig(type=NotifierType.STDOUT)
    notifier = StdoutNotifier(config)
    
    manager = NotificationManager(notifiers=[notifier])
    alerts = [make_alert()]
    
    results = manager.notify(alerts)
    
    assert len(results) == 1
    assert "stdout_0" in results
    assert results["stdout_0"] is True


def test_notification_manager_multiple_notifiers(capsys):
    config1 = NotifierConfig(type=NotifierType.STDOUT, min_level=AlertLevel.WARNING)
    config2 = NotifierConfig(type=NotifierType.STDOUT, min_level=AlertLevel.CRITICAL)
    
    notifier1 = StdoutNotifier(config1)
    notifier2 = StdoutNotifier(config2)
    
    manager = NotificationManager(notifiers=[notifier1, notifier2])
    alerts = [make_alert(level=AlertLevel.WARNING)]
    
    results = manager.notify(alerts)
    
    assert len(results) == 2
    assert results["stdout_0"] is True
    assert results["stdout_1"] is True


def test_notification_manager_add_notifier():
    manager = NotificationManager()
    assert len(manager.notifiers) == 0
    
    config = NotifierConfig(type=NotifierType.STDOUT)
    notifier = StdoutNotifier(config)
    manager.add_notifier(notifier)
    
    assert len(manager.notifiers) == 1


def test_notification_manager_from_config():
    config_list = [
        {"type": "stdout", "enabled": True, "min_level": "warning"},
        {"type": "stdout", "enabled": False, "min_level": "critical"}
    ]
    
    manager = NotificationManager.from_config(config_list)
    
    assert len(manager.notifiers) == 2
    assert manager.notifiers[0].config.enabled is True
    assert manager.notifiers[0].config.min_level == AlertLevel.WARNING
    assert manager.notifiers[1].config.enabled is False
    assert manager.notifiers[1].config.min_level == AlertLevel.CRITICAL


def test_notification_manager_from_config_with_webhook():
    config_list = [
        {
            "type": "webhook",
            "enabled": True,
            "min_level": "critical",
            "config": {"url": "https://example.com/hook"}
        }
    ]
    
    manager = NotificationManager.from_config(config_list)
    
    assert len(manager.notifiers) == 1
    assert manager.notifiers[0].config.type == NotifierType.WEBHOOK
    assert manager.notifiers[0].config.config["url"] == "https://example.com/hook"
