"""Notification backends for sending alerts to external services."""

import os
import json
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

from pipewatch.alerts import Alert, AlertLevel


class NotifierType(Enum):
    """Supported notification backends."""
    SLACK = "slack"
    EMAIL = "email"
    WEBHOOK = "webhook"
    STDOUT = "stdout"


@dataclass
class NotifierConfig:
    """Configuration for a notifier."""
    type: NotifierType
    enabled: bool = True
    min_level: AlertLevel = AlertLevel.WARNING
    config: Dict = None

    def __post_init__(self):
        if self.config is None:
            self.config = {}


class Notifier:
    """Base class for notification backends."""

    def __init__(self, config: NotifierConfig):
        self.config = config

    def should_notify(self, alert: Alert) -> bool:
        """Check if this alert should trigger a notification."""
        if not self.config.enabled:
            return False
        
        level_priority = {
            AlertLevel.INFO: 0,
            AlertLevel.WARNING: 1,
            AlertLevel.CRITICAL: 2
        }
        return level_priority[alert.level] >= level_priority[self.config.min_level]

    def send(self, alerts: List[Alert]) -> bool:
        """Send alerts. Returns True on success."""
        raise NotImplementedError


class StdoutNotifier(Notifier):
    """Simple stdout notifier for testing."""

    def send(self, alerts: List[Alert]) -> bool:
        filtered = [a for a in alerts if self.should_notify(a)]
        if not filtered:
            return True
        
        print("\n=== ALERTS ===")
        for alert in filtered:
            print(f"[{alert.level.value.upper()}] {alert.pipeline}: {alert.message}")
        print("=============\n")
        return True


class WebhookNotifier(Notifier):
    """Send alerts to a webhook endpoint."""

    def send(self, alerts: List[Alert]) -> bool:
        filtered = [a for a in alerts if self.should_notify(a)]
        if not filtered:
            return True
        
        url = self.config.config.get("url")
        if not url:
            return False
        
        payload = {
            "alerts": [
                {
                    "pipeline": a.pipeline,
                    "level": a.level.value,
                    "message": a.message,
                    "metric": a.metric,
                    "value": a.value,
                    "threshold": a.threshold
                }
                for a in filtered
            ]
        }
        
        # In real implementation, use requests library
        # For now, just print the payload
        print(f"Would POST to {url}: {json.dumps(payload, indent=2)}")
        return True


def create_notifier(config: NotifierConfig) -> Notifier:
    """Factory function to create notifiers."""
    if config.type == NotifierType.STDOUT:
        return StdoutNotifier(config)
    elif config.type == NotifierType.WEBHOOK:
        return WebhookNotifier(config)
    else:
        raise ValueError(f"Unsupported notifier type: {config.type}")
