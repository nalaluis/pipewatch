"""Manages multiple notification backends and dispatches alerts."""

from typing import List, Dict
from pipewatch.alerts import Alert
from pipewatch.notifier import Notifier, NotifierConfig, create_notifier


class NotificationManager:
    """Coordinates sending alerts to multiple notification backends."""

    def __init__(self, notifiers: List[Notifier] = None):
        self.notifiers = notifiers or []

    def add_notifier(self, notifier: Notifier):
        """Add a notifier backend."""
        self.notifiers.append(notifier)

    def notify(self, alerts: List[Alert]) -> Dict[str, bool]:
        """Send alerts to all configured notifiers.
        
        Returns:
            Dictionary mapping notifier type to success status.
        """
        if not alerts:
            return {}
        
        results = {}
        for i, notifier in enumerate(self.notifiers):
            notifier_name = f"{notifier.config.type.value}_{i}"
            try:
                success = notifier.send(alerts)
                results[notifier_name] = success
            except Exception as e:
                print(f"Error sending to {notifier_name}: {e}")
                results[notifier_name] = False
        
        return results

    @classmethod
    def from_config(cls, config_list: List[Dict]) -> 'NotificationManager':
        """Create a NotificationManager from configuration.
        
        Args:
            config_list: List of notifier configuration dictionaries.
        
        Example:
            [
                {"type": "stdout", "enabled": true, "min_level": "warning"},
                {"type": "webhook", "enabled": true, "min_level": "critical",
                 "config": {"url": "https://example.com/webhook"}}
            ]
        """
        from pipewatch.notifier import NotifierType, AlertLevel
        
        notifiers = []
        for cfg in config_list:
            notifier_type = NotifierType(cfg.get("type", "stdout"))
            enabled = cfg.get("enabled", True)
            min_level_str = cfg.get("min_level", "warning")
            min_level = AlertLevel[min_level_str.upper()]
            notifier_config_dict = cfg.get("config", {})
            
            notifier_config = NotifierConfig(
                type=notifier_type,
                enabled=enabled,
                min_level=min_level,
                config=notifier_config_dict
            )
            
            notifier = create_notifier(notifier_config)
            notifiers.append(notifier)
        
        return cls(notifiers=notifiers)
