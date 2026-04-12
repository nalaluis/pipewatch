"""Alert router: dispatch alerts to named channels based on routing rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class RouteRule:
    channel: str
    pipeline: Optional[str] = None  # None means match any
    min_level: AlertLevel = AlertLevel.WARNING
    tags: List[str] = field(default_factory=list)


@dataclass
class Router:
    _rules: List[RouteRule] = field(default_factory=list)
    _channels: Dict[str, Callable[[Alert], None]] = field(default_factory=dict)

    def register_channel(self, name: str, handler: Callable[[Alert], None]) -> None:
        """Register a named channel with a handler function."""
        self._channels[name] = handler

    def add_rule(self, rule: RouteRule) -> None:
        """Add a routing rule."""
        self._rules.append(rule)

    def route(self, alert: Alert) -> List[str]:
        """Route an alert to matching channels. Returns list of channel names invoked."""
        invoked: List[str] = []
        for rule in self._rules:
            if not _rule_matches(rule, alert):
                continue
            handler = self._channels.get(rule.channel)
            if handler is not None:
                handler(alert)
                invoked.append(rule.channel)
        return invoked

    def route_all(self, alerts: List[Alert]) -> Dict[str, List[str]]:
        """Route a list of alerts. Returns mapping of alert pipeline -> channels invoked."""
        results: Dict[str, List[str]] = {}
        for alert in alerts:
            channels = self.route(alert)
            results[alert.pipeline] = channels
        return results


def _rule_matches(rule: RouteRule, alert: Alert) -> bool:
    if alert.level.value < rule.min_level.value:
        return False
    if rule.pipeline is not None and rule.pipeline != alert.pipeline:
        return False
    if rule.tags:
        alert_tags = set(getattr(alert, "tags", []) or [])
        if not any(t in alert_tags for t in rule.tags):
            return False
    return True


def make_router(rules: List[RouteRule]) -> Router:
    """Construct a Router pre-loaded with the given rules."""
    r = Router()
    for rule in rules:
        r.add_rule(rule)
    return r
