"""Alert dispatcher: routes alerts to handlers based on pipeline and level."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.alerts import Alert, AlertLevel


Handler = Callable[[Alert], None]


@dataclass
class DispatchRule:
    pipeline: Optional[str]  # None means match all
    min_level: AlertLevel
    handler_name: str


@dataclass
class Dispatcher:
    _handlers: Dict[str, Handler] = field(default_factory=dict)
    _rules: List[DispatchRule] = field(default_factory=list)

    def register(self, name: str, handler: Handler) -> None:
        """Register a named handler callable."""
        self._handlers[name] = handler

    def add_rule(self, rule: DispatchRule) -> None:
        """Add a dispatch rule."""
        self._rules.append(rule)

    def dispatch(self, alert: Alert) -> List[str]:
        """Dispatch alert to all matching handlers. Returns list of handler names invoked."""
        invoked: List[str] = []
        for rule in self._rules:
            if rule.pipeline is not None and rule.pipeline != alert.pipeline:
                continue
            if alert.level.value < rule.min_level.value:
                continue
            handler = self._handlers.get(rule.handler_name)
            if handler is not None:
                handler(alert)
                invoked.append(rule.handler_name)
        return invoked

    def dispatch_all(self, alerts: List[Alert]) -> Dict[str, List[str]]:
        """Dispatch a list of alerts. Returns mapping of pipeline -> handler names."""
        result: Dict[str, List[str]] = {}
        for alert in alerts:
            invoked = self.dispatch(alert)
            result.setdefault(alert.pipeline, []).extend(invoked)
        return result


def make_dispatcher(rules: List[DispatchRule], handlers: Dict[str, Handler]) -> Dispatcher:
    """Construct a Dispatcher from rules and handlers."""
    d = Dispatcher()
    for name, fn in handlers.items():
        d.register(name, fn)
    for rule in rules:
        d.add_rule(rule)
    return d
