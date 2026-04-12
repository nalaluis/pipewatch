"""Tests for pipewatch.router."""

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.router import RouteRule, Router, make_router, _rule_matches


def make_alert(
    pipeline: str = "pipe_a",
    level: AlertLevel = AlertLevel.WARNING,
    message: str = "test alert",
) -> Alert:
    return Alert(pipeline=pipeline, level=level, message=message)


def test_router_no_rules_returns_empty():
    router = Router()
    received = []
    router.register_channel("slack", received.append)
    result = router.route(make_alert())
    assert result == []
    assert received == []


def test_router_matching_rule_invokes_handler():
    router = make_router([RouteRule(channel="slack", min_level=AlertLevel.WARNING)])
    received = []
    router.register_channel("slack", received.append)
    alert = make_alert(level=AlertLevel.WARNING)
    result = router.route(alert)
    assert "slack" in result
    assert len(received) == 1


def test_router_below_min_level_skipped():
    router = make_router([RouteRule(channel="pager", min_level=AlertLevel.CRITICAL)])
    received = []
    router.register_channel("pager", received.append)
    alert = make_alert(level=AlertLevel.WARNING)
    result = router.route(alert)
    assert result == []
    assert received == []


def test_router_pipeline_filter_matches():
    router = make_router([RouteRule(channel="email", pipeline="pipe_a")])
    received = []
    router.register_channel("email", received.append)
    result = router.route(make_alert(pipeline="pipe_a"))
    assert "email" in result


def test_router_pipeline_filter_excludes_other():
    router = make_router([RouteRule(channel="email", pipeline="pipe_b")])
    received = []
    router.register_channel("email", received.append)
    result = router.route(make_alert(pipeline="pipe_a"))
    assert result == []


def test_router_unregistered_channel_not_invoked():
    router = make_router([RouteRule(channel="missing")])
    result = router.route(make_alert())
    assert result == []


def test_route_all_returns_mapping():
    router = make_router([RouteRule(channel="slack", min_level=AlertLevel.WARNING)])
    router.register_channel("slack", lambda a: None)
    alerts = [make_alert("pipe_a"), make_alert("pipe_b", level=AlertLevel.CRITICAL)]
    mapping = router.route_all(alerts)
    assert "pipe_a" in mapping
    assert "pipe_b" in mapping
    assert mapping["pipe_a"] == ["slack"]
    assert mapping["pipe_b"] == ["slack"]


def test_multiple_channels_for_same_alert():
    router = make_router([
        RouteRule(channel="slack"),
        RouteRule(channel="email"),
    ])
    calls = []
    router.register_channel("slack", lambda a: calls.append("slack"))
    router.register_channel("email", lambda a: calls.append("email"))
    router.route(make_alert())
    assert "slack" in calls
    assert "email" in calls


def test_rule_matches_returns_false_for_low_level():
    rule = RouteRule(channel="x", min_level=AlertLevel.CRITICAL)
    assert not _rule_matches(rule, make_alert(level=AlertLevel.WARNING))


def test_rule_matches_returns_true_for_exact_level():
    rule = RouteRule(channel="x", min_level=AlertLevel.CRITICAL)
    assert _rule_matches(rule, make_alert(level=AlertLevel.CRITICAL))
