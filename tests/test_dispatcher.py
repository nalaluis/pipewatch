"""Tests for pipewatch.dispatcher and pipewatch.dispatcher_config."""
from __future__ import annotations

from pathlib import Path
from typing import List

import pytest
import yaml

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.dispatcher import DispatchRule, Dispatcher, make_dispatcher
from pipewatch.dispatcher_config import load_dispatcher_rules


def make_alert(pipeline: str = "pipe_a", level: AlertLevel = AlertLevel.CRITICAL) -> Alert:
    return Alert(pipeline=pipeline, level=level, message="test alert")


# --- Dispatcher unit tests ---

def test_dispatch_invokes_matching_handler():
    received: List[Alert] = []
    d = Dispatcher()
    d.register("sink", received.append)
    d.add_rule(DispatchRule(pipeline=None, min_level=AlertLevel.WARNING, handler_name="sink"))
    alert = make_alert(level=AlertLevel.CRITICAL)
    invoked = d.dispatch(alert)
    assert "sink" in invoked
    assert len(received) == 1


def test_dispatch_skips_below_min_level():
    received: List[Alert] = []
    d = Dispatcher()
    d.register("sink", received.append)
    d.add_rule(DispatchRule(pipeline=None, min_level=AlertLevel.CRITICAL, handler_name="sink"))
    alert = make_alert(level=AlertLevel.WARNING)
    invoked = d.dispatch(alert)
    assert invoked == []
    assert received == []


def test_dispatch_filters_by_pipeline():
    received: List[Alert] = []
    d = Dispatcher()
    d.register("sink", received.append)
    d.add_rule(DispatchRule(pipeline="pipe_b", min_level=AlertLevel.WARNING, handler_name="sink"))
    alert = make_alert(pipeline="pipe_a", level=AlertLevel.CRITICAL)
    invoked = d.dispatch(alert)
    assert invoked == []


def test_dispatch_pipeline_none_matches_any():
    received: List[Alert] = []
    d = Dispatcher()
    d.register("sink", received.append)
    d.add_rule(DispatchRule(pipeline=None, min_level=AlertLevel.WARNING, handler_name="sink"))
    for name in ["alpha", "beta", "gamma"]:
        d.dispatch(make_alert(pipeline=name))
    assert len(received) == 3


def test_dispatch_all_returns_mapping():
    received: List[Alert] = []
    d = Dispatcher()
    d.register("sink", received.append)
    d.add_rule(DispatchRule(pipeline=None, min_level=AlertLevel.WARNING, handler_name="sink"))
    alerts = [make_alert("p1"), make_alert("p2")]
    result = d.dispatch_all(alerts)
    assert "p1" in result
    assert "p2" in result


def test_make_dispatcher_convenience():
    received: List[Alert] = []
    rules = [DispatchRule(pipeline=None, min_level=AlertLevel.WARNING, handler_name="h")]
    d = make_dispatcher(rules, {"h": received.append})
    d.dispatch(make_alert())
    assert len(received) == 1


def test_missing_handler_name_is_skipped():
    d = Dispatcher()
    d.add_rule(DispatchRule(pipeline=None, min_level=AlertLevel.WARNING, handler_name="ghost"))
    invoked = d.dispatch(make_alert())
    assert invoked == []


# --- Config tests ---

def test_load_dispatcher_rules_missing_file_returns_empty(tmp_path):
    result = load_dispatcher_rules(str(tmp_path / "missing.yaml"))
    assert result == []


def test_load_dispatcher_rules_parses_rules(tmp_path):
    cfg = tmp_path / "dispatcher.yaml"
    cfg.write_text(yaml.dump({
        "rules": [
            {"pipeline": "etl", "min_level": "critical", "handler": "pagerduty"},
            {"min_level": "warning", "handler": "slack"},
        ]
    }))
    rules = load_dispatcher_rules(str(cfg))
    assert len(rules) == 2
    assert rules[0].pipeline == "etl"
    assert rules[0].handler_name == "pagerduty"
    assert rules[1].pipeline is None
    assert rules[1].handler_name == "slack"


def test_load_dispatcher_rules_skips_entry_without_handler(tmp_path):
    cfg = tmp_path / "dispatcher.yaml"
    cfg.write_text(yaml.dump({"rules": [{"min_level": "warning"}]}))
    rules = load_dispatcher_rules(str(cfg))
    assert rules == []
