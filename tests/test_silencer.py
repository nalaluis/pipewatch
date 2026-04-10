"""Tests for pipewatch.silencer."""

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.silencer import (
    SilenceRule,
    SilencerState,
    apply_silencer,
    should_silence,
    reset_state,
    get_state,
)


def make_alert(pipeline: str = "etl", level: AlertLevel = AlertLevel.WARNING) -> Alert:
    return Alert(pipeline=pipeline, level=level, message="test alert")


@pytest.fixture(autouse=True)
def clean_state():
    reset_state()
    yield
    reset_state()


def _future(seconds: int = 3600) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _past(seconds: int = 3600) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def test_no_rules_does_not_silence():
    alert = make_alert()
    assert should_silence(alert) is False


def test_exact_pipeline_match_silences():
    state = get_state()
    state.add_rule(SilenceRule(pipeline="etl", until=_future()))
    assert should_silence(make_alert(pipeline="etl")) is True


def test_non_matching_pipeline_not_silenced():
    state = get_state()
    state.add_rule(SilenceRule(pipeline="other", until=_future()))
    assert should_silence(make_alert(pipeline="etl")) is False


def test_wildcard_pipeline_silences():
    state = get_state()
    state.add_rule(SilenceRule(pipeline="etl_*", until=_future()))
    assert should_silence(make_alert(pipeline="etl_daily")) is True
    assert should_silence(make_alert(pipeline="batch")) is False


def test_expired_rule_does_not_silence():
    state = get_state()
    state.add_rule(SilenceRule(pipeline="etl", until=_past()))
    assert should_silence(make_alert(pipeline="etl")) is False


def test_indefinite_rule_silences():
    state = get_state()
    state.add_rule(SilenceRule(pipeline="etl"))  # until=None
    assert should_silence(make_alert(pipeline="etl")) is True


def test_level_specific_rule_only_silences_matching_level():
    state = get_state()
    state.add_rule(SilenceRule(pipeline="etl", level=AlertLevel.WARNING, until=_future()))
    assert should_silence(make_alert(pipeline="etl", level=AlertLevel.WARNING)) is True
    assert should_silence(make_alert(pipeline="etl", level=AlertLevel.CRITICAL)) is False


def test_apply_silencer_filters_silenced_alerts():
    state = get_state()
    state.add_rule(SilenceRule(pipeline="etl", until=_future()))
    alerts = [make_alert("etl"), make_alert("batch")]
    result = apply_silencer(alerts)
    assert len(result) == 1
    assert result[0].pipeline == "batch"


def test_apply_silencer_empty_list():
    assert apply_silencer([]) == []


def test_clear_expired_removes_only_expired_rules():
    state = SilencerState()
    state.add_rule(SilenceRule(pipeline="a", until=_past()))
    state.add_rule(SilenceRule(pipeline="b", until=_future()))
    removed = state.clear_expired()
    assert removed == 1
    assert len(state.active_rules()) == 1


def test_remove_rule_by_pipeline():
    state = SilencerState()
    state.add_rule(SilenceRule(pipeline="etl", until=_future()))
    state.add_rule(SilenceRule(pipeline="batch", until=_future()))
    removed = state.remove_rule("etl")
    assert removed == 1
    assert all(r.pipeline != "etl" for r in state.active_rules())
