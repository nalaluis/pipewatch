"""Tests for pipewatch.stabilizer."""
import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.stabilizer import (
    StabilizerConfig,
    apply_stabilizer,
    clear_state,
    consecutive_count,
    record_hit,
    reset_hit,
    should_fire,
)


def make_alert(
    pipeline: str = "pipe-a",
    metric: str = "failure_rate",
    level: AlertLevel = AlertLevel.CRITICAL,
    message: str = "too many failures",
) -> Alert:
    return Alert(pipeline=pipeline, metric=metric, level=level, message=message)


@pytest.fixture(autouse=True)
def clean():
    clear_state()
    yield
    clear_state()


def test_consecutive_count_starts_at_zero():
    alert = make_alert()
    assert consecutive_count(alert) == 0


def test_record_hit_increments_count():
    alert = make_alert()
    record_hit(alert)
    assert consecutive_count(alert) == 1
    record_hit(alert)
    assert consecutive_count(alert) == 2


def test_reset_hit_clears_count():
    alert = make_alert()
    record_hit(alert)
    record_hit(alert)
    reset_hit(alert)
    assert consecutive_count(alert) == 0


def test_should_fire_false_below_threshold():
    cfg = StabilizerConfig(min_consecutive=3)
    alert = make_alert()
    record_hit(alert)
    record_hit(alert)
    assert should_fire(alert, cfg) is False


def test_should_fire_true_at_threshold():
    cfg = StabilizerConfig(min_consecutive=3)
    alert = make_alert()
    for _ in range(3):
        record_hit(alert)
    assert should_fire(alert, cfg) is True


def test_should_fire_true_above_threshold():
    cfg = StabilizerConfig(min_consecutive=2)
    alert = make_alert()
    for _ in range(5):
        record_hit(alert)
    assert should_fire(alert, cfg) is True


def test_should_fire_false_for_ok_level():
    cfg = StabilizerConfig(min_consecutive=1)
    alert = make_alert(level=AlertLevel.OK)
    record_hit(alert)
    assert should_fire(alert, cfg) is False


def test_apply_stabilizer_suppresses_until_threshold():
    cfg = StabilizerConfig(min_consecutive=3)
    alert = make_alert()
    assert apply_stabilizer([alert], cfg) == []
    assert apply_stabilizer([alert], cfg) == []
    result = apply_stabilizer([alert], cfg)
    assert result == [alert]


def test_apply_stabilizer_ok_alert_resets_state():
    cfg = StabilizerConfig(min_consecutive=2)
    alert = make_alert()
    ok_alert = make_alert(level=AlertLevel.OK)
    apply_stabilizer([alert], cfg)
    apply_stabilizer([ok_alert], cfg)
    assert consecutive_count(alert) == 0


def test_apply_stabilizer_multiple_pipelines_tracked_independently():
    cfg = StabilizerConfig(min_consecutive=2)
    a1 = make_alert(pipeline="pipe-a")
    a2 = make_alert(pipeline="pipe-b")
    apply_stabilizer([a1], cfg)
    apply_stabilizer([a2], cfg)
    result = apply_stabilizer([a1, a2], cfg)
    assert a1 in result
    assert a2 in result


def test_apply_stabilizer_no_alerts_returns_empty():
    cfg = StabilizerConfig(min_consecutive=1)
    assert apply_stabilizer([], cfg) == []
