"""Tests for pipewatch.fingerprint."""

from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.fingerprint import (
    Fingerprint,
    compute_fingerprint,
    fingerprint_alerts,
    group_by_fingerprint,
)


def make_alert(
    pipeline: str = "etl",
    metric: str = "failure_rate",
    level: AlertLevel = AlertLevel.CRITICAL,
    message: str = "too many failures",
) -> Alert:
    return Alert(pipeline=pipeline, metric=metric, level=level, message=message)


def test_compute_fingerprint_returns_fingerprint():
    alert = make_alert()
    fp = compute_fingerprint(alert)
    assert isinstance(fp, Fingerprint)
    assert fp.pipeline == "etl"
    assert fp.metric == "failure_rate"
    assert fp.level == "critical"


def test_fingerprint_digest_is_64_hex_chars():
    fp = compute_fingerprint(make_alert())
    assert len(fp.digest) == 64
    assert all(c in "0123456789abcdef" for c in fp.digest)


def test_fingerprint_short_is_8_chars():
    fp = compute_fingerprint(make_alert())
    assert len(fp.short()) == 8


def test_fingerprint_str_format():
    fp = compute_fingerprint(make_alert(pipeline="pipe1", metric="throughput", level=AlertLevel.WARNING))
    s = str(fp)
    assert s.startswith("pipe1/throughput/warning:")
    assert len(s) == len("pipe1/throughput/warning:") + 8


def test_same_alert_produces_same_fingerprint():
    a1 = make_alert()
    a2 = make_alert()
    assert compute_fingerprint(a1).digest == compute_fingerprint(a2).digest


def test_different_pipeline_produces_different_fingerprint():
    a1 = make_alert(pipeline="pipe_a")
    a2 = make_alert(pipeline="pipe_b")
    assert compute_fingerprint(a1).digest != compute_fingerprint(a2).digest


def test_different_level_produces_different_fingerprint():
    a1 = make_alert(level=AlertLevel.WARNING)
    a2 = make_alert(level=AlertLevel.CRITICAL)
    assert compute_fingerprint(a1).digest != compute_fingerprint(a2).digest


def test_fingerprint_alerts_returns_mapping():
    alerts = [make_alert(pipeline="a"), make_alert(pipeline="b")]
    result = fingerprint_alerts(alerts)
    assert set(result.keys()) == {"0", "1"}
    assert result["0"].pipeline == "a"
    assert result["1"].pipeline == "b"


def test_fingerprint_alerts_empty_list():
    assert fingerprint_alerts([]) == {}


def test_group_by_fingerprint_groups_identical_alerts():
    a1 = make_alert(message="first occurrence")
    a2 = make_alert(message="second occurrence")
    groups = group_by_fingerprint([a1, a2])
    assert len(groups) == 1
    key = next(iter(groups))
    assert len(groups[key]) == 2


def test_group_by_fingerprint_separates_distinct_alerts():
    a1 = make_alert(pipeline="pipe_a")
    a2 = make_alert(pipeline="pipe_b")
    groups = group_by_fingerprint([a1, a2])
    assert len(groups) == 2


def test_group_by_fingerprint_empty():
    assert group_by_fingerprint([]) == {}
