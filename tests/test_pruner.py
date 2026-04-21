"""Tests for pipewatch.pruner."""

from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.pruner import PrunerConfig, PruneResult, prune_alerts


def make_alert(
    pipeline: str = "pipe",
    level: AlertLevel = AlertLevel.WARNING,
    metric: str = "failure_rate",
    message: str = "test alert",
) -> Alert:
    return Alert(pipeline=pipeline, level=level, metric=metric, message=message)


# ---------------------------------------------------------------------------
# PruneResult helpers
# ---------------------------------------------------------------------------

def test_prune_result_counts():
    kept = [make_alert()]
    removed = [make_alert(pipeline="other")]
    result = PruneResult(kept=kept, removed=removed)
    assert result.kept_count == 1
    assert result.removed_count == 1


def test_prune_result_str():
    result = PruneResult(kept=[], removed=[])
    assert "PruneResult" in str(result)


# ---------------------------------------------------------------------------
# prune_alerts — level filtering
# ---------------------------------------------------------------------------

def test_prune_removes_ok_alerts_by_default():
    alerts = [make_alert(level=AlertLevel.OK), make_alert(level=AlertLevel.WARNING)]
    result = prune_alerts(alerts)
    assert len(result.kept) == 1
    assert result.kept[0].level == AlertLevel.WARNING
    assert len(result.removed) == 1


def test_prune_keeps_critical_alerts():
    alerts = [make_alert(level=AlertLevel.CRITICAL)]
    result = prune_alerts(alerts)
    assert len(result.kept) == 1


def test_prune_min_level_critical_removes_warnings():
    cfg = PrunerConfig(min_level=AlertLevel.CRITICAL)
    alerts = [
        make_alert(level=AlertLevel.WARNING, pipeline="a"),
        make_alert(level=AlertLevel.CRITICAL, pipeline="b"),
    ]
    result = prune_alerts(alerts, cfg)
    assert len(result.kept) == 1
    assert result.kept[0].pipeline == "b"


# ---------------------------------------------------------------------------
# prune_alerts — exclude pipelines
# ---------------------------------------------------------------------------

def test_prune_excludes_named_pipelines():
    cfg = PrunerConfig(exclude_pipelines=["skip_me"])
    alerts = [
        make_alert(pipeline="skip_me", level=AlertLevel.CRITICAL),
        make_alert(pipeline="keep_me", level=AlertLevel.CRITICAL),
    ]
    result = prune_alerts(alerts, cfg)
    assert len(result.kept) == 1
    assert result.kept[0].pipeline == "keep_me"


# ---------------------------------------------------------------------------
# prune_alerts — deduplication
# ---------------------------------------------------------------------------

def test_prune_deduplicates_same_pipeline():
    alerts = [
        make_alert(pipeline="pipe", level=AlertLevel.CRITICAL),
        make_alert(pipeline="pipe", level=AlertLevel.WARNING),
    ]
    result = prune_alerts(alerts)
    assert len(result.kept) == 1
    assert result.kept[0].level == AlertLevel.CRITICAL


def test_prune_no_dedup_when_disabled():
    cfg = PrunerConfig(dedupe_pipeline=False)
    alerts = [
        make_alert(pipeline="pipe", level=AlertLevel.CRITICAL),
        make_alert(pipeline="pipe", level=AlertLevel.WARNING),
    ]
    result = prune_alerts(alerts, cfg)
    assert len(result.kept) == 2


# ---------------------------------------------------------------------------
# prune_alerts — max_alerts cap
# ---------------------------------------------------------------------------

def test_prune_enforces_max_alerts():
    cfg = PrunerConfig(max_alerts=3, dedupe_pipeline=False)
    alerts = [make_alert(pipeline=f"p{i}", level=AlertLevel.CRITICAL) for i in range(6)]
    result = prune_alerts(alerts, cfg)
    assert len(result.kept) == 3
    assert len(result.removed) == 3


def test_prune_empty_list_returns_empty():
    result = prune_alerts([])
    assert result.kept == []
    assert result.removed == []
