"""Tests for pipewatch.budget alert-budget tracking."""
from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.budget import (
    BudgetConfig,
    apply_budget,
    budget_exhausted,
    record_alert,
    reset_budget,
)


def make_alert(pipeline: str = "pipe", level: AlertLevel = AlertLevel.WARNING) -> Alert:
    return Alert(pipeline=pipeline, level=level, message="test alert")


@pytest.fixture(autouse=True)
def clean_state():
    reset_budget()
    yield
    reset_budget()


def test_budget_not_exhausted_initially():
    cfg = BudgetConfig(max_alerts=5, window_seconds=60)
    alert = make_alert()
    assert not budget_exhausted(alert, cfg)


def test_budget_exhausted_after_max_alerts():
    cfg = BudgetConfig(max_alerts=3, window_seconds=60)
    alert = make_alert()
    for _ in range(3):
        record_alert(alert, cfg)
    assert budget_exhausted(alert, cfg)


def test_apply_budget_allows_up_to_max():
    cfg = BudgetConfig(max_alerts=2, window_seconds=60)
    alerts = [make_alert() for _ in range(5)]
    result = apply_budget(alerts, cfg)
    assert len(result) == 2


def test_apply_budget_returns_all_when_under_limit():
    cfg = BudgetConfig(max_alerts=10, window_seconds=60)
    alerts = [make_alert() for _ in range(4)]
    result = apply_budget(alerts, cfg)
    assert len(result) == 4


def test_apply_budget_empty_list():
    cfg = BudgetConfig(max_alerts=5, window_seconds=60)
    assert apply_budget([], cfg) == []


def test_per_pipeline_budget_tracks_separately():
    cfg = BudgetConfig(max_alerts=2, window_seconds=60, per_pipeline=True)
    a1 = make_alert(pipeline="pipe-a")
    a2 = make_alert(pipeline="pipe-b")
    alerts = [a1, a1, a1, a2, a2, a2]
    result = apply_budget(alerts, cfg)
    # 2 from pipe-a + 2 from pipe-b = 4
    assert len(result) == 4


def test_global_budget_shared_across_pipelines():
    cfg = BudgetConfig(max_alerts=3, window_seconds=60, per_pipeline=False)
    alerts = [
        make_alert(pipeline="pipe-a"),
        make_alert(pipeline="pipe-b"),
        make_alert(pipeline="pipe-c"),
        make_alert(pipeline="pipe-d"),
    ]
    result = apply_budget(alerts, cfg)
    assert len(result) == 3


def test_reset_budget_clears_counts():
    cfg = BudgetConfig(max_alerts=2, window_seconds=60)
    alert = make_alert()
    for _ in range(2):
        record_alert(alert, cfg)
    assert budget_exhausted(alert, cfg)
    reset_budget()
    assert not budget_exhausted(alert, cfg)
