"""Tests for pipewatch.labeler."""
from __future__ import annotations

from pipewatch.health import HealthResult, HealthStatus
from pipewatch.labeler import (
    LabelRule,
    LabeledResult,
    apply_labels,
    filter_by_label,
    label_results,
    parse_label_rules,
)


def make_result(pipeline: str = "test-pipeline") -> HealthResult:
    return HealthResult(
        pipeline=pipeline,
        status=HealthStatus.HEALTHY,
        failure_rate=0.01,
        throughput=100.0,
        violations=[],
    )


# ---------------------------------------------------------------------------
# apply_labels
# ---------------------------------------------------------------------------

def test_apply_labels_no_rules():
    result = make_result("payments-etl")
    labeled = apply_labels(result, [])
    assert isinstance(labeled, LabeledResult)
    assert labeled.labels == []
    assert labeled.pipeline == "payments-etl"


def test_apply_labels_matching_pattern():
    rules = [LabelRule(pattern="payments-*", labels=["finance", "critical"])]
    labeled = apply_labels(make_result("payments-etl"), rules)
    assert "finance" in labeled.labels
    assert "critical" in labeled.labels


def test_apply_labels_non_matching_pattern():
    rules = [LabelRule(pattern="analytics-*", labels=["analytics"])]
    labeled = apply_labels(make_result("payments-etl"), rules)
    assert labeled.labels == []


def test_apply_labels_multiple_rules_merged():
    rules = [
        LabelRule(pattern="payments-*", labels=["finance"]),
        LabelRule(pattern="*", labels=["monitored"]),
    ]
    labeled = apply_labels(make_result("payments-etl"), rules)
    assert "finance" in labeled.labels
    assert "monitored" in labeled.labels


def test_apply_labels_no_duplicate_labels():
    rules = [
        LabelRule(pattern="*", labels=["monitored"]),
        LabelRule(pattern="*", labels=["monitored"]),
    ]
    labeled = apply_labels(make_result("any-pipeline"), rules)
    assert labeled.labels.count("monitored") == 1


# ---------------------------------------------------------------------------
# label_results
# ---------------------------------------------------------------------------

def test_label_results_returns_one_per_result():
    results = [make_result("a"), make_result("b")]
    rules = [LabelRule(pattern="*", labels=["all"])]
    labeled = label_results(results, rules)
    assert len(labeled) == 2
    assert all("all" in lr.labels for lr in labeled)


# ---------------------------------------------------------------------------
# parse_label_rules
# ---------------------------------------------------------------------------

def test_parse_label_rules_from_dicts():
    raw = [
        {"pattern": "payments-*", "labels": ["finance"]},
        {"pattern": "*-nightly", "labels": ["scheduled", "nightly"]},
    ]
    rules = parse_label_rules(raw)
    assert len(rules) == 2
    assert rules[0].pattern == "payments-*"
    assert rules[1].labels == ["scheduled", "nightly"]


def test_parse_label_rules_defaults():
    rules = parse_label_rules([{}])
    assert rules[0].pattern == "*"
    assert rules[0].labels == []


# ---------------------------------------------------------------------------
# filter_by_label
# ---------------------------------------------------------------------------

def test_filter_by_label_returns_matching():
    rules = [
        LabelRule(pattern="payments-*", labels=["finance"]),
        LabelRule(pattern="analytics-*", labels=["analytics"]),
    ]
    labeled = label_results(
        [make_result("payments-etl"), make_result("analytics-daily")],
        rules,
    )
    finance = filter_by_label(labeled, "finance")
    assert len(finance) == 1
    assert finance[0].pipeline == "payments-etl"


def test_filter_by_label_empty_when_no_match():
    labeled = label_results([make_result("other-pipeline")], [])
    assert filter_by_label(labeled, "finance") == []
