"""Tests for pipewatch.splitter."""
from __future__ import annotations

import pytest

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.splitter import SplitRule, SplitBucket, split, format_split


def make_result(
    name: str,
    status: PipelineStatus = PipelineStatus.HEALTHY,
    failure_rate: float = 0.0,
    throughput: float = 100.0,
) -> HealthResult:
    metric = PipelineMetric(
        pipeline=name,
        total=100,
        failed=int(failure_rate * 100),
        records_processed=int(throughput),
    )
    return HealthResult(pipeline=name, status=status, metric=metric, violations=[])


def test_split_empty_results_returns_empty():
    rules = [SplitRule(bucket="critical", status=PipelineStatus.CRITICAL)]
    assert split([], rules) == {}


def test_split_no_rules_all_go_to_default():
    results = [make_result("pipe-a"), make_result("pipe-b")]
    buckets = split(results, [], default_bucket="unmatched")
    assert "unmatched" in buckets
    assert buckets["unmatched"].size == 2


def test_split_by_status_critical():
    results = [
        make_result("pipe-a", PipelineStatus.CRITICAL),
        make_result("pipe-b", PipelineStatus.HEALTHY),
    ]
    rules = [SplitRule(bucket="critical", status=PipelineStatus.CRITICAL)]
    buckets = split(results, rules)
    assert "critical" in buckets
    assert buckets["critical"].size == 1
    assert buckets["critical"].results[0].pipeline == "pipe-a"


def test_split_unmatched_goes_to_default():
    results = [make_result("pipe-x", PipelineStatus.WARNING)]
    rules = [SplitRule(bucket="critical", status=PipelineStatus.CRITICAL)]
    buckets = split(results, rules, default_bucket="other")
    assert "other" in buckets
    assert buckets["other"].size == 1


def test_split_first_matching_rule_wins():
    results = [make_result("pipe-a", PipelineStatus.CRITICAL, failure_rate=0.9)]
    rules = [
        SplitRule(bucket="critical", status=PipelineStatus.CRITICAL),
        SplitRule(bucket="high_failure", min_failure_rate=0.5),
    ]
    buckets = split(results, rules)
    assert "critical" in buckets
    assert "high_failure" not in buckets


def test_split_by_pattern():
    results = [
        make_result("etl-prod-1"),
        make_result("etl-staging-1"),
        make_result("batch-prod-1"),
    ]
    rules = [SplitRule(bucket="etl", pattern="etl-*")]
    buckets = split(results, rules)
    assert buckets["etl"].size == 2
    assert buckets["unmatched"].size == 1


def test_split_by_min_failure_rate():
    results = [
        make_result("pipe-a", failure_rate=0.6),
        make_result("pipe-b", failure_rate=0.2),
    ]
    rules = [SplitRule(bucket="high_failure", min_failure_rate=0.5)]
    buckets = split(results, rules)
    assert buckets["high_failure"].size == 1
    assert buckets["high_failure"].results[0].pipeline == "pipe-a"


def test_split_by_max_failure_rate():
    results = [
        make_result("pipe-a", failure_rate=0.1),
        make_result("pipe-b", failure_rate=0.8),
    ]
    rules = [SplitRule(bucket="low_failure", max_failure_rate=0.3)]
    buckets = split(results, rules)
    assert buckets["low_failure"].size == 1
    assert buckets["low_failure"].results[0].pipeline == "pipe-a"


def test_format_split_empty():
    assert format_split({}) == "No buckets."


def test_format_split_shows_bucket_names():
    results = [make_result("pipe-a", PipelineStatus.CRITICAL)]
    rules = [SplitRule(bucket="critical", status=PipelineStatus.CRITICAL)]
    buckets = split(results, rules)
    output = format_split(buckets)
    assert "critical" in output
    assert "pipe-a" in output


def test_split_bucket_size_property():
    bucket = SplitBucket(name="test")
    assert bucket.size == 0
    bucket.results.append(make_result("pipe-a"))
    assert bucket.size == 1
