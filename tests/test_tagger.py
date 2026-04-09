"""Tests for pipewatch.tagger and pipewatch.tag_config."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.health import HealthResult, HealthThresholds
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.tagger import TagRule, TaggedResult, apply_tags, apply_tags_to_all
from pipewatch.tag_config import load_tag_config, rules_for_pipeline


def make_result(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.OK,
    failure_rate: float = 0.0,
    throughput: float = 100.0,
) -> HealthResult:
    metric = PipelineMetric(
        pipeline=name,
        status=status,
        failure_rate=failure_rate,
        throughput=throughput,
    )
    thresholds = HealthThresholds()
    return HealthResult(pipeline=name, metric=metric, thresholds=thresholds, violations=[])


# --- apply_tags ---

def test_apply_tags_no_rules():
    result = make_result()
    tagged = apply_tags(result, [])
    assert tagged.tags == []
    assert tagged.pipeline == "pipe"


def test_apply_tags_matching_status():
    result = make_result(status=PipelineStatus.CRITICAL)
    rules = [TagRule(tag="crit", status="critical")]
    tagged = apply_tags(result, rules)
    assert "crit" in tagged.tags


def test_apply_tags_non_matching_status():
    result = make_result(status=PipelineStatus.OK)
    rules = [TagRule(tag="crit", status="critical")]
    tagged = apply_tags(result, rules)
    assert tagged.tags == []


def test_apply_tags_min_failure_rate():
    result = make_result(failure_rate=0.25)
    rules = [TagRule(tag="high-failure", min_failure_rate=0.1)]
    tagged = apply_tags(result, rules)
    assert "high-failure" in tagged.tags


def test_apply_tags_max_throughput():
    result = make_result(throughput=5.0)
    rules = [TagRule(tag="slow", max_throughput=10.0)]
    tagged = apply_tags(result, rules)
    assert "slow" in tagged.tags


def test_apply_tags_name_pattern_wildcard():
    result = make_result(name="ingest-orders")
    rules = [TagRule(tag="ingestion", name_pattern="ingest-*")]
    tagged = apply_tags(result, rules)
    assert "ingestion" in tagged.tags


def test_apply_tags_name_pattern_no_match():
    result = make_result(name="transform-orders")
    rules = [TagRule(tag="ingestion", name_pattern="ingest-*")]
    tagged = apply_tags(result, rules)
    assert tagged.tags == []


def test_apply_tags_multiple_rules_merged():
    result = make_result(status=PipelineStatus.WARNING, failure_rate=0.15, throughput=5.0)
    rules = [
        TagRule(tag="warn", status="warning"),
        TagRule(tag="high-failure", min_failure_rate=0.1),
        TagRule(tag="slow", max_throughput=10.0),
    ]
    tagged = apply_tags(result, rules)
    assert set(tagged.tags) == {"warn", "high-failure", "slow"}


def test_apply_tags_to_all():
    results = [make_result(name="a", failure_rate=0.2), make_result(name="b", failure_rate=0.0)]
    rules = [TagRule(tag="hi", min_failure_rate=0.1)]
    tagged = apply_tags_to_all(results, rules)
    assert len(tagged) == 2
    assert "hi" in tagged[0].tags
    assert "hi" not in tagged[1].tags


# --- tag_config ---

def test_load_tag_config_missing_file_returns_empty(tmp_path):
    rules = load_tag_config(str(tmp_path / "nonexistent.yaml"))
    assert rules == []


def test_load_tag_config_parses_rules(tmp_path):
    cfg = tmp_path / "tags.yaml"
    cfg.write_text(textwrap.dedent("""\
        tag_rules:
          - tag: slow
            max_throughput: 5.0
          - tag: critical-pipeline
            status: critical
    """))
    rules = load_tag_config(str(cfg))
    assert len(rules) == 2
    assert rules[0].tag == "slow"
    assert rules[0].max_throughput == 5.0
    assert rules[1].status == "critical"


def test_rules_for_pipeline_filters_by_pattern():
    rules = [
        TagRule(tag="a", name_pattern="ingest-*"),
        TagRule(tag="b", name_pattern="transform-*"),
        TagRule(tag="c"),  # no pattern — applies to all
    ]
    filtered = rules_for_pipeline("ingest-orders", rules)
    tags = [r.tag for r in filtered]
    assert "a" in tags
    assert "c" in tags
    assert "b" not in tags
