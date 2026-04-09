"""Tests for pipewatch.enricher."""

import pytest

from pipewatch.enricher import EnrichedResult, enrich, format_enriched
from pipewatch.health import HealthResult, HealthThresholds
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(name="pipe", processed=100, failed=5, status=PipelineStatus.OK):
    return PipelineMetric(
        pipeline=name,
        processed=processed,
        failed=failed,
        status=status,
    )


def make_result(status=PipelineStatus.OK):
    metric = make_metric(status=status)
    thresholds = HealthThresholds()
    return HealthResult(metric=metric, status=status, violations=[])


# ---------------------------------------------------------------------------
# enrich()
# ---------------------------------------------------------------------------

def test_enrich_returns_enriched_result():
    result = make_result()
    er = enrich(result, pipeline="my_pipe")
    assert isinstance(er, EnrichedResult)
    assert er.pipeline == "my_pipe"


def test_enrich_no_context_has_empty_defaults():
    er = enrich(make_result(), pipeline="p")
    assert er.tags == []
    assert er.labels == {}
    assert er.region is None
    assert er.owner is None
    assert er.env is None


def test_enrich_with_full_context():
    ctx = {
        "tags": ["critical", "nightly"],
        "labels": {"team": "data", "tier": "1"},
        "region": "us-east-1",
        "owner": "alice",
        "env": "prod",
    }
    er = enrich(make_result(), pipeline="etl", context=ctx)
    assert er.tags == ["critical", "nightly"]
    assert er.labels == {"team": "data", "tier": "1"}
    assert er.region == "us-east-1"
    assert er.owner == "alice"
    assert er.env == "prod"


def test_enrich_status_proxied():
    er = enrich(make_result(status=PipelineStatus.CRITICAL), pipeline="p")
    assert er.status == PipelineStatus.CRITICAL


def test_enrich_metric_proxied():
    result = make_result()
    er = enrich(result, pipeline="p")
    assert er.metric is result.metric


# ---------------------------------------------------------------------------
# format_enriched()
# ---------------------------------------------------------------------------

def test_format_enriched_minimal():
    er = enrich(make_result(), pipeline="pipe_a")
    text = format_enriched(er)
    assert "pipe_a" in text
    assert "status=" in text


def test_format_enriched_includes_env_and_region():
    er = enrich(make_result(), pipeline="p", context={"env": "staging", "region": "eu-west-1"})
    text = format_enriched(er)
    assert "env=staging" in text
    assert "region=eu-west-1" in text


def test_format_enriched_includes_tags():
    er = enrich(make_result(), pipeline="p", context={"tags": ["a", "b"]})
    text = format_enriched(er)
    assert "tags=a,b" in text


def test_format_enriched_includes_labels():
    er = enrich(make_result(), pipeline="p", context={"labels": {"k": "v"}})
    text = format_enriched(er)
    assert "labels=k=v" in text


def test_format_enriched_omits_missing_fields():
    er = enrich(make_result(), pipeline="p")
    text = format_enriched(er)
    assert "region" not in text
    assert "owner" not in text
    assert "env" not in text
    assert "tags" not in text
    assert "labels" not in text
