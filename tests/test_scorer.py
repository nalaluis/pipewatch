"""Tests for pipewatch.scorer and pipewatch.scorer_config."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult, HealthThresholds
from pipewatch.scorer import (
    ScorerConfig,
    PipelineScore,
    score_pipeline,
    score_all,
    _grade,
)
from pipewatch.scorer_config import load_scorer_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_metric(
    pipeline: str = "etl",
    failure_rate: float = 0.0,
    throughput: float = 100.0,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        failure_rate=failure_rate,
        throughput=throughput,
        status=status,
    )


def make_result(
    failure_rate: float = 0.0,
    throughput: float = 100.0,
    status: PipelineStatus = PipelineStatus.HEALTHY,
    pipeline: str = "etl",
) -> HealthResult:
    metric = make_metric(pipeline, failure_rate, throughput, status)
    return HealthResult(
        metric=metric,
        status=status,
        violations=[],
        thresholds=HealthThresholds(),
    )


# ---------------------------------------------------------------------------
# _grade
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("score,expected", [
    (95.0, "A"),
    (80.0, "B"),
    (60.0, "C"),
    (40.0, "D"),
    (20.0, "F"),
])
def test_grade_boundaries(score, expected):
    assert _grade(score) == expected


# ---------------------------------------------------------------------------
# score_pipeline
# ---------------------------------------------------------------------------

def test_score_pipeline_returns_pipeline_score():
    result = make_result()
    ps = score_pipeline(result)
    assert isinstance(ps, PipelineScore)
    assert ps.pipeline == "etl"


def test_score_perfect_pipeline():
    result = make_result(failure_rate=0.0, throughput=100.0, status=PipelineStatus.HEALTHY)
    ps = score_pipeline(result)
    assert ps.score == 100.0
    assert ps.grade == "A"


def test_score_critical_pipeline_is_low():
    result = make_result(failure_rate=1.0, throughput=0.0, status=PipelineStatus.CRITICAL)
    ps = score_pipeline(result)
    assert ps.score < 20.0
    assert ps.grade == "F"


def test_score_respects_custom_config():
    cfg = ScorerConfig(
        failure_rate_weight=1.0,
        throughput_weight=0.0,
        status_weight=0.0,
        throughput_baseline=100.0,
    )
    result = make_result(failure_rate=0.5, throughput=50.0, status=PipelineStatus.WARNING)
    ps = score_pipeline(result, cfg)
    assert abs(ps.score - 50.0) < 0.01


def test_score_clamped_to_100():
    result = make_result(failure_rate=0.0, throughput=9999.0, status=PipelineStatus.HEALTHY)
    ps = score_pipeline(result)
    assert ps.score <= 100.0


def test_score_clamped_to_zero():
    result = make_result(failure_rate=2.0, throughput=0.0, status=PipelineStatus.CRITICAL)
    ps = score_pipeline(result)
    assert ps.score >= 0.0


def test_score_all_returns_list_of_scores():
    results = [
        make_result(pipeline="a"),
        make_result(pipeline="b"),
        make_result(pipeline="c"),
    ]
    scores = score_all(results)
    assert len(scores) == 3
    assert {s.pipeline for s in scores} == {"a", "b", "c"}


def test_pipeline_score_str():
    ps = PipelineScore(
        pipeline="my-pipe",
        score=87.5,
        grade="B",
        failure_rate_score=90.0,
        throughput_score=80.0,
        status_score=100.0,
    )
    assert "my-pipe" in str(ps)
    assert "87.5" in str(ps)
    assert "B" in str(ps)


# ---------------------------------------------------------------------------
# scorer_config
# ---------------------------------------------------------------------------

def test_load_scorer_config_missing_file_returns_defaults(tmp_path):
    cfg = load_scorer_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.failure_rate_weight == 0.5
    assert cfg.throughput_weight == 0.3
    assert cfg.status_weight == 0.2
    assert cfg.throughput_baseline == 100.0


def test_load_scorer_config_parses_yaml(tmp_path):
    yaml_text = textwrap.dedent("""
        scorer:
          failure_rate_weight: 0.6
          throughput_weight: 0.2
          status_weight: 0.2
          throughput_baseline: 200.0
    """)
    p = tmp_path / "scorer.yaml"
    p.write_text(yaml_text)
    cfg = load_scorer_config(str(p))
    assert cfg.failure_rate_weight == 0.6
    assert cfg.throughput_baseline == 200.0


def test_load_scorer_config_partial_yaml(tmp_path):
    p = tmp_path / "scorer.yaml"
    p.write_text("scorer:\n  throughput_baseline: 50.0\n")
    cfg = load_scorer_config(str(p))
    assert cfg.throughput_baseline == 50.0
    assert cfg.failure_rate_weight == 0.5  # default
