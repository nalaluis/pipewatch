"""Pipeline health scorer — assigns a numeric score (0–100) to each pipeline
based on failure rate, throughput, and current status."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult


@dataclass
class ScorerConfig:
    failure_rate_weight: float = 0.5
    throughput_weight: float = 0.3
    status_weight: float = 0.2
    throughput_baseline: float = 100.0  # records/sec considered "perfect"


@dataclass
class PipelineScore:
    pipeline: str
    score: float          # 0.0 – 100.0
    grade: str            # A / B / C / D / F
    failure_rate_score: float
    throughput_score: float
    status_score: float

    def __str__(self) -> str:
        return f"{self.pipeline}: {self.score:.1f}/100 ({self.grade})"


_STATUS_SCORE: dict[PipelineStatus, float] = {
    PipelineStatus.HEALTHY: 100.0,
    PipelineStatus.WARNING: 60.0,
    PipelineStatus.CRITICAL: 0.0,
}


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 55:
        return "C"
    if score >= 35:
        return "D"
    return "F"


def score_pipeline(
    result: HealthResult,
    cfg: Optional[ScorerConfig] = None,
) -> PipelineScore:
    """Compute a weighted health score for a single pipeline."""
    if cfg is None:
        cfg = ScorerConfig()

    metric: PipelineMetric = result.metric

    fr_score = max(0.0, 100.0 - metric.failure_rate * 100.0)
    tp_ratio = min(1.0, metric.throughput / max(cfg.throughput_baseline, 1e-9))
    tp_score = tp_ratio * 100.0
    st_score = _STATUS_SCORE.get(result.status, 0.0)

    total = (
        fr_score * cfg.failure_rate_weight
        + tp_score * cfg.throughput_weight
        + st_score * cfg.status_weight
    )
    total = round(min(100.0, max(0.0, total)), 2)

    return PipelineScore(
        pipeline=metric.pipeline,
        score=total,
        grade=_grade(total),
        failure_rate_score=round(fr_score, 2),
        throughput_score=round(tp_score, 2),
        status_score=st_score,
    )


def score_all(
    results: list[HealthResult],
    cfg: Optional[ScorerConfig] = None,
) -> list[PipelineScore]:
    """Score every pipeline in a list of health results."""
    return [score_pipeline(r, cfg) for r in results]
