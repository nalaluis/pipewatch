"""Enriches HealthResult objects with additional context metadata."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineMetric


@dataclass
class EnrichedResult:
    """A HealthResult decorated with extra context fields."""

    result: HealthResult
    pipeline: str
    tags: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    region: Optional[str] = None
    owner: Optional[str] = None
    env: Optional[str] = None

    @property
    def status(self):
        return self.result.status

    @property
    def metric(self) -> PipelineMetric:
        return self.result.metric


def enrich(
    result: HealthResult,
    pipeline: str,
    context: Optional[Dict] = None,
) -> EnrichedResult:
    """Attach context metadata to a HealthResult.

    Args:
        result:   The evaluated health result.
        pipeline: Name of the pipeline.
        context:  Optional dict with keys: tags, labels, region, owner, env.

    Returns:
        An EnrichedResult combining result and context.
    """
    ctx = context or {}
    return EnrichedResult(
        result=result,
        pipeline=pipeline,
        tags=list(ctx.get("tags", [])),
        labels=dict(ctx.get("labels", {})),
        region=ctx.get("region"),
        owner=ctx.get("owner"),
        env=ctx.get("env"),
    )


def format_enriched(er: EnrichedResult) -> str:
    """Return a human-readable summary of an EnrichedResult."""
    parts = [f"[{er.pipeline}] status={er.status.value}"]
    if er.env:
        parts.append(f"env={er.env}")
    if er.region:
        parts.append(f"region={er.region}")
    if er.owner:
        parts.append(f"owner={er.owner}")
    if er.tags:
        parts.append(f"tags={','.join(er.tags)}")
    if er.labels:
        label_str = ",".join(f"{k}={v}" for k, v in er.labels.items())
        parts.append(f"labels={label_str}")
    return " ".join(parts)
