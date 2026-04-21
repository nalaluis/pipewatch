"""Pipeline result splitter — partitions HealthResults into named buckets by rule."""
from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Callable, Dict, List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


@dataclass
class SplitRule:
    bucket: str
    pattern: Optional[str] = None          # glob on pipeline name
    status: Optional[PipelineStatus] = None
    min_failure_rate: Optional[float] = None
    max_failure_rate: Optional[float] = None


@dataclass
class SplitBucket:
    name: str
    results: List[HealthResult] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.results)

    def __str__(self) -> str:  # pragma: no cover
        return f"SplitBucket({self.name!r}, size={self.size})"


def _matches_rule(result: HealthResult, rule: SplitRule) -> bool:
    if rule.pattern and not fnmatch(result.pipeline, rule.pattern):
        return False
    if rule.status and result.status != rule.status:
        return False
    fr = result.metric.failure_rate if result.metric else 0.0
    if rule.min_failure_rate is not None and fr < rule.min_failure_rate:
        return False
    if rule.max_failure_rate is not None and fr > rule.max_failure_rate:
        return False
    return True


def split(
    results: List[HealthResult],
    rules: List[SplitRule],
    default_bucket: str = "unmatched",
) -> Dict[str, SplitBucket]:
    """Partition results into named buckets according to the first matching rule."""
    buckets: Dict[str, SplitBucket] = {}

    for result in results:
        matched = False
        for rule in rules:
            if _matches_rule(result, rule):
                buckets.setdefault(rule.bucket, SplitBucket(rule.bucket))
                buckets[rule.bucket].results.append(result)
                matched = True
                break
        if not matched:
            buckets.setdefault(default_bucket, SplitBucket(default_bucket))
            buckets[default_bucket].results.append(result)

    return buckets


def format_split(buckets: Dict[str, SplitBucket]) -> str:
    """Render a human-readable summary of split buckets."""
    if not buckets:
        return "No buckets."
    lines = []
    for name, bucket in sorted(buckets.items()):
        pipelines = ", ".join(r.pipeline for r in bucket.results) or "(none)"
        lines.append(f"  [{name}] ({bucket.size}) {pipelines}")
    return "\n".join(lines)
