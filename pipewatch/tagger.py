"""Tag inference for pipeline results based on metric patterns."""
from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


@dataclass
class TagRule:
    """A rule that assigns a tag when conditions are met."""
    tag: str
    name_pattern: Optional[str] = None
    status: Optional[str] = None
    min_failure_rate: Optional[float] = None
    max_throughput: Optional[float] = None


@dataclass
class TaggedResult:
    """A health result decorated with inferred tags."""
    result: HealthResult
    tags: List[str] = field(default_factory=list)

    @property
    def pipeline(self) -> str:
        return self.result.pipeline


def _rule_matches(rule: TagRule, result: HealthResult) -> bool:
    """Return True if the rule applies to the given result."""
    if rule.name_pattern and not fnmatch(result.pipeline, rule.name_pattern):
        return False
    if rule.status and result.metric.status.value != rule.status:
        return False
    if rule.min_failure_rate is not None:
        if result.metric.failure_rate < rule.min_failure_rate:
            return False
    if rule.max_throughput is not None:
        if result.metric.throughput > rule.max_throughput:
            return False
    return True


def apply_tags(result: HealthResult, rules: List[TagRule]) -> TaggedResult:
    """Apply all matching tag rules to a result."""
    tags = [rule.tag for rule in rules if _rule_matches(rule, result)]
    return TaggedResult(result=result, tags=tags)


def apply_tags_to_all(
    results: List[HealthResult], rules: List[TagRule]
) -> List[TaggedResult]:
    """Apply tag rules to a list of results."""
    return [apply_tags(r, rules) for r in results]
