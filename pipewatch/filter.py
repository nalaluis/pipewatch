"""Filter utilities for selecting pipelines by status, name, or tag."""

from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Iterable, List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


@dataclass
class FilterCriteria:
    """Criteria used to select a subset of pipeline health results."""

    names: Optional[List[str]] = None          # glob patterns, e.g. ["etl_*"]
    statuses: Optional[List[PipelineStatus]] = None
    tags: Optional[List[str]] = None           # matched against result.pipeline_name prefix convention
    min_failure_rate: Optional[float] = None   # inclusive lower bound
    max_failure_rate: Optional[float] = None   # inclusive upper bound


def _matches_name(result: HealthResult, patterns: List[str]) -> bool:
    return any(fnmatch(result.pipeline_name, p) for p in patterns)


def _matches_status(result: HealthResult, statuses: List[PipelineStatus]) -> bool:
    return result.status in statuses


def _matches_tag(result: HealthResult, tags: List[str]) -> bool:
    """Treat the first colon-delimited segment of a pipeline name as its tag.

    Example: "payments:daily_load" has tag "payments".
    """
    name_tag = result.pipeline_name.split(":")[0] if ":" in result.pipeline_name else None
    return name_tag in tags


def apply_filter(
    results: Iterable[HealthResult],
    criteria: FilterCriteria,
) -> List[HealthResult]:
    """Return only the results that satisfy every non-None criterion."""
    out: List[HealthResult] = []
    for r in results:
        if criteria.names and not _matches_name(r, criteria.names):
            continue
        if criteria.statuses and not _matches_status(r, criteria.statuses):
            continue
        if criteria.tags and not _matches_tag(r, criteria.tags):
            continue
        if criteria.min_failure_rate is not None and r.failure_rate < criteria.min_failure_rate:
            continue
        if criteria.max_failure_rate is not None and r.failure_rate > criteria.max_failure_rate:
            continue
        out.append(r)
    return out


def filter_critical(results: Iterable[HealthResult]) -> List[HealthResult]:
    """Convenience: return only CRITICAL results."""
    return apply_filter(results, FilterCriteria(statuses=[PipelineStatus.CRITICAL]))


def filter_unhealthy(results: Iterable[HealthResult]) -> List[HealthResult]:
    """Convenience: return WARNING and CRITICAL results."""
    return apply_filter(
        results,
        FilterCriteria(statuses=[PipelineStatus.WARNING, PipelineStatus.CRITICAL]),
    )
