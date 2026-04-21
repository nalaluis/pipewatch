"""Group pipeline health results by a shared key (tag, status, or label)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


@dataclass
class PipelineGroup:
    key: str
    results: List[HealthResult] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.results)

    @property
    def critical_count(self) -> int:
        return sum(1 for r in self.results if r.metric.status == PipelineStatus.CRITICAL)

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if r.metric.status == PipelineStatus.WARNING)

    @property
    def healthy_count(self) -> int:
        return sum(1 for r in self.results if r.metric.status == PipelineStatus.OK)

    @property
    def worst_status(self) -> PipelineStatus:
        """Return the most severe status present in the group."""
        if self.critical_count > 0:
            return PipelineStatus.CRITICAL
        if self.warning_count > 0:
            return PipelineStatus.WARNING
        return PipelineStatus.OK


def group_by_status(results: List[HealthResult]) -> Dict[str, PipelineGroup]:
    """Group results by pipeline status string."""
    groups: Dict[str, PipelineGroup] = {}
    for result in results:
        key = result.metric.status.value
        groups.setdefault(key, PipelineGroup(key=key))
        groups[key].results.append(result)
    return groups


def group_by_tag(results: List[HealthResult], tag_key: str) -> Dict[str, PipelineGroup]:
    """Group results by a specific metadata tag value."""
    groups: Dict[str, PipelineGroup] = {}
    untagged_key = "__untagged__"
    for result in results:
        tags = getattr(result.metric, "tags", {}) or {}
        key = tags.get(tag_key, untagged_key)
        groups.setdefault(key, PipelineGroup(key=key))
        groups[key].results.append(result)
    return groups


def group_by(results: List[HealthResult], key_fn: Callable[[HealthResult], str]) -> Dict[str, PipelineGroup]:
    """Generic grouping by an arbitrary key function."""
    groups: Dict[str, PipelineGroup] = {}
    for result in results:
        key = key_fn(result)
        groups.setdefault(key, PipelineGroup(key=key))
        groups[key].results.append(result)
    return groups


def format_groups(groups: Dict[str, PipelineGroup]) -> str:
    """Render a human-readable summary of pipeline groups."""
    lines = []
    for key, group in sorted(groups.items()):
        lines.append(
            f"[{key}] total={group.size} "
            f"healthy={group.healthy_count} "
            f"warning={group.warning_count} "
            f"critical={group.critical_count}"
        )
    return "\n".join(lines) if lines else "(no groups)"
