"""Pipeline execution profiler: tracks and reports timing stats per pipeline."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ProfileEntry:
    pipeline: str
    duration_seconds: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class ProfileStats:
    pipeline: str
    count: int
    avg_duration: float
    min_duration: float
    max_duration: float
    last_duration: float

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: avg={self.avg_duration:.3f}s "
            f"min={self.min_duration:.3f}s max={self.max_duration:.3f}s "
            f"(n={self.count})"
        )


# Module-level store keyed by pipeline name
_store: Dict[str, List[ProfileEntry]] = {}


def record_profile(pipeline: str, duration_seconds: float) -> ProfileEntry:
    """Record a timing observation for a pipeline."""
    entry = ProfileEntry(pipeline=pipeline, duration_seconds=duration_seconds)
    _store.setdefault(pipeline, []).append(entry)
    return entry


def get_stats(pipeline: str) -> Optional[ProfileStats]:
    """Return aggregated timing stats for a pipeline, or None if no data."""
    entries = _store.get(pipeline, [])
    if not entries:
        return None
    durations = [e.duration_seconds for e in entries]
    return ProfileStats(
        pipeline=pipeline,
        count=len(durations),
        avg_duration=sum(durations) / len(durations),
        min_duration=min(durations),
        max_duration=max(durations),
        last_duration=durations[-1],
    )


def all_stats() -> List[ProfileStats]:
    """Return stats for every pipeline that has been profiled."""
    return [s for name in _store if (s := get_stats(name)) is not None]


def clear_profiles(pipeline: Optional[str] = None) -> None:
    """Clear stored profiles for a specific pipeline or all pipelines."""
    if pipeline is not None:
        _store.pop(pipeline, None)
    else:
        _store.clear()


def format_profile_report(stats: List[ProfileStats]) -> str:
    """Render a human-readable profile report."""
    if not stats:
        return "No profiling data available."
    lines = ["Pipeline Profiling Report", "-" * 40]
    for s in sorted(stats, key=lambda x: x.avg_duration, reverse=True):
        lines.append(str(s))
    return "\n".join(lines)
