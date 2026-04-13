"""Eviction policy for removing stale pipeline state entries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List
import time


@dataclass
class EvictionConfig:
    max_age_seconds: float = 3600.0  # 1 hour default
    max_entries: int = 500
    enabled: bool = True


@dataclass
class EvictionResult:
    evicted: List[str] = field(default_factory=list)
    kept: List[str] = field(default_factory=list)

    @property
    def evicted_count(self) -> int:
        return len(self.evicted)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    def __str__(self) -> str:
        return f"EvictionResult(evicted={self.evicted_count}, kept={self.kept_count})"


def evict_by_age(
    state: Dict[str, float],
    config: EvictionConfig,
    now: float | None = None,
) -> EvictionResult:
    """Remove entries older than max_age_seconds. Mutates state in place."""
    if not config.enabled:
        return EvictionResult(kept=list(state.keys()))

    now = now if now is not None else time.time()
    result = EvictionResult()
    to_remove = []

    for key, ts in state.items():
        age = now - ts
        if age > config.max_age_seconds:
            to_remove.append(key)
        else:
            result.kept.append(key)

    for key in to_remove:
        del state[key]
        result.evicted.append(key)

    return result


def evict_by_count(
    state: Dict[str, float],
    config: EvictionConfig,
) -> EvictionResult:
    """Remove oldest entries if count exceeds max_entries. Mutates state in place."""
    if not config.enabled or len(state) <= config.max_entries:
        return EvictionResult(kept=list(state.keys()))

    sorted_keys = sorted(state.keys(), key=lambda k: state[k])
    overflow = len(state) - config.max_entries
    to_remove = sorted_keys[:overflow]

    result = EvictionResult()
    for key in to_remove:
        del state[key]
        result.evicted.append(key)
    result.kept = list(state.keys())
    return result


def apply_eviction(
    state: Dict[str, float],
    config: EvictionConfig,
    now: float | None = None,
) -> EvictionResult:
    """Apply both age-based and count-based eviction in sequence."""
    age_result = evict_by_age(state, config, now=now)
    count_result = evict_by_count(state, config)
    return EvictionResult(
        evicted=age_result.evicted + count_result.evicted,
        kept=count_result.kept,
    )
