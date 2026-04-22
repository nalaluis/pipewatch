"""Flusher: clears stale in-memory state across pipewatch modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List


@dataclass
class FlushTarget:
    """Describes a single flushable state store."""

    name: str
    flush_fn: Callable[[], None]


@dataclass
class FlushResult:
    """Summary of a flush operation."""

    flushed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return len(self.flushed)

    @property
    def error_count(self) -> int:
        return len(self.errors)

    def __str__(self) -> str:
        return (
            f"FlushResult(flushed={self.success_count}, errors={self.error_count})"
        )


_targets: List[FlushTarget] = []


def register_flush_target(name: str, flush_fn: Callable[[], None]) -> None:
    """Register a named state store for flushing."""
    _targets.append(FlushTarget(name=name, flush_fn=flush_fn))


def flush_all(targets: List[FlushTarget] | None = None) -> FlushResult:
    """Flush all registered (or provided) targets.

    Args:
        targets: Optional explicit list of targets; defaults to global registry.

    Returns:
        FlushResult summarising which targets were flushed and any errors.
    """
    result = FlushResult()
    work = targets if targets is not None else _targets
    for target in work:
        try:
            target.flush_fn()
            result.flushed.append(target.name)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"{target.name}: {exc}")
    return result


def clear_registry() -> None:
    """Remove all registered flush targets (useful in tests)."""
    _targets.clear()


def registered_names() -> List[str]:
    """Return names of all currently registered flush targets."""
    return [t.name for t in _targets]
