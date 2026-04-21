"""cascade.py — Detect and report cascading failure propagation across pipelines.

A cascade occurs when a failing pipeline causes downstream pipelines to
also fail, amplifying the blast radius of an incident. This module
identifies cascade chains using the dependency graph and alert state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.dependency import DependencyGraph


@dataclass
class CascadeNode:
    """A single pipeline in a cascade chain."""

    pipeline: str
    level: AlertLevel
    depth: int  # 0 = root cause, 1 = first downstream, etc.

    def __str__(self) -> str:
        indent = "  " * self.depth
        arrow = "->" if self.depth > 0 else "!!"
        return f"{indent}{arrow} {self.pipeline} [{self.level.value}]"


@dataclass
class CascadeChain:
    """A chain of pipelines affected by a cascading failure."""

    root: str
    nodes: List[CascadeNode] = field(default_factory=list)

    @property
    def depth(self) -> int:
        """Maximum propagation depth of the cascade."""
        return max((n.depth for n in self.nodes), default=0)

    @property
    def affected_pipelines(self) -> List[str]:
        """All pipeline names involved in this cascade."""
        return [n.pipeline for n in self.nodes]

    @property
    def is_critical(self) -> bool:
        """True if any node in the chain has a CRITICAL alert level."""
        return any(n.level == AlertLevel.CRITICAL for n in self.nodes)

    def __str__(self) -> str:
        lines = [f"Cascade from '{self.root}' (depth={self.depth})"]
        lines.extend(str(n) for n in self.nodes)
        return "\n".join(lines)


@dataclass
class CascadeReport:
    """Summary of all detected cascade chains in the current run."""

    chains: List[CascadeChain] = field(default_factory=list)

    @property
    def has_cascades(self) -> bool:
        return len(self.chains) > 0

    @property
    def total_affected(self) -> int:
        """Total unique pipelines affected across all chains."""
        seen: Set[str] = set()
        for chain in self.chains:
            seen.update(chain.affected_pipelines)
        return len(seen)

    def __str__(self) -> str:
        if not self.chains:
            return "No cascade failures detected."
        parts = [f"Cascades detected: {len(self.chains)} chain(s), "
                 f"{self.total_affected} pipeline(s) affected"]
        for chain in self.chains:
            parts.append(str(chain))
        return "\n".join(parts)


def _alert_index(alerts: List[Alert]) -> Dict[str, AlertLevel]:
    """Build a mapping of pipeline -> highest AlertLevel from alert list."""
    index: Dict[str, AlertLevel] = {}
    _order = {AlertLevel.OK: 0, AlertLevel.WARNING: 1, AlertLevel.CRITICAL: 2}
    for alert in alerts:
        current = index.get(alert.pipeline)
        if current is None or _order[alert.level] > _order[current]:
            index[alert.pipeline] = alert.level
    return index


def _walk_cascade(
    pipeline: str,
    graph: DependencyGraph,
    alert_index: Dict[str, AlertLevel],
    depth: int,
    visited: Set[str],
    nodes: List[CascadeNode],
) -> None:
    """Recursively walk downstream dependents to build a cascade chain."""
    if pipeline in visited:
        return
    visited.add(pipeline)

    level = alert_index.get(pipeline, AlertLevel.OK)
    nodes.append(CascadeNode(pipeline=pipeline, level=level, depth=depth))

    for downstream in graph.dependents_of(pipeline):
        if alert_index.get(downstream, AlertLevel.OK) != AlertLevel.OK:
            _walk_cascade(downstream, graph, alert_index, depth + 1, visited, nodes)


def detect_cascades(
    alerts: List[Alert],
    graph: DependencyGraph,
    min_depth: int = 1,
) -> CascadeReport:
    """Detect cascading failures propagating through the dependency graph.

    Args:
        alerts: Current active alerts for all pipelines.
        graph: Dependency graph describing pipeline relationships.
        min_depth: Minimum cascade depth required to include a chain.

    Returns:
        A CascadeReport containing all chains meeting the depth threshold.
    """
    alert_index = _alert_index(alerts)
    # Only consider pipelines with no upstream dependencies as potential roots
    roots = [
        p for p in graph.roots()
        if alert_index.get(p, AlertLevel.OK) != AlertLevel.OK
    ]

    chains: List[CascadeChain] = []
    for root in roots:
        nodes: List[CascadeNode] = []
        _walk_cascade(root, graph, alert_index, 0, set(), nodes)
        if len(nodes) > 1:  # at least root + one downstream
            chain = CascadeChain(root=root, nodes=nodes)
            if chain.depth >= min_depth:
                chains.append(chain)

    return CascadeReport(chains=chains)
