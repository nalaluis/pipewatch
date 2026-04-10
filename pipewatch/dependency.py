"""Pipeline dependency tracking and ordering."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.health import HealthResult


@dataclass
class DependencyGraph:
    """Directed graph of pipeline dependencies."""
    edges: Dict[str, List[str]] = field(default_factory=dict)  # pipeline -> depends_on

    def add_dependency(self, pipeline: str, depends_on: str) -> None:
        """Register that *pipeline* depends on *depends_on*."""
        self.edges.setdefault(pipeline, [])
        if depends_on not in self.edges[pipeline]:
            self.edges[pipeline].append(depends_on)

    def dependencies_of(self, pipeline: str) -> List[str]:
        """Return direct dependencies of *pipeline*."""
        return list(self.edges.get(pipeline, []))

    def all_pipelines(self) -> Set[str]:
        """Return every pipeline name mentioned in the graph."""
        names: Set[str] = set(self.edges.keys())
        for deps in self.edges.values():
            names.update(deps)
        return names


@dataclass
class BlockedPipeline:
    """A pipeline that is blocked due to an unhealthy upstream dependency."""
    pipeline: str
    blocked_by: str
    reason: str

    def __str__(self) -> str:
        return f"{self.pipeline} blocked by {self.blocked_by}: {self.reason}"


def build_graph(config: Dict) -> DependencyGraph:
    """Build a DependencyGraph from a parsed YAML config dict.

    Expected shape::

        dependencies:
          pipeline_b:
            - pipeline_a
          pipeline_c:
            - pipeline_a
            - pipeline_b
    """
    graph = DependencyGraph()
    for pipeline, deps in config.get("dependencies", {}).items():
        for dep in deps or []:
            graph.add_dependency(pipeline, dep)
    return graph


def find_blocked(
    graph: DependencyGraph,
    results: List[HealthResult],
) -> List[BlockedPipeline]:
    """Return pipelines that are blocked because at least one dependency is not healthy."""
    health_map: Dict[str, HealthResult] = {r.pipeline: r for r in results}
    blocked: List[BlockedPipeline] = []

    for pipeline, deps in graph.edges.items():
        for dep in deps:
            dep_result = health_map.get(dep)
            if dep_result is not None and not dep_result.healthy:
                blocked.append(
                    BlockedPipeline(
                        pipeline=pipeline,
                        blocked_by=dep,
                        reason=f"upstream '{dep}' status={dep_result.status}",
                    )
                )
                break  # one blocked dep is enough to mark the pipeline

    return blocked


def topological_order(graph: DependencyGraph) -> Optional[List[str]]:
    """Return pipelines in topological order, or None if a cycle is detected."""
    in_degree: Dict[str, int] = {p: 0 for p in graph.all_pipelines()}
    for pipeline, deps in graph.edges.items():
        for dep in deps:
            in_degree[pipeline] = in_degree.get(pipeline, 0)
            in_degree[dep] = in_degree.get(dep, 0)

    # recalculate using adjacency
    in_deg: Dict[str, int] = {p: 0 for p in graph.all_pipelines()}
    for pipeline, deps in graph.edges.items():
        for _dep in deps:
            in_deg[pipeline] += 1

    queue = [p for p, d in in_deg.items() if d == 0]
    order: List[str] = []

    while queue:
        node = queue.pop(0)
        order.append(node)
        for pipeline, deps in graph.edges.items():
            if node in deps:
                in_deg[pipeline] -= 1
                if in_deg[pipeline] == 0:
                    queue.append(pipeline)

    return order if len(order) == len(graph.all_pipelines()) else None
