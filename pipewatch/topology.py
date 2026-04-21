"""Pipeline topology analysis — detects cycles, ranks pipelines by criticality,
and surfaces upstream/downstream impact chains from the dependency graph."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.dependency import DependencyGraph


@dataclass
class TopologyNode:
    """A single node in the resolved pipeline topology."""

    pipeline: str
    depth: int  # distance from a root (no upstream dependencies)
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"TopologyNode({self.pipeline!r}, depth={self.depth}, "
            f"upstream={self.upstream}, downstream={self.downstream})"
        )


@dataclass
class TopologyReport:
    """Full topology derived from a DependencyGraph."""

    nodes: Dict[str, TopologyNode] = field(default_factory=dict)
    cycles: List[List[str]] = field(default_factory=list)

    @property
    def has_cycles(self) -> bool:
        return bool(self.cycles)

    def roots(self) -> List[str]:
        """Pipelines with no upstream dependencies."""
        return [name for name, node in self.nodes.items() if not node.upstream]

    def leaves(self) -> List[str]:
        """Pipelines with no downstream dependents."""
        return [name for name, node in self.nodes.items() if not node.downstream]

    def impact_chain(self, pipeline: str) -> List[str]:
        """Return all pipelines transitively downstream of *pipeline*."""
        visited: Set[str] = set()
        stack = list(self.nodes.get(pipeline, TopologyNode(pipeline, 0)).downstream)
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            node = self.nodes.get(current)
            if node:
                stack.extend(node.downstream)
        return sorted(visited)


def build_topology(graph: DependencyGraph) -> TopologyReport:
    """Analyse *graph* and return a :class:`TopologyReport`.

    Depth is computed via BFS from root nodes.  Cycle detection uses
    DFS with a recursion-stack colour map (white / grey / black).
    """
    all_names = list(graph.all_pipelines())

    # Build adjacency maps
    downstream_map: Dict[str, List[str]] = {n: [] for n in all_names}
    upstream_map: Dict[str, List[str]] = {n: [] for n in all_names}
    for name in all_names:
        for dep in graph.dependencies_of(name):
            upstream_map[name].append(dep)
            downstream_map[dep].append(name)

    # Detect cycles via iterative DFS
    cycles: List[List[str]] = []
    WHITE, GREY, BLACK = 0, 1, 2
    colour: Dict[str, int] = {n: WHITE for n in all_names}
    parent: Dict[str, Optional[str]] = {n: None for n in all_names}

    def _dfs(start: str) -> None:
        stack = [(start, iter(downstream_map[start]))]
        colour[start] = GREY
        path = [start]
        while stack:
            node, children = stack[-1]
            try:
                child = next(children)
                if colour[child] == GREY:
                    # Found a back-edge — extract the cycle
                    idx = path.index(child)
                    cycles.append(path[idx:] + [child])
                elif colour[child] == WHITE:
                    colour[child] = GREY
                    parent[child] = node
                    path.append(child)
                    stack.append((child, iter(downstream_map[child])))
            except StopIteration:
                colour[node] = BLACK
                stack.pop()
                if path and path[-1] == node:
                    path.pop()

    for name in all_names:
        if colour[name] == WHITE:
            _dfs(name)

    # BFS from roots to assign depths
    depth_map: Dict[str, int] = {}
    roots = [n for n in all_names if not upstream_map[n]]
    queue = [(r, 0) for r in roots]
    visited_bfs: Set[str] = set(roots)
    while queue:
        current, d = queue.pop(0)
        depth_map[current] = d
        for child in downstream_map[current]:
            if child not in visited_bfs:
                visited_bfs.add(child)
                queue.append((child, d + 1))
    # Nodes not reachable from roots (e.g. inside cycles) get depth -1
    for name in all_names:
        depth_map.setdefault(name, -1)

    nodes = {
        name: TopologyNode(
            pipeline=name,
            depth=depth_map[name],
            upstream=upstream_map[name],
            downstream=downstream_map[name],
        )
        for name in all_names
    }

    return TopologyReport(nodes=nodes, cycles=cycles)


def format_topology(report: TopologyReport) -> str:
    """Return a human-readable summary of the topology report."""
    lines: List[str] = ["Pipeline Topology"]
    lines.append("=" * 40)
    if report.has_cycles:
        lines.append(f"WARNING: {len(report.cycles)} cycle(s) detected")
        for cycle in report.cycles:
            lines.append("  cycle: " + " -> ".join(cycle))
        lines.append("")

    sorted_nodes = sorted(report.nodes.values(), key=lambda n: (n.depth, n.pipeline))
    for node in sorted_nodes:
        depth_prefix = "  " * max(node.depth, 0)
        lines.append(f"{depth_prefix}[depth={node.depth}] {node.pipeline}")
        if node.upstream:
            lines.append(f"{depth_prefix}  upstream : {', '.join(node.upstream)}")
        if node.downstream:
            lines.append(f"{depth_prefix}  downstream: {', '.join(node.downstream)}")

    lines.append("")
    lines.append(f"Roots : {', '.join(report.roots()) or 'none'}")
    lines.append(f"Leaves: {', '.join(report.leaves()) or 'none'}")
    return "\n".join(lines)
