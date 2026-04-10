"""Tests for pipewatch.dependency."""
import pytest

from pipewatch.dependency import (
    DependencyGraph,
    BlockedPipeline,
    build_graph,
    find_blocked,
    topological_order,
)
from pipewatch.health import HealthResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_result(pipeline: str, healthy: bool, status: str = "critical") -> HealthResult:
    return HealthResult(
        pipeline=pipeline,
        healthy=healthy,
        status=status if not healthy else "ok",
        violations=[],
    )


# ---------------------------------------------------------------------------
# DependencyGraph
# ---------------------------------------------------------------------------

def test_add_dependency_stores_edge():
    g = DependencyGraph()
    g.add_dependency("b", "a")
    assert "a" in g.dependencies_of("b")


def test_add_dependency_no_duplicates():
    g = DependencyGraph()
    g.add_dependency("b", "a")
    g.add_dependency("b", "a")
    assert g.dependencies_of("b").count("a") == 1


def test_all_pipelines_includes_both_sides():
    g = DependencyGraph()
    g.add_dependency("c", "a")
    g.add_dependency("c", "b")
    assert g.all_pipelines() == {"a", "b", "c"}


def test_dependencies_of_unknown_pipeline_returns_empty():
    g = DependencyGraph()
    assert g.dependencies_of("unknown") == []


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

def test_build_graph_from_config():
    config = {
        "dependencies": {
            "pipeline_b": ["pipeline_a"],
            "pipeline_c": ["pipeline_a", "pipeline_b"],
        }
    }
    g = build_graph(config)
    assert "pipeline_a" in g.dependencies_of("pipeline_b")
    assert "pipeline_b" in g.dependencies_of("pipeline_c")


def test_build_graph_empty_config():
    g = build_graph({})
    assert g.all_pipelines() == set()


# ---------------------------------------------------------------------------
# find_blocked
# ---------------------------------------------------------------------------

def test_find_blocked_returns_empty_when_all_healthy():
    g = DependencyGraph()
    g.add_dependency("b", "a")
    results = [make_result("a", healthy=True), make_result("b", healthy=True)]
    assert find_blocked(g, results) == []


def test_find_blocked_detects_unhealthy_upstream():
    g = DependencyGraph()
    g.add_dependency("b", "a")
    results = [make_result("a", healthy=False), make_result("b", healthy=True)]
    blocked = find_blocked(g, results)
    assert len(blocked) == 1
    assert blocked[0].pipeline == "b"
    assert blocked[0].blocked_by == "a"


def test_find_blocked_str_representation():
    bp = BlockedPipeline(pipeline="b", blocked_by="a", reason="upstream 'a' status=critical")
    assert "blocked by" in str(bp)


def test_find_blocked_missing_dep_in_results_is_ignored():
    g = DependencyGraph()
    g.add_dependency("b", "a")
    # 'a' not in results at all
    results = [make_result("b", healthy=True)]
    assert find_blocked(g, results) == []


# ---------------------------------------------------------------------------
# topological_order
# ---------------------------------------------------------------------------

def test_topological_order_simple_chain():
    g = DependencyGraph()
    g.add_dependency("b", "a")
    g.add_dependency("c", "b")
    order = topological_order(g)
    assert order is not None
    assert order.index("a") < order.index("b") < order.index("c")


def test_topological_order_no_dependencies():
    g = DependencyGraph()
    g.add_dependency("b", "a")
    order = topological_order(g)
    assert order is not None
    assert set(order) == {"a", "b"}
