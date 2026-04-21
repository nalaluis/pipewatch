"""Tests for pipewatch.collector and pipewatch.collector_config."""
from __future__ import annotations

import os
import textwrap
from typing import Optional

import pytest

from pipewatch.collector import (
    CollectionResult,
    CollectorConfig,
    collect_metrics,
    filter_by_status,
)
from pipewatch.collector_config import load_collector_config
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(name: str, status: PipelineStatus = PipelineStatus.HEALTHY) -> PipelineMetric:
    return PipelineMetric(
        pipeline=name,
        status=status,
        failure_rate=0.0,
        throughput=100.0,
    )


# ---------------------------------------------------------------------------
# CollectionResult helpers
# ---------------------------------------------------------------------------

def test_collection_result_total_counts_collected():
    r = CollectionResult(collected=[make_metric("a"), make_metric("b")])
    assert r.total == 2


def test_collection_result_success_rate_all_ok():
    r = CollectionResult(collected=[make_metric("a")])
    assert r.success_rate == 1.0


def test_collection_result_success_rate_with_errors():
    r = CollectionResult(collected=[make_metric("a")], errors=["b: oops"])
    assert r.success_rate == pytest.approx(0.5)


def test_collection_result_str_format():
    r = CollectionResult(collected=[make_metric("a")], skipped=["b"], errors=["c: err"])
    assert "collected=1" in str(r)
    assert "skipped=1" in str(r)
    assert "errors=1" in str(r)


# ---------------------------------------------------------------------------
# collect_metrics
# ---------------------------------------------------------------------------

def test_collect_metrics_happy_path():
    def fetch(name: str) -> Optional[PipelineMetric]:
        return make_metric(name)

    result = collect_metrics(["pipe_a", "pipe_b"], fetch)
    assert result.total == 2
    assert result.errors == []


def test_collect_metrics_none_fetch_with_skip_unknown():
    cfg = CollectorConfig(skip_unknown=True)
    result = collect_metrics(["missing"], lambda _: None, config=cfg)
    assert result.total == 0
    assert "missing" in result.skipped


def test_collect_metrics_none_fetch_without_skip_records_error():
    cfg = CollectorConfig(skip_unknown=False)
    result = collect_metrics(["missing"], lambda _: None, config=cfg)
    assert result.total == 0
    assert any("missing" in e for e in result.errors)


def test_collect_metrics_exception_recorded_as_error():
    def boom(_: str) -> PipelineMetric:
        raise RuntimeError("network error")

    result = collect_metrics(["pipe_x"], boom)
    assert result.total == 0
    assert any("network error" in e for e in result.errors)


def test_collect_metrics_respects_max_batch_size():
    cfg = CollectorConfig(max_batch_size=2)
    names = ["a", "b", "c", "d"]
    result = collect_metrics(names, make_metric, config=cfg)
    assert result.total == 2


# ---------------------------------------------------------------------------
# filter_by_status
# ---------------------------------------------------------------------------

def test_filter_by_status_returns_matching():
    metrics = [
        make_metric("a", PipelineStatus.HEALTHY),
        make_metric("b", PipelineStatus.CRITICAL),
        make_metric("c", PipelineStatus.WARNING),
    ]
    filtered = filter_by_status(metrics, [PipelineStatus.CRITICAL])
    assert len(filtered) == 1
    assert filtered[0].pipeline == "b"


def test_filter_by_status_empty_list_returns_empty():
    assert filter_by_status([], [PipelineStatus.HEALTHY]) == []


# ---------------------------------------------------------------------------
# load_collector_config
# ---------------------------------------------------------------------------

def test_load_collector_config_missing_file_returns_defaults(tmp_path):
    cfg = load_collector_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.max_batch_size == 50
    assert cfg.skip_unknown is False
    assert cfg.default_status == PipelineStatus.HEALTHY


def test_load_collector_config_parses_values(tmp_path):
    p = tmp_path / "collector.yaml"
    p.write_text(textwrap.dedent("""\
        collector:
          max_batch_size: 10
          skip_unknown: true
          default_status: warning
    """))
    cfg = load_collector_config(str(p))
    assert cfg.max_batch_size == 10
    assert cfg.skip_unknown is True
    assert cfg.default_status == PipelineStatus.WARNING


def test_load_collector_config_empty_yaml_returns_defaults(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("")
    cfg = load_collector_config(str(p))
    assert cfg.max_batch_size == 50
