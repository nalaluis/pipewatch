"""Tests for pipewatch.replay and pipewatch.replay_config."""

from __future__ import annotations

import pytest
from unittest.mock import patch

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthThresholds
from pipewatch.alerts import AlertLevel
from pipewatch.replay import (
    ReplayFrame,
    replay_pipeline,
    replay_all,
    format_replay,
)
from pipewatch.replay_config import ReplayConfig, load_replay_config


def make_metric(
    name: str = "pipe",
    failure_rate: float = 0.0,
    throughput: float = 100.0,
    status: PipelineStatus = PipelineStatus.OK,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=name,
        failure_rate=failure_rate,
        throughput=throughput,
        status=status,
    )


# ---------------------------------------------------------------------------
# replay_pipeline
# ---------------------------------------------------------------------------

def test_replay_pipeline_no_snapshot_returns_empty():
    with patch("pipewatch.replay.list_snapshots", return_value=[]):
        frames = replay_pipeline("missing")
    assert frames == []


def test_replay_pipeline_returns_one_frame_per_snapshot():
    metric = make_metric("etl", failure_rate=0.01, throughput=200.0)
    with (
        patch("pipewatch.replay.list_snapshots", return_value=["etl"]),
        patch("pipewatch.replay.load_snapshot", return_value=metric),
    ):
        frames = replay_pipeline("etl")
    assert len(frames) == 1
    assert frames[0].pipeline == "etl"


def test_replay_pipeline_healthy_metric_produces_healthy_frame():
    metric = make_metric("etl", failure_rate=0.0, throughput=500.0)
    thresholds = HealthThresholds(max_failure_rate=0.1, min_throughput=10.0)
    with (
        patch("pipewatch.replay.list_snapshots", return_value=["etl"]),
        patch("pipewatch.replay.load_snapshot", return_value=metric),
    ):
        frames = replay_pipeline("etl", thresholds=thresholds)
    assert frames[0].is_healthy is True
    assert frames[0].critical_count == 0


def test_replay_pipeline_critical_metric_produces_unhealthy_frame():
    metric = make_metric("etl", failure_rate=0.9, throughput=1.0)
    thresholds = HealthThresholds(max_failure_rate=0.1, min_throughput=10.0)
    with (
        patch("pipewatch.replay.list_snapshots", return_value=["etl"]),
        patch("pipewatch.replay.load_snapshot", return_value=metric),
    ):
        frames = replay_pipeline("etl", thresholds=thresholds)
    assert frames[0].is_healthy is False


# ---------------------------------------------------------------------------
# replay_all
# ---------------------------------------------------------------------------

def test_replay_all_aggregates_all_pipelines():
    m1 = make_metric("a")
    m2 = make_metric("b")

    def _load(name):
        return m1 if name == "a" else m2

    with (
        patch("pipewatch.replay.list_snapshots", return_value=["a", "b"]),
        patch("pipewatch.replay.load_snapshot", side_effect=_load),
    ):
        frames = replay_all()
    assert len(frames) == 2
    names = {f.pipeline for f in frames}
    assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# format_replay
# ---------------------------------------------------------------------------

def test_format_replay_empty_returns_message():
    assert format_replay([]) == "No replay data available."


def test_format_replay_includes_pipeline_name():
    metric = make_metric("etl")
    frame = ReplayFrame(
        pipeline="etl", metric=metric, is_healthy=True, alert_count=0, critical_count=0
    )
    output = format_replay([frame])
    assert "etl" in output
    assert "OK" in output


# ---------------------------------------------------------------------------
# load_replay_config
# ---------------------------------------------------------------------------

def test_load_replay_config_missing_file_returns_defaults(tmp_path):
    cfg = load_replay_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.pipelines == []
    assert cfg.min_level == "WARNING"
    assert cfg.include_healthy is True


def test_load_replay_config_parses_yaml(tmp_path):
    cfg_file = tmp_path / "replay.yaml"
    cfg_file.write_text(
        "replay:\n  pipelines:\n    - etl\n    - ingest\n  min_level: CRITICAL\n  include_healthy: false\n"
    )
    cfg = load_replay_config(str(cfg_file))
    assert cfg.pipelines == ["etl", "ingest"]
    assert cfg.min_level == "CRITICAL"
    assert cfg.include_healthy is False
