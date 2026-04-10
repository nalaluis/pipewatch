"""Tests for pipewatch.anomaly and pipewatch.anomaly_config."""

from __future__ import annotations

import os
import textwrap
import tempfile
from datetime import datetime
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.history import HistoryEntry
from pipewatch.health import HealthResult
from pipewatch.anomaly import (
    AnomalyConfig,
    AnomalyResult,
    detect_anomaly,
    format_anomalies,
)
from pipewatch.anomaly_config import load_anomaly_config


def make_metric(pipeline: str = "pipe", failure_rate: float = 0.0, throughput: float = 100.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        failure_rate=failure_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime(2024, 1, 1),
    )


def make_entry(failure_rate: float = 0.0, throughput: float = 100.0) -> HistoryEntry:
    metric = make_metric(failure_rate=failure_rate, throughput=throughput)
    result = HealthResult(pipeline="pipe", status=PipelineStatus.HEALTHY, violations=[])
    return HistoryEntry(metric=metric, result=result, timestamp=datetime(2024, 1, 1))


# --- detect_anomaly ---

def test_detect_anomaly_returns_empty_without_enough_history():
    current = make_metric(failure_rate=0.9)
    history = [make_entry() for _ in range(2)]  # below min_history=3
    results = detect_anomaly(current, history)
    assert results == []


def test_detect_anomaly_no_anomaly_for_stable_metrics():
    current = make_metric(failure_rate=0.05, throughput=100.0)
    history = [make_entry(failure_rate=0.05, throughput=100.0) for _ in range(5)]
    results = detect_anomaly(current, history)
    assert all(not r.is_anomaly for r in results)


def test_detect_anomaly_flags_failure_rate_spike():
    history = [make_entry(failure_rate=0.01) for _ in range(10)]
    current = make_metric(failure_rate=0.99)  # extreme spike
    results = detect_anomaly(current, history)
    fr_result = next(r for r in results if r.metric_name == "failure_rate")
    assert fr_result.is_anomaly


def test_detect_anomaly_flags_throughput_drop():
    history = [make_entry(throughput=1000.0) for _ in range(10)]
    current = make_metric(throughput=1.0)  # extreme drop
    results = detect_anomaly(current, history)
    tp_result = next(r for r in results if r.metric_name == "throughput")
    assert tp_result.is_anomaly


def test_detect_anomaly_returns_two_results_per_call():
    history = [make_entry() for _ in range(5)]
    current = make_metric()
    results = detect_anomaly(current, history)
    assert len(results) == 2
    names = {r.metric_name for r in results}
    assert names == {"failure_rate", "throughput"}


def test_detect_anomaly_zero_std_dev_no_anomaly():
    history = [make_entry(failure_rate=0.1, throughput=50.0) for _ in range(5)]
    current = make_metric(failure_rate=0.1, throughput=50.0)
    results = detect_anomaly(current, history)
    assert all(not r.is_anomaly for r in results)


def test_detect_anomaly_custom_config_lower_threshold():
    cfg = AnomalyConfig(z_score_threshold=0.5, min_history=2)
    history = [make_entry(failure_rate=0.0), make_entry(failure_rate=0.0), make_entry(failure_rate=0.0)]
    current = make_metric(failure_rate=0.5)
    results = detect_anomaly(current, history, config=cfg)
    fr = next(r for r in results if r.metric_name == "failure_rate")
    assert fr.is_anomaly


# --- format_anomalies ---

def test_format_anomalies_empty():
    assert format_anomalies([]) == "No anomalies detected."


def test_format_anomalies_contains_pipeline_name():
    history = [make_entry(failure_rate=0.01) for _ in range(10)]
    current = make_metric(pipeline="critical-pipe", failure_rate=0.99)
    results = detect_anomaly(current, history)
    output = format_anomalies(results)
    assert "critical-pipe" in output


# --- load_anomaly_config ---

def test_load_anomaly_config_missing_file_returns_defaults():
    cfg = load_anomaly_config(path="/nonexistent/path.yaml")
    assert cfg.z_score_threshold == 2.5
    assert cfg.min_history == 3


def test_load_anomaly_config_parses_yaml():
    content = textwrap.dedent("""
        anomaly:
          z_score_threshold: 1.8
          min_history: 5
    """)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(content)
        tmp = f.name
    try:
        cfg = load_anomaly_config(path=tmp)
        assert cfg.z_score_threshold == 1.8
        assert cfg.min_history == 5
    finally:
        os.unlink(tmp)


def test_load_anomaly_config_empty_yaml_returns_defaults():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("")
        tmp = f.name
    try:
        cfg = load_anomaly_config(path=tmp)
        assert cfg.z_score_threshold == 2.5
    finally:
        os.unlink(tmp)
