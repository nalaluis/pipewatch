"""Tests for pipewatch.audit and pipewatch.audit_config."""

from __future__ import annotations

import json
import os
import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult
from pipewatch.alerts import Alert, AlertLevel
from pipewatch.audit import AuditEntry, record_audit, load_audit, clear_audit, _audit_path
from pipewatch.audit_config import AuditConfig, load_audit_config


def make_metric(pipeline="pipe-a", failure_rate=0.1, throughput=100.0):
    return PipelineMetric(
        pipeline=pipeline,
        success=90,
        failure=10,
        failure_rate=failure_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


def make_result(status=PipelineStatus.HEALTHY):
    return HealthResult(status=status, violations=[])


def make_alert(level=AlertLevel.WARNING):
    return Alert(pipeline="pipe-a", level=level, message="test alert")


@pytest.fixture(autouse=True)
def cleanup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    yield
    # cleanup handled by tmp_path


def test_record_audit_creates_file():
    metric = make_metric()
    result = make_result()
    entry = record_audit(metric, result)
    path = _audit_path(metric.pipeline)
    assert os.path.exists(path)
    assert isinstance(entry, AuditEntry)


def test_record_audit_entry_fields():
    metric = make_metric(failure_rate=0.25, throughput=50.0)
    result = make_result(status=PipelineStatus.WARNING)
    alerts = [make_alert(AlertLevel.WARNING)]
    entry = record_audit(metric, result, alerts=alerts, notes="test note")
    assert entry.pipeline == "pipe-a"
    assert entry.status == PipelineStatus.WARNING.value
    assert entry.failure_rate == 0.25
    assert entry.throughput == 50.0
    assert "warning" in entry.alert_levels
    assert entry.notes == "test note"


def test_load_audit_returns_all_entries():
    metric = make_metric()
    result = make_result()
    record_audit(metric, result)
    record_audit(metric, result)
    entries = load_audit(metric.pipeline)
    assert len(entries) == 2
    assert all(isinstance(e, AuditEntry) for e in entries)


def test_load_audit_returns_empty_for_missing():
    entries = load_audit("nonexistent-pipeline")
    assert entries == []


def test_clear_audit_removes_file():
    metric = make_metric()
    result = make_result()
    record_audit(metric, result)
    clear_audit(metric.pipeline)
    assert load_audit(metric.pipeline) == []


def test_audit_entry_appends_multiple():
    metric = make_metric()
    result1 = make_result(PipelineStatus.HEALTHY)
    result2 = make_result(PipelineStatus.CRITICAL)
    record_audit(metric, result1)
    record_audit(metric, result2)
    entries = load_audit(metric.pipeline)
    assert entries[0].status == PipelineStatus.HEALTHY.value
    assert entries[1].status == PipelineStatus.CRITICAL.value


def test_load_audit_config_defaults_when_missing():
    cfg = load_audit_config("no-such-file.yaml")
    assert cfg.enabled is True
    assert cfg.audit_dir == ".pipewatch/audit"
    assert cfg.max_entries_per_pipeline is None
    assert cfg.include_notes is False


def test_load_audit_config_parses_yaml(tmp_path):
    yaml_path = tmp_path / "pipewatch-audit.yaml"
    yaml_path.write_text(
        "audit:\n  enabled: false\n  max_entries_per_pipeline: 100\n  include_notes: true\n"
    )
    cfg = load_audit_config(str(yaml_path))
    assert cfg.enabled is False
    assert cfg.max_entries_per_pipeline == 100
    assert cfg.include_notes is True
