"""Tests for pipewatch.splitter_config."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipewatch.metrics import PipelineStatus
from pipewatch.splitter_config import load_splitter_rules


def _write(tmp_path: Path, data: dict) -> str:
    p = tmp_path / "pipewatch-splitter.yaml"
    p.write_text(yaml.dump(data))
    return str(p)


def test_load_missing_file_returns_empty(tmp_path):
    rules = load_splitter_rules(str(tmp_path / "missing.yaml"))
    assert rules == []


def test_load_empty_yaml_returns_empty(tmp_path):
    p = tmp_path / "s.yaml"
    p.write_text("")
    assert load_splitter_rules(str(p)) == []


def test_load_parses_bucket_name(tmp_path):
    path = _write(tmp_path, {"rules": [{"bucket": "critical", "status": "critical"}]})
    rules = load_splitter_rules(path)
    assert len(rules) == 1
    assert rules[0].bucket == "critical"


def test_load_parses_status(tmp_path):
    path = _write(tmp_path, {"rules": [{"bucket": "warn", "status": "warning"}]})
    rules = load_splitter_rules(path)
    assert rules[0].status == PipelineStatus.WARNING


def test_load_parses_pattern(tmp_path):
    path = _write(tmp_path, {"rules": [{"bucket": "etl", "pattern": "etl-*"}]})
    rules = load_splitter_rules(path)
    assert rules[0].pattern == "etl-*"
    assert rules[0].status is None


def test_load_parses_failure_rate_bounds(tmp_path):
    path = _write(
        tmp_path,
        {"rules": [{"bucket": "high", "min_failure_rate": 0.5, "max_failure_rate": 0.9}]},
    )
    rules = load_splitter_rules(path)
    assert rules[0].min_failure_rate == pytest.approx(0.5)
    assert rules[0].max_failure_rate == pytest.approx(0.9)


def test_load_multiple_rules_preserves_order(tmp_path):
    path = _write(
        tmp_path,
        {
            "rules": [
                {"bucket": "critical", "status": "critical"},
                {"bucket": "warning", "status": "warning"},
                {"bucket": "healthy", "status": "healthy"},
            ]
        },
    )
    rules = load_splitter_rules(path)
    assert [r.bucket for r in rules] == ["critical", "warning", "healthy"]


def test_unknown_status_defaults_to_healthy(tmp_path):
    path = _write(tmp_path, {"rules": [{"bucket": "x", "status": "unknown_value"}]})
    rules = load_splitter_rules(path)
    assert rules[0].status == PipelineStatus.HEALTHY
