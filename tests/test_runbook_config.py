"""Tests for pipewatch.runbook_config."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipewatch.runbook_config import load_runbook_config


def _write_yaml(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "runbooks.yaml"
    p.write_text(yaml.dump(data))
    return p


def test_load_missing_file_returns_empty(tmp_path):
    result = load_runbook_config(tmp_path / "nonexistent.yaml")
    assert result == {}


def test_load_empty_yaml_returns_empty(tmp_path):
    p = tmp_path / "runbooks.yaml"
    p.write_text("")
    assert load_runbook_config(p) == {}


def test_load_parses_single_entry(tmp_path):
    data = {
        "runbooks": {
            "failure_rate": {
                "title": "High Failure Rate",
                "steps": ["check logs", "restart"],
                "reference": "http://example.com",
            }
        }
    }
    p = _write_yaml(tmp_path, data)
    result = load_runbook_config(p)
    assert "failure_rate" in result
    entry = result["failure_rate"]
    assert entry.title == "High Failure Rate"
    assert entry.steps == ["check logs", "restart"]
    assert entry.reference == "http://example.com"


def test_load_parses_multiple_entries(tmp_path):
    data = {
        "runbooks": {
            "failure_rate": {"title": "FR", "steps": ["a"]},
            "throughput": {"title": "TP", "steps": ["b"]},
        }
    }
    p = _write_yaml(tmp_path, data)
    result = load_runbook_config(p)
    assert len(result) == 2
    assert "throughput" in result


def test_load_entry_without_reference(tmp_path):
    data = {
        "runbooks": {
            "throughput": {"title": "Low TP", "steps": ["check io"]},
        }
    }
    p = _write_yaml(tmp_path, data)
    result = load_runbook_config(p)
    assert result["throughput"].reference is None


def test_load_skips_non_dict_entries(tmp_path):
    data = {"runbooks": {"bad_entry": "not a dict", "good": {"title": "G", "steps": []}}}
    p = _write_yaml(tmp_path, data)
    result = load_runbook_config(p)
    assert "bad_entry" not in result
    assert "good" in result
