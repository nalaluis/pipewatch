"""Tests for pipewatch.label_config."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.label_config import load_label_config, rules_for_pipeline
from pipewatch.labeler import LabelRule


@pytest.fixture()
def label_yaml(tmp_path: Path) -> Path:
    content = textwrap.dedent("""\
        label_rules:
          - pattern: "payments-*"
            labels:
              - finance
              - critical
          - pattern: "*-nightly"
            labels:
              - scheduled
    """)
    p = tmp_path / "pipewatch-labels.yaml"
    p.write_text(content)
    return p


def test_load_label_config_parses_rules(label_yaml: Path):
    rules = load_label_config(str(label_yaml))
    assert len(rules) == 2
    assert rules[0].pattern == "payments-*"
    assert "finance" in rules[0].labels
    assert "critical" in rules[0].labels


def test_load_label_config_missing_file_returns_empty(tmp_path: Path):
    rules = load_label_config(str(tmp_path / "nonexistent.yaml"))
    assert rules == []


def test_load_label_config_empty_yaml(tmp_path: Path):
    p = tmp_path / "empty.yaml"
    p.write_text("")
    rules = load_label_config(str(p))
    assert rules == []


def test_rules_for_pipeline_filters_correctly(label_yaml: Path):
    all_rules = load_label_config(str(label_yaml))
    matched = rules_for_pipeline("payments-etl", all_rules)
    assert len(matched) == 1
    assert matched[0].pattern == "payments-*"


def test_rules_for_pipeline_no_match(label_yaml: Path):
    all_rules = load_label_config(str(label_yaml))
    matched = rules_for_pipeline("analytics-daily", all_rules)
    assert matched == []


def test_rules_for_pipeline_nightly_match(label_yaml: Path):
    all_rules = load_label_config(str(label_yaml))
    matched = rules_for_pipeline("orders-nightly", all_rules)
    assert len(matched) == 1
    assert "scheduled" in matched[0].labels
