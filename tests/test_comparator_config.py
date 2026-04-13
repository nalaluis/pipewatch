"""Tests for pipewatch.comparator_config."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.comparator_config import ComparatorConfig, load_comparator_config


def _write(tmp_path: Path, content: str) -> str:
    p = tmp_path / "pipewatch-comparator.yaml"
    p.write_text(textwrap.dedent(content))
    return str(p)


def test_load_missing_file_returns_defaults(tmp_path):
    cfg = load_comparator_config(str(tmp_path / "nonexistent.yaml"))
    assert isinstance(cfg, ComparatorConfig)
    assert cfg.include_pipelines == []
    assert cfg.exclude_pipelines == []
    assert cfg.highlight_regressions is True
    assert cfg.highlight_improvements is True


def test_load_empty_yaml_returns_defaults(tmp_path):
    path = _write(tmp_path, "")
    cfg = load_comparator_config(path)
    assert cfg.include_pipelines == []
    assert cfg.exclude_pipelines == []


def test_load_partial_yaml(tmp_path):
    path = _write(tmp_path, """
        highlight_regressions: false
    """)
    cfg = load_comparator_config(path)
    assert cfg.highlight_regressions is False
    assert cfg.highlight_improvements is True  # default


def test_load_full_yaml(tmp_path):
    path = _write(tmp_path, """
        include_pipelines:
          - alpha
          - beta
        exclude_pipelines:
          - gamma
        highlight_regressions: true
        highlight_improvements: false
    """)
    cfg = load_comparator_config(path)
    assert cfg.include_pipelines == ["alpha", "beta"]
    assert cfg.exclude_pipelines == ["gamma"]
    assert cfg.highlight_regressions is True
    assert cfg.highlight_improvements is False


def test_comparator_config_dataclass_defaults():
    cfg = ComparatorConfig()
    assert cfg.include_pipelines == []
    assert cfg.exclude_pipelines == []
    assert cfg.highlight_regressions is True
    assert cfg.highlight_improvements is True
