"""Tests for pipewatch.forecaster_config."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.forecaster_config import ForecasterConfig, load_forecaster_config


def _write(tmp_path: Path, content: str) -> str:
    p = tmp_path / "pipewatch-forecaster.yaml"
    p.write_text(textwrap.dedent(content))
    return str(p)


def test_load_missing_file_returns_defaults(tmp_path):
    cfg = load_forecaster_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg == ForecasterConfig()


def test_load_empty_yaml_returns_defaults(tmp_path):
    path = _write(tmp_path, "")
    cfg = load_forecaster_config(path)
    assert cfg == ForecasterConfig()


def test_load_partial_yaml(tmp_path):
    path = _write(tmp_path, "steps_ahead: 3\n")
    cfg = load_forecaster_config(path)
    assert cfg.steps_ahead == 3
    assert cfg.min_samples == 3  # default


def test_load_full_yaml(tmp_path):
    path = _write(
        tmp_path,
        """
        steps_ahead: 5
        min_samples: 10
        max_samples: 50
        enabled: false
        """,
    )
    cfg = load_forecaster_config(path)
    assert cfg.steps_ahead == 5
    assert cfg.min_samples == 10
    assert cfg.max_samples == 50
    assert cfg.enabled is False


def test_forecaster_config_dataclass_defaults():
    cfg = ForecasterConfig()
    assert cfg.steps_ahead == 1
    assert cfg.min_samples == 3
    assert cfg.max_samples == 20
    assert cfg.enabled is True
