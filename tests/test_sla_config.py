"""Tests for pipewatch.sla_config."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.sla import SLAConfig
from pipewatch.sla_config import load_sla_config


def _write(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "pipewatch-sla.yaml"
    p.write_text(textwrap.dedent(content))
    return p


def test_load_sla_config_missing_file_returns_defaults(tmp_path):
    cfg = load_sla_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg == SLAConfig()


def test_load_sla_config_empty_yaml(tmp_path):
    p = _write(tmp_path, "")
    cfg = load_sla_config(str(p))
    assert cfg == SLAConfig()


def test_load_sla_config_partial_yaml(tmp_path):
    p = _write(tmp_path, """
        sla:
          max_failure_rate: 0.10
    """)
    cfg = load_sla_config(str(p))
    assert cfg.max_failure_rate == pytest.approx(0.10)
    assert cfg.min_throughput == pytest.approx(1.0)   # default
    assert cfg.max_downtime_seconds == pytest.approx(300.0)  # default


def test_load_sla_config_full_yaml(tmp_path):
    p = _write(tmp_path, """
        sla:
          max_failure_rate: 0.02
          min_throughput: 5.0
          max_downtime_seconds: 120
    """)
    cfg = load_sla_config(str(p))
    assert cfg.max_failure_rate == pytest.approx(0.02)
    assert cfg.min_throughput == pytest.approx(5.0)
    assert cfg.max_downtime_seconds == pytest.approx(120.0)


def test_sla_config_dataclass_defaults():
    cfg = SLAConfig()
    assert cfg.max_failure_rate == pytest.approx(0.05)
    assert cfg.min_throughput == pytest.approx(1.0)
    assert cfg.max_downtime_seconds == pytest.approx(300.0)
