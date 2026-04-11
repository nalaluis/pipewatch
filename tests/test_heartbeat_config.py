"""Tests for pipewatch.heartbeat_config."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.heartbeat_config import load_heartbeat_config


def _write(tmp_path: Path, content: str) -> str:
    p = tmp_path / "pipewatch-heartbeat.yaml"
    p.write_text(content)
    return str(p)


def test_load_missing_file_returns_defaults(tmp_path: Path) -> None:
    cfg = load_heartbeat_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.timeout_seconds == 60.0
    assert cfg.warning_seconds == 30.0


def test_load_empty_yaml_returns_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, "")
    cfg = load_heartbeat_config(path)
    assert cfg.timeout_seconds == 60.0
    assert cfg.warning_seconds == 30.0


def test_load_partial_yaml(tmp_path: Path) -> None:
    path = _write(tmp_path, "heartbeat:\n  timeout_seconds: 120\n")
    cfg = load_heartbeat_config(path)
    assert cfg.timeout_seconds == 120.0
    assert cfg.warning_seconds == 30.0


def test_load_full_yaml(tmp_path: Path) -> None:
    content = "heartbeat:\n  timeout_seconds: 90\n  warning_seconds: 45\n"
    path = _write(tmp_path, content)
    cfg = load_heartbeat_config(path)
    assert cfg.timeout_seconds == 90.0
    assert cfg.warning_seconds == 45.0


def test_heartbeat_config_dataclass_defaults() -> None:
    from pipewatch.heartbeat import HeartbeatConfig

    cfg = HeartbeatConfig()
    assert cfg.timeout_seconds == 60.0
    assert cfg.warning_seconds == 30.0
