"""Tests for pipewatch.watchdog_config."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.watchdog_config import load_watchdog_config
from pipewatch.watchdog import WatchdogConfig


def test_load_watchdog_config_missing_file_returns_defaults(tmp_path):
    cfg = load_watchdog_config(str(tmp_path / "nonexistent.yaml"))
    assert isinstance(cfg, WatchdogConfig)
    assert cfg.stale_after_seconds == 300
    assert cfg.enabled is True


def test_load_watchdog_config_empty_yaml(tmp_path):
    f = tmp_path / "watchdog.yaml"
    f.write_text("")
    cfg = load_watchdog_config(str(f))
    assert cfg.stale_after_seconds == 300
    assert cfg.enabled is True


def test_load_watchdog_config_partial_yaml(tmp_path):
    f = tmp_path / "watchdog.yaml"
    f.write_text(textwrap.dedent("""\
        watchdog:
          stale_after_seconds: 120
    """))
    cfg = load_watchdog_config(str(f))
    assert cfg.stale_after_seconds == 120
    assert cfg.enabled is True


def test_load_watchdog_config_full_yaml(tmp_path):
    f = tmp_path / "watchdog.yaml"
    f.write_text(textwrap.dedent("""\
        watchdog:
          stale_after_seconds: 600
          enabled: false
    """))
    cfg = load_watchdog_config(str(f))
    assert cfg.stale_after_seconds == 600
    assert cfg.enabled is False


def test_watchdog_config_dataclass_defaults():
    cfg = WatchdogConfig()
    assert cfg.stale_after_seconds == 300
    assert cfg.enabled is True
