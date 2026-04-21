"""Tests for pipewatch.mirror_config."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.mirror_config import MirrorConfig, load_mirror_config


def _write(tmp_path: Path, content: str) -> str:
    p = tmp_path / "pipewatch-mirror.yaml"
    p.write_text(textwrap.dedent(content))
    return str(p)


def test_mirror_config_dataclass_defaults():
    cfg = MirrorConfig()
    assert cfg.left_env == "production"
    assert cfg.right_env == "staging"
    assert cfg.left_config == "pipewatch.yaml"
    assert cfg.right_config == "pipewatch-staging.yaml"
    assert cfg.fail_on_divergence is False


def test_load_missing_file_returns_defaults(tmp_path):
    cfg = load_mirror_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.left_env == "production"
    assert cfg.right_env == "staging"


def test_load_empty_yaml_returns_defaults(tmp_path):
    path = _write(tmp_path, "")
    cfg = load_mirror_config(path)
    assert cfg.left_env == "production"


def test_load_partial_yaml(tmp_path):
    path = _write(
        tmp_path,
        """
        left_env: eu-prod
        """,
    )
    cfg = load_mirror_config(path)
    assert cfg.left_env == "eu-prod"
    assert cfg.right_env == "staging"  # default preserved


def test_load_full_yaml(tmp_path):
    path = _write(
        tmp_path,
        """
        left_env: prod
        right_env: canary
        left_config: prod.yaml
        right_config: canary.yaml
        fail_on_divergence: true
        """,
    )
    cfg = load_mirror_config(path)
    assert cfg.left_env == "prod"
    assert cfg.right_env == "canary"
    assert cfg.left_config == "prod.yaml"
    assert cfg.right_config == "canary.yaml"
    assert cfg.fail_on_divergence is True
