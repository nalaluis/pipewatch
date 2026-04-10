"""Tests for pipewatch.checkpoint_config."""
import textwrap
import pytest
from pipewatch.checkpoint_config import CheckpointConfig, load_checkpoint_config


def test_checkpoint_config_dataclass_defaults():
    cfg = CheckpointConfig()
    assert cfg.enabled is True
    assert cfg.directory == ".pipewatch/checkpoints"
    assert cfg.max_consecutive_failures == 3


def test_load_checkpoint_config_missing_file_returns_defaults(tmp_path):
    cfg = load_checkpoint_config(str(tmp_path / "nope.yaml"))
    assert isinstance(cfg, CheckpointConfig)
    assert cfg.enabled is True


def test_load_checkpoint_config_empty_yaml(tmp_path):
    p = tmp_path / "checkpoint.yaml"
    p.write_text("")
    cfg = load_checkpoint_config(str(p))
    assert cfg.enabled is True
    assert cfg.max_consecutive_failures == 3


def test_load_checkpoint_config_partial_yaml(tmp_path):
    p = tmp_path / "checkpoint.yaml"
    p.write_text(textwrap.dedent("""\
        checkpoint:
          enabled: false
    """))
    cfg = load_checkpoint_config(str(p))
    assert cfg.enabled is False
    assert cfg.max_consecutive_failures == 3


def test_load_checkpoint_config_full_yaml(tmp_path):
    p = tmp_path / "checkpoint.yaml"
    p.write_text(textwrap.dedent("""\
        checkpoint:
          enabled: true
          directory: /tmp/cp
          max_consecutive_failures: 5
    """))
    cfg = load_checkpoint_config(str(p))
    assert cfg.enabled is True
    assert cfg.directory == "/tmp/cp"
    assert cfg.max_consecutive_failures == 5
