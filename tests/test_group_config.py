"""Tests for pipewatch.group_config."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.group_config import GroupConfig, load_group_config


GROUP_YAML = """
grouping:
  mode: tag
  tag_key: team
  name_prefix_len: 2
"""


def test_load_group_config_defaults_when_missing(tmp_path):
    cfg = load_group_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.mode == "status"
    assert cfg.tag_key is None
    assert cfg.name_prefix_len == 1


def test_load_group_config_parses_yaml(tmp_path):
    p = tmp_path / "groups.yaml"
    p.write_text(GROUP_YAML)
    cfg = load_group_config(str(p))
    assert cfg.mode == "tag"
    assert cfg.tag_key == "team"
    assert cfg.name_prefix_len == 2


def test_load_group_config_empty_yaml(tmp_path):
    p = tmp_path / "groups.yaml"
    p.write_text("")
    cfg = load_group_config(str(p))
    assert cfg.mode == "status"


def test_load_group_config_partial_yaml(tmp_path):
    p = tmp_path / "groups.yaml"
    p.write_text("grouping:\n  mode: name_prefix\n")
    cfg = load_group_config(str(p))
    assert cfg.mode == "name_prefix"
    assert cfg.tag_key is None
    assert cfg.name_prefix_len == 1


def test_group_config_dataclass_defaults():
    cfg = GroupConfig()
    assert cfg.mode == "status"
    assert cfg.tag_key is None
    assert cfg.name_prefix_len == 1
