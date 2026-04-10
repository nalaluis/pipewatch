"""Additional tests for audit_config edge cases."""

from __future__ import annotations

import pytest
from pipewatch.audit_config import AuditConfig, load_audit_config


def test_audit_config_dataclass_defaults():
    cfg = AuditConfig()
    assert cfg.enabled is True
    assert cfg.audit_dir == ".pipewatch/audit"
    assert cfg.max_entries_per_pipeline is None
    assert cfg.include_notes is False


def test_load_audit_config_empty_yaml(tmp_path):
    path = tmp_path / "audit.yaml"
    path.write_text("")
    cfg = load_audit_config(str(path))
    assert cfg.enabled is True


def test_load_audit_config_partial_yaml(tmp_path):
    path = tmp_path / "audit.yaml"
    path.write_text("audit:\n  enabled: false\n")
    cfg = load_audit_config(str(path))
    assert cfg.enabled is False
    assert cfg.audit_dir == ".pipewatch/audit"


def test_load_audit_config_custom_dir(tmp_path):
    path = tmp_path / "audit.yaml"
    path.write_text("audit:\n  audit_dir: /var/log/pipewatch\n")
    cfg = load_audit_config(str(path))
    assert cfg.audit_dir == "/var/log/pipewatch"


def test_load_audit_config_missing_file_returns_defaults():
    cfg = load_audit_config("definitely-missing.yaml")
    assert isinstance(cfg, AuditConfig)
    assert cfg.enabled is True
