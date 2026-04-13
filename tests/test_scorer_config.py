"""Additional focused tests for scorer_config edge cases."""
from __future__ import annotations

import textwrap
from pathlib import Path

from pipewatch.scorer_config import load_scorer_config
from pipewatch.scorer import ScorerConfig


def test_load_scorer_config_empty_yaml_returns_defaults(tmp_path):
    p = tmp_path / "scorer.yaml"
    p.write_text("")
    cfg = load_scorer_config(str(p))
    assert isinstance(cfg, ScorerConfig)
    assert cfg.failure_rate_weight == 0.5


def test_load_scorer_config_top_level_keys(tmp_path):
    """Config without 'scorer' wrapper should still parse."""
    yaml_text = textwrap.dedent("""
        failure_rate_weight: 0.4
        throughput_weight: 0.4
        status_weight: 0.2
        throughput_baseline: 75.0
    """)
    p = tmp_path / "scorer.yaml"
    p.write_text(yaml_text)
    cfg = load_scorer_config(str(p))
    assert cfg.failure_rate_weight == 0.4
    assert cfg.throughput_baseline == 75.0


def test_scorer_config_dataclass_defaults():
    cfg = ScorerConfig()
    assert cfg.failure_rate_weight == 0.5
    assert cfg.throughput_weight == 0.3
    assert cfg.status_weight == 0.2
    assert cfg.throughput_baseline == 100.0


def test_load_scorer_config_zero_weights(tmp_path):
    yaml_text = textwrap.dedent("""
        scorer:
          failure_rate_weight: 0.0
          throughput_weight: 0.0
          status_weight: 0.0
          throughput_baseline: 1.0
    """)
    p = tmp_path / "scorer.yaml"
    p.write_text(yaml_text)
    cfg = load_scorer_config(str(p))
    assert cfg.failure_rate_weight == 0.0
    assert cfg.throughput_weight == 0.0
    assert cfg.status_weight == 0.0
