"""Load tag rules from YAML configuration."""
from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.tagger import TagRule

_DEFAULT_PATH = "pipewatch-tags.yaml"


def load_tag_config(path: str = _DEFAULT_PATH) -> List[TagRule]:
    """Load tag rules from a YAML file.

    Returns an empty list if the file is missing or contains no rules.
    """
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open() as fh:
        data = yaml.safe_load(fh) or {}
    raw_rules = data.get("tag_rules", [])
    return [_parse_rule(r) for r in raw_rules]


def _parse_rule(raw: dict) -> TagRule:
    return TagRule(
        tag=raw["tag"],
        name_pattern=raw.get("name_pattern"),
        status=raw.get("status"),
        min_failure_rate=raw.get("min_failure_rate"),
        max_throughput=raw.get("max_throughput"),
    )


def rules_for_pipeline(pipeline: str, rules: List[TagRule]) -> List[TagRule]:
    """Filter rules relevant to a specific pipeline name."""
    from fnmatch import fnmatch
    return [
        r for r in rules
        if r.name_pattern is None or fnmatch(pipeline, r.name_pattern)
    ]
