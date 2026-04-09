"""Load label rules from YAML configuration."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import yaml

from pipewatch.labeler import LabelRule, parse_label_rules

_DEFAULT_PATH = "pipewatch-labels.yaml"


def load_label_config(path: str = _DEFAULT_PATH) -> List[LabelRule]:
    """Load label rules from a YAML file.

    Returns an empty list if the file does not exist.
    """
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open() as fh:
        data = yaml.safe_load(fh) or {}
    raw_rules = data.get("label_rules", [])
    return parse_label_rules(raw_rules)


def rules_for_pipeline(
    pipeline: str,
    rules: List[LabelRule],
) -> List[LabelRule]:
    """Return only the rules whose pattern matches *pipeline*."""
    from fnmatch import fnmatch
    return [r for r in rules if fnmatch(pipeline, r.pattern)]
