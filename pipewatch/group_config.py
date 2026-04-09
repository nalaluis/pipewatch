"""Load pipeline grouping configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class GroupConfig:
    mode: str = "status"          # "status" | "tag" | "name_prefix"
    tag_key: Optional[str] = None  # used when mode == "tag"
    name_prefix_len: int = 1       # used when mode == "name_prefix"


def load_group_config(path: str = "pipewatch-groups.yaml") -> GroupConfig:
    """Load grouping config from YAML, returning defaults if file is missing."""
    p = Path(path)
    if not p.exists():
        return GroupConfig()
    with p.open() as fh:
        data = yaml.safe_load(fh) or {}
    grouping = data.get("grouping", {})
    return GroupConfig(
        mode=grouping.get("mode", "status"),
        tag_key=grouping.get("tag_key"),
        name_prefix_len=int(grouping.get("name_prefix_len", 1)),
    )
