"""Load custom runbook definitions from YAML configuration."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import yaml

from pipewatch.runbook import RunbookEntry

_DEFAULT_PATH = Path("pipewatch-runbooks.yaml")


def _parse_entry(raw: dict) -> RunbookEntry:
    return RunbookEntry(
        title=raw.get("title", "Untitled"),
        steps=raw.get("steps", []),
        reference=raw.get("reference"),
    )


def load_runbook_config(
    path: Path = _DEFAULT_PATH,
) -> Dict[str, RunbookEntry]:
    """Load keyword-to-RunbookEntry mapping from YAML.

    Returns an empty dict when the file is missing or empty.
    """
    if not path.exists():
        return {}
    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    runbooks: Dict[str, RunbookEntry] = {}
    for key, raw in data.get("runbooks", {}).items():
        if isinstance(raw, dict):
            runbooks[key] = _parse_entry(raw)
    return runbooks
