"""Load pipeline metadata definitions from a YAML config file."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from pipewatch.metadata import PipelineMetadata, set_metadata


def load_metadata_config(path: str = "pipewatch-metadata.yaml") -> List[PipelineMetadata]:
    """Parse a YAML file and populate the metadata store.

    Expected format::

        pipelines:
          - name: ingest
            owner: data-team
            description: Raw ingestion pipeline
            tags: [critical, nightly]
            env: production

    Returns the list of parsed PipelineMetadata entries.
    """
    if yaml is None:  # pragma: no cover
        raise RuntimeError("PyYAML is required for metadata_config")

    p = Path(path)
    if not p.exists():
        return []

    raw = yaml.safe_load(p.read_text()) or {}
    pipelines: List[Dict[str, Any]] = raw.get("pipelines", []) or []

    results: List[PipelineMetadata] = []
    for entry in pipelines:
        name = entry.get("name", "")
        if not name:
            continue
        kwargs: Dict[str, Any] = {}
        if "owner" in entry:
            kwargs["owner"] = entry["owner"]
        if "description" in entry:
            kwargs["description"] = entry["description"]
        if "tags" in entry:
            kwargs["tags"] = list(entry["tags"] or [])
        # Remaining keys go into extra
        reserved = {"name", "owner", "description", "tags"}
        for k, v in entry.items():
            if k not in reserved:
                kwargs[k] = v
        results.append(set_metadata(name, **kwargs))

    return results
