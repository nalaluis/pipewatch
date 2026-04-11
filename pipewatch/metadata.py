"""Pipeline metadata store: attach and retrieve arbitrary key-value metadata for pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# Module-level store keyed by pipeline name
_store: Dict[str, Dict[str, Any]] = {}


@dataclass
class PipelineMetadata:
    """Metadata bag for a single pipeline."""

    pipeline: str
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    description: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "tags": self.tags,
            "owner": self.owner,
            "description": self.description,
            "extra": self.extra,
        }


def set_metadata(pipeline: str, **kwargs: Any) -> PipelineMetadata:
    """Set or update metadata for a pipeline. Returns the updated entry."""
    existing = _store.get(pipeline, {})
    existing.update(kwargs)
    _store[pipeline] = existing
    return get_metadata(pipeline)


def get_metadata(pipeline: str) -> PipelineMetadata:
    """Return metadata for a pipeline, or an empty entry if none exists."""
    data = _store.get(pipeline, {})
    return PipelineMetadata(
        pipeline=pipeline,
        tags=data.get("tags", []),
        owner=data.get("owner"),
        description=data.get("description"),
        extra={k: v for k, v in data.items() if k not in ("tags", "owner", "description")},
    )


def clear_metadata(pipeline: Optional[str] = None) -> None:
    """Clear metadata for one pipeline, or all pipelines if none specified."""
    global _store
    if pipeline is None:
        _store = {}
    else:
        _store.pop(pipeline, None)


def all_metadata() -> List[PipelineMetadata]:
    """Return metadata for every known pipeline."""
    return [get_metadata(name) for name in sorted(_store)]


def format_metadata(meta: PipelineMetadata) -> str:
    """Human-readable one-line summary of pipeline metadata."""
    parts = [f"pipeline={meta.pipeline}"]
    if meta.owner:
        parts.append(f"owner={meta.owner}")
    if meta.tags:
        parts.append(f"tags={','.join(meta.tags)}")
    if meta.description:
        parts.append(f"desc={meta.description!r}")
    if meta.extra:
        for k, v in meta.extra.items():
            parts.append(f"{k}={v}")
    return " ".join(parts)
