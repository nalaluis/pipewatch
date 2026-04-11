"""Tests for pipewatch.metadata and pipewatch.metadata_config."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.metadata import (
    PipelineMetadata,
    all_metadata,
    clear_metadata,
    format_metadata,
    get_metadata,
    set_metadata,
)
from pipewatch.metadata_config import load_metadata_config


@pytest.fixture(autouse=True)
def reset_store():
    """Ensure the module-level store is clean before every test."""
    clear_metadata()
    yield
    clear_metadata()


# ---------------------------------------------------------------------------
# set_metadata / get_metadata
# ---------------------------------------------------------------------------

def test_get_metadata_unknown_returns_empty_entry():
    meta = get_metadata("ghost")
    assert meta.pipeline == "ghost"
    assert meta.owner is None
    assert meta.tags == []
    assert meta.description is None
    assert meta.extra == {}


def test_set_metadata_stores_owner_and_tags():
    meta = set_metadata("ingest", owner="data-team", tags=["critical"])
    assert meta.owner == "data-team"
    assert meta.tags == ["critical"]


def test_set_metadata_merges_on_second_call():
    set_metadata("ingest", owner="data-team")
    meta = set_metadata("ingest", description="raw ingestion")
    assert meta.owner == "data-team"
    assert meta.description == "raw ingestion"


def test_set_metadata_stores_extra_keys():
    meta = set_metadata("transform", env="production", sla="99.9")
    assert meta.extra["env"] == "production"
    assert meta.extra["sla"] == "99.9"


def test_to_dict_includes_all_fields():
    set_metadata("load", owner="ops", tags=["nightly"], description="load step", env="prod")
    d = get_metadata("load").to_dict()
    assert d["pipeline"] == "load"
    assert d["owner"] == "ops"
    assert d["tags"] == ["nightly"]
    assert d["description"] == "load step"
    assert d["extra"]["env"] == "prod"


# ---------------------------------------------------------------------------
# clear_metadata
# ---------------------------------------------------------------------------

def test_clear_single_pipeline_removes_entry():
    set_metadata("a", owner="x")
    set_metadata("b", owner="y")
    clear_metadata("a")
    assert get_metadata("a").owner is None
    assert get_metadata("b").owner == "y"


def test_clear_all_removes_everything():
    set_metadata("a", owner="x")
    set_metadata("b", owner="y")
    clear_metadata()
    assert all_metadata() == []


# ---------------------------------------------------------------------------
# all_metadata
# ---------------------------------------------------------------------------

def test_all_metadata_returns_sorted_entries():
    set_metadata("z-pipe", owner="z")
    set_metadata("a-pipe", owner="a")
    names = [m.pipeline for m in all_metadata()]
    assert names == ["a-pipe", "z-pipe"]


# ---------------------------------------------------------------------------
# format_metadata
# ---------------------------------------------------------------------------

def test_format_metadata_minimal():
    meta = PipelineMetadata(pipeline="p")
    out = format_metadata(meta)
    assert out == "pipeline=p"


def test_format_metadata_full():
    meta = PipelineMetadata(
        pipeline="ingest",
        owner="team",
        tags=["a", "b"],
        description="desc",
        extra={"env": "prod"},
    )
    out = format_metadata(meta)
    assert "pipeline=ingest" in out
    assert "owner=team" in out
    assert "tags=a,b" in out
    assert "env=prod" in out


# ---------------------------------------------------------------------------
# load_metadata_config
# ---------------------------------------------------------------------------

def test_load_metadata_config_missing_file_returns_empty(tmp_path):
    result = load_metadata_config(str(tmp_path / "missing.yaml"))
    assert result == []


def test_load_metadata_config_parses_entries(tmp_path):
    cfg = tmp_path / "meta.yaml"
    cfg.write_text(textwrap.dedent("""
        pipelines:
          - name: ingest
            owner: data-team
            tags: [critical, nightly]
            description: Raw ingestion
            env: production
    """))
    results = load_metadata_config(str(cfg))
    assert len(results) == 1
    m = results[0]
    assert m.pipeline == "ingest"
    assert m.owner == "data-team"
    assert "critical" in m.tags
    assert m.description == "Raw ingestion"
    assert m.extra.get("env") == "production"


def test_load_metadata_config_skips_entries_without_name(tmp_path):
    cfg = tmp_path / "meta.yaml"
    cfg.write_text(textwrap.dedent("""
        pipelines:
          - owner: nobody
          - name: valid
            owner: someone
    """))
    results = load_metadata_config(str(cfg))
    assert len(results) == 1
    assert results[0].pipeline == "valid"


def test_load_metadata_config_populates_store(tmp_path):
    cfg = tmp_path / "meta.yaml"
    cfg.write_text(textwrap.dedent("""
        pipelines:
          - name: pipe-a
            owner: alice
          - name: pipe-b
            owner: bob
    """))
    load_metadata_config(str(cfg))
    assert get_metadata("pipe-a").owner == "alice"
    assert get_metadata("pipe-b").owner == "bob"
