"""Tests for pipewatch.checkpoint."""
import os
import pytest
from pipewatch.checkpoint import (
    Checkpoint,
    save_checkpoint,
    load_checkpoint,
    update_checkpoint,
    clear_checkpoint,
    _checkpoint_path,
)


@pytest.fixture()
def tmp_dir(tmp_path):
    return str(tmp_path / "checkpoints")


def make_checkpoint(pipeline: str = "etl", status: str = "healthy") -> Checkpoint:
    return Checkpoint(
        pipeline=pipeline,
        last_run_at="2024-01-01T00:00:00+00:00",
        last_status=status,
        run_count=1,
        consecutive_failures=0,
    )


def test_save_creates_file(tmp_dir):
    cp = make_checkpoint()
    path = save_checkpoint(cp, directory=tmp_dir)
    assert os.path.exists(path)


def test_load_returns_none_for_missing(tmp_dir):
    result = load_checkpoint("nonexistent", directory=tmp_dir)
    assert result is None


def test_roundtrip_preserves_fields(tmp_dir):
    cp = make_checkpoint(pipeline="my_pipeline", status="warning")
    save_checkpoint(cp, directory=tmp_dir)
    loaded = load_checkpoint("my_pipeline", directory=tmp_dir)
    assert loaded is not None
    assert loaded.pipeline == "my_pipeline"
    assert loaded.last_status == "warning"
    assert loaded.run_count == 1
    assert loaded.consecutive_failures == 0


def test_update_checkpoint_creates_new(tmp_dir):
    cp = update_checkpoint("pipe_a", "healthy", directory=tmp_dir)
    assert cp.run_count == 1
    assert cp.consecutive_failures == 0
    assert cp.last_status == "healthy"


def test_update_checkpoint_increments_run_count(tmp_dir):
    update_checkpoint("pipe_b", "healthy", directory=tmp_dir)
    cp = update_checkpoint("pipe_b", "healthy", directory=tmp_dir)
    assert cp.run_count == 2


def test_update_checkpoint_tracks_consecutive_failures(tmp_dir):
    update_checkpoint("pipe_c", "critical", directory=tmp_dir)
    cp = update_checkpoint("pipe_c", "critical", directory=tmp_dir)
    assert cp.consecutive_failures == 2


def test_update_checkpoint_resets_consecutive_on_healthy(tmp_dir):
    update_checkpoint("pipe_d", "critical", directory=tmp_dir)
    update_checkpoint("pipe_d", "critical", directory=tmp_dir)
    cp = update_checkpoint("pipe_d", "healthy", directory=tmp_dir)
    assert cp.consecutive_failures == 0


def test_clear_checkpoint_removes_file(tmp_dir):
    cp = make_checkpoint(pipeline="pipe_e")
    save_checkpoint(cp, directory=tmp_dir)
    removed = clear_checkpoint("pipe_e", directory=tmp_dir)
    assert removed is True
    assert load_checkpoint("pipe_e", directory=tmp_dir) is None


def test_clear_checkpoint_returns_false_when_missing(tmp_dir):
    removed = clear_checkpoint("ghost", directory=tmp_dir)
    assert removed is False


def test_checkpoint_path_sanitises_slashes(tmp_dir):
    path = _checkpoint_path("team/pipe", directory=tmp_dir)
    assert "/" not in os.path.basename(path)
