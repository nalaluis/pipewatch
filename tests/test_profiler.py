"""Tests for pipewatch.profiler."""

import pytest
from pipewatch.profiler import (
    ProfileEntry,
    ProfileStats,
    record_profile,
    get_stats,
    all_stats,
    clear_profiles,
    format_profile_report,
)


@pytest.fixture(autouse=True)
def clean_state():
    clear_profiles()
    yield
    clear_profiles()


def test_record_profile_returns_entry():
    entry = record_profile("etl_main", 1.5)
    assert isinstance(entry, ProfileEntry)
    assert entry.pipeline == "etl_main"
    assert entry.duration_seconds == 1.5


def test_get_stats_returns_none_for_unknown_pipeline():
    assert get_stats("nonexistent") is None


def test_get_stats_single_entry():
    record_profile("pipe_a", 2.0)
    stats = get_stats("pipe_a")
    assert stats is not None
    assert stats.count == 1
    assert stats.avg_duration == 2.0
    assert stats.min_duration == 2.0
    assert stats.max_duration == 2.0
    assert stats.last_duration == 2.0


def test_get_stats_multiple_entries_averages_correctly():
    record_profile("pipe_b", 1.0)
    record_profile("pipe_b", 3.0)
    record_profile("pipe_b", 2.0)
    stats = get_stats("pipe_b")
    assert stats.count == 3
    assert stats.avg_duration == pytest.approx(2.0)
    assert stats.min_duration == 1.0
    assert stats.max_duration == 3.0
    assert stats.last_duration == 2.0


def test_all_stats_returns_all_pipelines():
    record_profile("alpha", 0.5)
    record_profile("beta", 1.5)
    stats = all_stats()
    names = {s.pipeline for s in stats}
    assert "alpha" in names
    assert "beta" in names


def test_clear_profiles_specific_pipeline():
    record_profile("pipe_x", 1.0)
    record_profile("pipe_y", 2.0)
    clear_profiles("pipe_x")
    assert get_stats("pipe_x") is None
    assert get_stats("pipe_y") is not None


def test_clear_profiles_all():
    record_profile("p1", 1.0)
    record_profile("p2", 2.0)
    clear_profiles()
    assert all_stats() == []


def test_profile_stats_str():
    stats = ProfileStats(
        pipeline="my_pipe",
        count=5,
        avg_duration=1.234,
        min_duration=0.5,
        max_duration=2.0,
        last_duration=1.1,
    )
    result = str(stats)
    assert "my_pipe" in result
    assert "avg=1.234s" in result
    assert "n=5" in result


def test_format_profile_report_empty():
    report = format_profile_report([])
    assert "No profiling data" in report


def test_format_profile_report_lists_pipelines():
    record_profile("fast_pipe", 0.1)
    record_profile("slow_pipe", 5.0)
    stats = all_stats()
    report = format_profile_report(stats)
    assert "fast_pipe" in report
    assert "slow_pipe" in report
    assert "Pipeline Profiling Report" in report
