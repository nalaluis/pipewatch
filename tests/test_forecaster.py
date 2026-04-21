"""Tests for pipewatch.forecaster."""
from __future__ import annotations

import pytest
from pipewatch.forecaster import (
    ForecastResult,
    _least_squares_slope,
    _confidence,
    forecast,
)
from pipewatch.history import HistoryEntry


def _make_entry(pipeline: str, failure_rate: float, throughput: float) -> HistoryEntry:
    return HistoryEntry(pipeline=pipeline, failure_rate=failure_rate, throughput=throughput)


# ---------------------------------------------------------------------------
# _least_squares_slope
# ---------------------------------------------------------------------------

def test_slope_of_flat_series_is_zero():
    assert _least_squares_slope([5.0, 5.0, 5.0]) == pytest.approx(0.0)


def test_slope_of_strictly_increasing_series_is_positive():
    slope = _least_squares_slope([0.0, 1.0, 2.0, 3.0])
    assert slope > 0


def test_slope_of_strictly_decreasing_series_is_negative():
    slope = _least_squares_slope([3.0, 2.0, 1.0, 0.0])
    assert slope < 0


def test_slope_single_value_returns_zero():
    assert _least_squares_slope([42.0]) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _confidence
# ---------------------------------------------------------------------------

def test_confidence_below_min_samples_is_zero():
    assert _confidence(2, min_samples=3) == pytest.approx(0.0)


def test_confidence_at_min_samples_is_zero():
    assert _confidence(3, min_samples=3) == pytest.approx(0.0)


def test_confidence_at_max_samples_is_one():
    assert _confidence(20, min_samples=3, max_samples=20) == pytest.approx(1.0)


def test_confidence_beyond_max_samples_capped_at_one():
    assert _confidence(100, min_samples=3, max_samples=20) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# forecast
# ---------------------------------------------------------------------------

def test_forecast_returns_none_for_unknown_pipeline():
    history = [_make_entry("pipe-a", 0.1, 100.0)]
    assert forecast(history, "pipe-x") is None


def test_forecast_returns_two_results():
    history = [_make_entry("pipe-a", 0.1 * i, 100.0 + i) for i in range(5)]
    results = forecast(history, "pipe-a")
    assert results is not None
    assert len(results) == 2
    metrics = {r.metric for r in results}
    assert metrics == {"failure_rate", "throughput"}


def test_forecast_predicted_value_is_non_negative():
    history = [_make_entry("pipe-a", 0.0, 0.0) for _ in range(5)]
    results = forecast(history, "pipe-a")
    assert results is not None
    for r in results:
        assert r.predicted_value >= 0.0


def test_forecast_steps_ahead_reflected_in_result():
    history = [_make_entry("p", 0.1 * i, 10.0) for i in range(5)]
    results = forecast(history, "p", steps_ahead=3)
    assert results is not None
    for r in results:
        assert r.steps_ahead == 3


def test_forecast_str_contains_pipeline_name():
    history = [_make_entry("my-pipe", 0.05 * i, 50.0) for i in range(6)]
    results = forecast(history, "my-pipe")
    assert results is not None
    for r in results:
        assert "my-pipe" in str(r)


def test_forecast_increasing_failure_rate_slope_positive():
    history = [_make_entry("p", 0.1 * i, 100.0) for i in range(6)]
    results = forecast(history, "p")
    assert results is not None
    fr = next(r for r in results if r.metric == "failure_rate")
    assert fr.trend_slope > 0
