"""Tests for pipewatch.velocity."""
import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.velocity import (
    VelocityConfig,
    VelocityResult,
    compute_velocity,
    record_metric,
    reset_velocity,
)


def make_metric(pipeline: str, failure_rate: float, throughput: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=PipelineStatus.HEALTHY,
        failure_rate=failure_rate,
        throughput=throughput,
        processed=100,
        failed=int(failure_rate * 100),
    )


@pytest.fixture(autouse=True)
def clean_state():
    reset_velocity()
    yield
    reset_velocity()


def test_compute_velocity_returns_none_without_enough_samples():
    cfg = VelocityConfig(min_samples=3)
    record_metric(make_metric("pipe", 0.1, 50.0), cfg)
    record_metric(make_metric("pipe", 0.2, 45.0), cfg)
    result = compute_velocity("pipe", cfg)
    assert result is None


def test_compute_velocity_returns_result_with_enough_samples():
    cfg = VelocityConfig(min_samples=3)
    for fr in [0.1, 0.2, 0.3]:
        record_metric(make_metric("pipe", fr, 100.0), cfg)
    result = compute_velocity("pipe", cfg)
    assert result is not None
    assert isinstance(result, VelocityResult)
    assert result.pipeline == "pipe"
    assert result.samples_used == 3


def test_failure_rate_velocity_positive_for_increasing_failure_rate():
    cfg = VelocityConfig(min_samples=3)
    for fr in [0.1, 0.2, 0.3]:
        record_metric(make_metric("pipe", fr, 100.0), cfg)
    result = compute_velocity("pipe", cfg)
    assert result is not None
    assert result.failure_rate_velocity > 0.0


def test_throughput_velocity_negative_for_decreasing_throughput():
    cfg = VelocityConfig(min_samples=3)
    for tp in [100.0, 80.0, 60.0]:
        record_metric(make_metric("pipe", 0.1, tp), cfg)
    result = compute_velocity("pipe", cfg)
    assert result is not None
    assert result.throughput_velocity < 0.0


def test_flat_series_produces_near_zero_velocity():
    cfg = VelocityConfig(min_samples=3)
    for _ in range(5):
        record_metric(make_metric("pipe", 0.05, 200.0), cfg)
    result = compute_velocity("pipe", cfg)
    assert result is not None
    assert abs(result.failure_rate_velocity) < 1e-9
    assert abs(result.throughput_velocity) < 1e-9


def test_window_size_limits_history():
    cfg = VelocityConfig(min_samples=3, window_size=4)
    for i in range(10):
        record_metric(make_metric("pipe", float(i) * 0.01, 100.0), cfg)
    result = compute_velocity("pipe", cfg)
    assert result is not None
    assert result.samples_used == 4


def test_reset_velocity_clears_single_pipeline():
    cfg = VelocityConfig(min_samples=2)
    for fr in [0.1, 0.2, 0.3]:
        record_metric(make_metric("pipe_a", fr, 50.0), cfg)
        record_metric(make_metric("pipe_b", fr, 50.0), cfg)
    reset_velocity("pipe_a")
    assert compute_velocity("pipe_a", cfg) is None
    assert compute_velocity("pipe_b", cfg) is not None


def test_velocity_result_str_contains_pipeline_name():
    r = VelocityResult(pipeline="my_pipe", failure_rate_velocity=0.05,
                       throughput_velocity=-2.0, samples_used=5)
    s = str(r)
    assert "my_pipe" in s
    assert "failure_rate_velocity" in s
    assert "throughput_velocity" in s
    assert "n=5" in s
