"""Tests for pipewatch.sampler."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.sampler import MetricSampler, SampleWindow, SamplerConfig


def make_metric(name: str = "pipe", processed: int = 100, failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=name,
        processed=processed,
        failed=failed,
        status=PipelineStatus.HEALTHY,
    )


def test_record_adds_to_window():
    sampler = MetricSampler()
    sampler.record(make_metric())
    w = sampler.window("pipe")
    assert w is not None
    assert len(w) == 1


def test_window_respects_max_size():
    cfg = SamplerConfig(window_size=3)
    sampler = MetricSampler(cfg)
    for i in range(6):
        sampler.record(make_metric(processed=i * 10))
    w = sampler.window("pipe")
    assert len(w) == 3


def test_latest_returns_most_recent():
    sampler = MetricSampler()
    sampler.record(make_metric(processed=10))
    sampler.record(make_metric(processed=99))
    assert sampler.window("pipe").latest().processed == 99


def test_has_enough_samples_false_when_below_min():
    cfg = SamplerConfig(min_samples=3)
    sampler = MetricSampler(cfg)
    sampler.record(make_metric())
    sampler.record(make_metric())
    assert not sampler.has_enough_samples("pipe")


def test_has_enough_samples_true_at_min():
    cfg = SamplerConfig(min_samples=2)
    sampler = MetricSampler(cfg)
    sampler.record(make_metric())
    sampler.record(make_metric())
    assert sampler.has_enough_samples("pipe")


def test_all_pipelines_lists_recorded_names():
    sampler = MetricSampler()
    sampler.record(make_metric("alpha"))
    sampler.record(make_metric("beta"))
    assert set(sampler.all_pipelines()) == {"alpha", "beta"}


def test_reset_removes_pipeline():
    sampler = MetricSampler()
    sampler.record(make_metric())
    sampler.reset("pipe")
    assert sampler.window("pipe") is None


def test_clear_removes_all():
    sampler = MetricSampler()
    sampler.record(make_metric("a"))
    sampler.record(make_metric("b"))
    sampler.clear()
    assert sampler.all_pipelines() == []


def test_as_list_returns_copy():
    sampler = MetricSampler()
    m1 = make_metric(processed=1)
    m2 = make_metric(processed=2)
    sampler.record(m1)
    sampler.record(m2)
    lst = sampler.window("pipe").as_list()
    assert len(lst) == 2
    assert lst[0].processed == 1
    assert lst[1].processed == 2


def test_window_returns_none_for_unknown_pipeline():
    sampler = MetricSampler()
    assert sampler.window("nonexistent") is None
