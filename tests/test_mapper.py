"""Tests for pipewatch.mapper."""

from __future__ import annotations

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.mapper import MapRule, Mapper, MappedOutput, map_and_dispatch


def make_result(name: str, status: PipelineStatus = PipelineStatus.HEALTHY) -> HealthResult:
    metric = PipelineMetric(
        pipeline=name,
        total=100,
        failed=5,
        status=status,
    )
    return HealthResult(pipeline=name, status=status, metric=metric, violations=[])


def test_map_no_rules_returns_empty():
    mapper = Mapper()
    results = [make_result("etl-ingest")]
    assert mapper.map(results) == []


def test_map_exact_pattern_matches():
    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="etl-ingest", channel="slack"))
    outputs = mapper.map([make_result("etl-ingest")])
    assert len(outputs) == 1
    assert outputs[0].channel == "slack"
    assert outputs[0].pipeline == "etl-ingest"


def test_map_wildcard_pattern_matches():
    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="etl-*", channel="ops"))
    results = [make_result("etl-ingest"), make_result("etl-transform")]
    outputs = mapper.map(results)
    assert len(outputs) == 2
    assert all(o.channel == "ops" for o in outputs)


def test_map_non_matching_pattern_skipped():
    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="reporting-*", channel="email"))
    outputs = mapper.map([make_result("etl-ingest")])
    assert outputs == []


def test_map_uses_first_matching_rule():
    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="etl-*", channel="first"))
    mapper.add_rule(MapRule(pattern="etl-ingest", channel="second"))
    outputs = mapper.map([make_result("etl-ingest")])
    assert len(outputs) == 1
    assert outputs[0].channel == "first"


def test_default_payload_contains_status():
    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="*", channel="default"))
    outputs = mapper.map([make_result("pipe", PipelineStatus.CRITICAL)])
    assert outputs[0].payload["status"] == "critical"


def test_custom_transform_is_applied():
    def custom(result):
        return {"name": result.pipeline, "ok": result.status == PipelineStatus.HEALTHY}

    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="*", channel="custom", transform=custom))
    outputs = mapper.map([make_result("pipe", PipelineStatus.HEALTHY)])
    assert outputs[0].payload == {"name": "pipe", "ok": True}


def test_dispatch_calls_registered_handler():
    received = []
    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="*", channel="test"))
    mapper.register_channel("test", received.append)
    map_and_dispatch(mapper, [make_result("pipe")])
    assert len(received) == 1
    assert isinstance(received[0], MappedOutput)


def test_dispatch_skips_unregistered_channel():
    called = []
    mapper = Mapper()
    mapper.add_rule(MapRule(pattern="*", channel="nowhere"))
    mapper.register_channel("somewhere", called.append)
    map_and_dispatch(mapper, [make_result("pipe")])
    assert called == []


def test_mapped_output_str():
    output = MappedOutput(pipeline="etl", channel="slack", payload={"status": "healthy"})
    assert str(output) == "[slack] etl: healthy"
