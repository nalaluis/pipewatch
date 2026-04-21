"""Maps pipeline results to structured output channels by name pattern."""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.health import HealthResult


@dataclass
class MapRule:
    pattern: str
    channel: str
    transform: Optional[Callable[[HealthResult], dict]] = field(default=None, repr=False)


@dataclass
class MappedOutput:
    pipeline: str
    channel: str
    payload: dict

    def __str__(self) -> str:
        return f"[{self.channel}] {self.pipeline}: {self.payload.get('status', '?')}"


class Mapper:
    def __init__(self) -> None:
        self._rules: List[MapRule] = []
        self._channels: Dict[str, Callable[[MappedOutput], None]] = {}

    def add_rule(self, rule: MapRule) -> None:
        self._rules.append(rule)

    def register_channel(self, name: str, handler: Callable[[MappedOutput], None]) -> None:
        self._channels[name] = handler

    def map(self, results: List[HealthResult]) -> List[MappedOutput]:
        outputs: List[MappedOutput] = []
        for result in results:
            for rule in self._rules:
                if fnmatch.fnmatch(result.pipeline, rule.pattern):
                    payload = _default_transform(result)
                    if rule.transform is not None:
                        payload = rule.transform(result)
                    outputs.append(MappedOutput(
                        pipeline=result.pipeline,
                        channel=rule.channel,
                        payload=payload,
                    ))
                    break
        return outputs

    def dispatch(self, outputs: List[MappedOutput]) -> None:
        for output in outputs:
            handler = self._channels.get(output.channel)
            if handler is not None:
                handler(output)


def _default_transform(result: HealthResult) -> dict:
    return {
        "pipeline": result.pipeline,
        "status": result.status.value,
        "failure_rate": result.metric.failure_rate if result.metric else None,
        "throughput": result.metric.throughput if result.metric else None,
    }


def map_and_dispatch(mapper: Mapper, results: List[HealthResult]) -> List[MappedOutput]:
    outputs = mapper.map(results)
    mapper.dispatch(outputs)
    return outputs
