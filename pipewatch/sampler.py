"""Metric sampling: collect a rolling window of recent metric snapshots per pipeline."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class SamplerConfig:
    window_size: int = 10  # max samples retained per pipeline
    min_samples: int = 2   # minimum samples before stats are meaningful


@dataclass
class SampleWindow:
    pipeline: str
    samples: Deque[PipelineMetric] = field(default_factory=deque)

    def add(self, metric: PipelineMetric, max_size: int) -> None:
        self.samples.append(metric)
        while len(self.samples) > max_size:
            self.samples.popleft()

    def latest(self) -> Optional[PipelineMetric]:
        return self.samples[-1] if self.samples else None

    def as_list(self) -> List[PipelineMetric]:
        return list(self.samples)

    def __len__(self) -> int:
        return len(self.samples)


class MetricSampler:
    """Maintains a rolling window of PipelineMetric samples per pipeline."""

    def __init__(self, config: Optional[SamplerConfig] = None) -> None:
        self._config = config or SamplerConfig()
        self._windows: Dict[str, SampleWindow] = {}

    def record(self, metric: PipelineMetric) -> None:
        name = metric.pipeline
        if name not in self._windows:
            self._windows[name] = SampleWindow(pipeline=name)
        self._windows[name].add(metric, self._config.window_size)

    def window(self, pipeline: str) -> Optional[SampleWindow]:
        return self._windows.get(pipeline)

    def has_enough_samples(self, pipeline: str) -> bool:
        w = self._windows.get(pipeline)
        return w is not None and len(w) >= self._config.min_samples

    def all_pipelines(self) -> List[str]:
        return list(self._windows.keys())

    def reset(self, pipeline: str) -> None:
        self._windows.pop(pipeline, None)

    def clear(self) -> None:
        self._windows.clear()
