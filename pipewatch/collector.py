"""Metric collector: batch-collects pipeline metrics and applies pre-processing."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class CollectorConfig:
    max_batch_size: int = 50
    skip_unknown: bool = False
    default_status: PipelineStatus = PipelineStatus.HEALTHY


@dataclass
class CollectionResult:
    collected: List[PipelineMetric] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.collected)

    @property
    def success_rate(self) -> float:
        attempted = self.total + len(self.skipped) + len(self.errors)
        if attempted == 0:
            return 1.0
        return self.total / attempted

    def __str__(self) -> str:
        return (
            f"CollectionResult(collected={self.total}, "
            f"skipped={len(self.skipped)}, errors={len(self.errors)})"
        )


def collect_metrics(
    pipeline_names: List[str],
    fetch_fn: Callable[[str], Optional[PipelineMetric]],
    config: Optional[CollectorConfig] = None,
) -> CollectionResult:
    """Collect metrics for a list of pipeline names using a fetch function."""
    cfg = config or CollectorConfig()
    result = CollectionResult()

    for name in pipeline_names[: cfg.max_batch_size]:
        try:
            metric = fetch_fn(name)
            if metric is None:
                if cfg.skip_unknown:
                    result.skipped.append(name)
                else:
                    result.errors.append(f"{name}: fetch returned None")
            else:
                result.collected.append(metric)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"{name}: {exc}")

    return result


def filter_by_status(
    metrics: List[PipelineMetric],
    statuses: List[PipelineStatus],
) -> List[PipelineMetric]:
    """Return only metrics whose status is in *statuses*."""
    return [m for m in metrics if m.status in statuses]
