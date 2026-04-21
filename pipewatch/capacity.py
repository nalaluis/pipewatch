"""Capacity planning module: tracks pipeline load and flags over/under-capacity states."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class CapacityConfig:
    max_throughput: float = 1000.0   # records/sec considered full capacity
    warn_pct: float = 0.80           # warn when throughput >= 80% of max
    critical_pct: float = 0.95       # critical when throughput >= 95% of max
    min_throughput: float = 0.0      # 0 disables under-capacity check
    enabled: bool = True


@dataclass
class CapacityResult:
    pipeline: str
    throughput: float
    max_throughput: float
    utilization: float               # 0.0 – 1.0
    status: str                      # "ok" | "warning" | "critical" | "under"
    message: str

    def __str__(self) -> str:
        pct = round(self.utilization * 100, 1)
        return f"[{self.status.upper()}] {self.pipeline}: {pct}% capacity ({self.throughput:.1f}/{self.max_throughput:.1f} rec/s) — {self.message}"


def check_capacity(metric: PipelineMetric, cfg: CapacityConfig) -> Optional[CapacityResult]:
    """Return a CapacityResult for *metric* or None when capacity checks are disabled."""
    if not cfg.enabled:
        return None

    tp = metric.throughput
    util = tp / cfg.max_throughput if cfg.max_throughput > 0 else 0.0

    if util >= cfg.critical_pct:
        status = "critical"
        msg = f"throughput exceeds {int(cfg.critical_pct * 100)}% of max capacity"
    elif util >= cfg.warn_pct:
        status = "warning"
        msg = f"throughput exceeds {int(cfg.warn_pct * 100)}% of max capacity"
    elif cfg.min_throughput > 0 and tp < cfg.min_throughput:
        status = "under"
        msg = f"throughput below minimum threshold ({cfg.min_throughput:.1f} rec/s)"
    else:
        status = "ok"
        msg = "within normal capacity range"

    return CapacityResult(
        pipeline=metric.pipeline,
        throughput=tp,
        max_throughput=cfg.max_throughput,
        utilization=util,
        status=status,
        message=msg,
    )


def check_all_capacity(
    metrics: List[PipelineMetric], cfg: CapacityConfig
) -> List[CapacityResult]:
    """Run capacity checks across all metrics, returning only non-None results."""
    results = []
    for m in metrics:
        r = check_capacity(m, cfg)
        if r is not None:
            results.append(r)
    return results
