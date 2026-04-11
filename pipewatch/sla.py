"""SLA (Service Level Agreement) tracking for pipeline health."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


@dataclass
class SLAConfig:
    max_failure_rate: float = 0.05       # 5% max failures allowed
    min_throughput: float = 1.0          # minimum records/sec
    max_downtime_seconds: float = 300.0  # 5 minutes max downtime


@dataclass
class SLAViolation:
    pipeline: str
    rule: str
    detail: str
    violated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __str__(self) -> str:
        ts = self.violated_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"[{ts}] SLA violation on '{self.pipeline}': {self.rule} — {self.detail}"


@dataclass
class SLAResult:
    pipeline: str
    compliant: bool
    violations: List[SLAViolation] = field(default_factory=list)


_downtime_registry: Dict[str, datetime] = {}


def check_sla(result: HealthResult, config: SLAConfig) -> SLAResult:
    """Evaluate SLA compliance for a single pipeline health result."""
    violations: List[SLAViolation] = []
    name = result.pipeline
    metric = result.metric

    if metric.failure_rate > config.max_failure_rate:
        violations.append(SLAViolation(
            pipeline=name,
            rule="max_failure_rate",
            detail=f"failure_rate={metric.failure_rate:.3f} exceeds limit={config.max_failure_rate:.3f}",
        ))

    if metric.throughput < config.min_throughput:
        violations.append(SLAViolation(
            pipeline=name,
            rule="min_throughput",
            detail=f"throughput={metric.throughput:.2f} below minimum={config.min_throughput:.2f}",
        ))

    now = datetime.now(timezone.utc)
    if result.metric.status == PipelineStatus.CRITICAL:
        if name not in _downtime_registry:
            _downtime_registry[name] = now
        else:
            elapsed = (now - _downtime_registry[name]).total_seconds()
            if elapsed > config.max_downtime_seconds:
                violations.append(SLAViolation(
                    pipeline=name,
                    rule="max_downtime_seconds",
                    detail=f"pipeline down for {elapsed:.0f}s, limit={config.max_downtime_seconds:.0f}s",
                ))
    else:
        _downtime_registry.pop(name, None)

    return SLAResult(pipeline=name, compliant=len(violations) == 0, violations=violations)


def check_all_slas(
    results: List[HealthResult],
    config: Optional[SLAConfig] = None,
) -> List[SLAResult]:
    """Evaluate SLA compliance across all pipeline results."""
    cfg = config or SLAConfig()
    return [check_sla(r, cfg) for r in results]


def format_sla_report(sla_results: List[SLAResult]) -> str:
    """Render a human-readable SLA compliance report."""
    lines = ["=== SLA Compliance Report ==="]
    for sr in sla_results:
        status = "OK" if sr.compliant else "VIOLATION"
        lines.append(f"  [{status}] {sr.pipeline}")
        for v in sr.violations:
            lines.append(f"      - {v.rule}: {v.detail}")
    return "\n".join(lines)
