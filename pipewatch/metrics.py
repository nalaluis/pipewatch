"""Core metrics data structures for pipeline health monitoring."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class PipelineStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class PipelineMetric:
    """Represents a single pipeline health metric snapshot."""

    pipeline_id: str
    rows_processed: int
    rows_failed: int
    duration_seconds: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None

    @property
    def failure_rate(self) -> float:
        """Return the ratio of failed rows to total rows."""
        total = self.rows_processed + self.rows_failed
        if total == 0:
            return 0.0
        return self.rows_failed / total

    @property
    def throughput(self) -> float:
        """Return rows processed per second."""
        if self.duration_seconds <= 0:
            return 0.0
        return self.rows_processed / self.duration_seconds

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "rows_processed": self.rows_processed,
            "rows_failed": self.rows_failed,
            "duration_seconds": self.duration_seconds,
            "failure_rate": round(self.failure_rate, 4),
            "throughput": round(self.throughput, 2),
            "timestamp": self.timestamp.isoformat(),
            "error_message": self.error_message,
        }
