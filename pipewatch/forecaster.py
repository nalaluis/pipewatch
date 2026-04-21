"""Simple linear-trend forecaster for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import HistoryEntry


@dataclass
class ForecastResult:
    pipeline: str
    metric: str  # 'failure_rate' or 'throughput'
    predicted_value: float
    confidence: float        # 0.0 – 1.0 based on sample size
    trend_slope: float       # change per step
    steps_ahead: int

    def __str__(self) -> str:
        direction = "up" if self.trend_slope > 0 else "down" if self.trend_slope < 0 else "flat"
        return (
            f"[{self.pipeline}] {self.metric} forecast ({self.steps_ahead} step(s) ahead): "
            f"{self.predicted_value:.4f} | slope={self.trend_slope:+.4f} ({direction}) "
            f"| confidence={self.confidence:.2f}"
        )


def _least_squares_slope(values: List[float]) -> float:
    """Return slope of the best-fit line through evenly-spaced values."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    return numerator / denominator if denominator else 0.0


def _confidence(n: int, min_samples: int = 3, max_samples: int = 20) -> float:
    if n <= min_samples:
        return 0.0
    return min(1.0, (n - min_samples) / (max_samples - min_samples))


def forecast(
    history: List[HistoryEntry],
    pipeline: str,
    steps_ahead: int = 1,
) -> Optional[List[ForecastResult]]:
    """Produce failure_rate and throughput forecasts for *pipeline*."""
    entries = [e for e in history if e.pipeline == pipeline]
    if not entries:
        return None

    failure_rates = [e.failure_rate for e in entries]
    throughputs = [e.throughput for e in entries]

    results: List[ForecastResult] = []
    for metric, values in (("failure_rate", failure_rates), ("throughput", throughputs)):
        slope = _least_squares_slope(values)
        last = values[-1]
        predicted = last + slope * steps_ahead
        conf = _confidence(len(values))
        results.append(
            ForecastResult(
                pipeline=pipeline,
                metric=metric,
                predicted_value=max(0.0, predicted),
                confidence=conf,
                trend_slope=slope,
                steps_ahead=steps_ahead,
            )
        )
    return results
