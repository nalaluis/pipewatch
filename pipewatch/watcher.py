"""Watcher module: orchestrates metric collection, health evaluation, alerting, and reporting."""

import time
import logging
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.health import evaluate_health, HealthResult
from pipewatch.alerts import build_alerts, emit_alerts, Alert
from pipewatch.reporter import build_report, emit_report
from pipewatch.config import load_config, parse_thresholds, get_pipelines

logger = logging.getLogger(__name__)


def run_once(config_path: str = "pipewatch.yaml") -> dict:
    """Run a single watch cycle: load config, evaluate health, emit alerts and report."""
    config = load_config(config_path)
    thresholds = parse_thresholds(config)
    pipelines = get_pipelines(config)

    results: List[HealthResult] = []
    all_alerts: List[Alert] = []

    for pipeline in pipelines:
        metric = _collect_metric(pipeline)
        if metric is None:
            logger.warning("No metric collected for pipeline: %s", pipeline)
            continue
        result = evaluate_health(metric, thresholds)
        alerts = build_alerts(result)
        results.append(result)
        all_alerts.extend(alerts)

    emit_alerts(all_alerts)
    report = build_report(results, all_alerts)
    emit_report(report)
    return report


def run_loop(config_path: str = "pipewatch.yaml", interval: int = 60) -> None:
    """Continuously run watch cycles at the given interval (seconds)."""
    logger.info("Starting pipewatch loop (interval=%ds)", interval)
    while True:
        try:
            run_once(config_path)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Watch cycle failed: %s", exc)
        time.sleep(interval)


def _collect_metric(pipeline: dict) -> Optional[PipelineMetric]:
    """Stub: collect a PipelineMetric for the given pipeline config entry."""
    try:
        from pipewatch.metrics import PipelineMetric, PipelineStatus
        return PipelineMetric(
            pipeline_id=pipeline.get("id", "unknown"),
            total=pipeline.get("_mock_total", 100),
            failed=pipeline.get("_mock_failed", 0),
            status=PipelineStatus(pipeline.get("_mock_status", "ok")),
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed to collect metric for %s: %s", pipeline, exc)
        return None
