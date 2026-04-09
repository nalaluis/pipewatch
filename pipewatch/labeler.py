"""Tag-based labeling for pipeline results."""
from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Dict, List, Optional

from pipewatch.health import HealthResult


@dataclass
class LabelRule:
    """Maps a glob pattern to a set of labels."""
    pattern: str
    labels: List[str] = field(default_factory=list)


@dataclass
class LabeledResult:
    """A HealthResult decorated with computed labels."""
    result: HealthResult
    labels: List[str] = field(default_factory=list)

    @property
    def pipeline(self) -> str:
        return self.result.pipeline


def _matches_pattern(name: str, pattern: str) -> bool:
    return fnmatch(name, pattern)


def apply_labels(result: HealthResult, rules: List[LabelRule]) -> LabeledResult:
    """Apply all matching label rules to a single HealthResult."""
    labels: List[str] = []
    for rule in rules:
        if _matches_pattern(result.pipeline, rule.pattern):
            for lbl in rule.labels:
                if lbl not in labels:
                    labels.append(lbl)
    return LabeledResult(result=result, labels=labels)


def label_results(
    results: List[HealthResult],
    rules: List[LabelRule],
) -> List[LabeledResult]:
    """Apply label rules to a list of HealthResults."""
    return [apply_labels(r, rules) for r in results]


def parse_label_rules(raw: List[Dict]) -> List[LabelRule]:
    """Parse a list of dicts (e.g. from YAML) into LabelRule objects."""
    rules: List[LabelRule] = []
    for entry in raw:
        pattern = entry.get("pattern", "*")
        labels = entry.get("labels", [])
        rules.append(LabelRule(pattern=pattern, labels=list(labels)))
    return rules


def filter_by_label(
    labeled: List[LabeledResult],
    label: str,
) -> List[LabeledResult]:
    """Return only LabeledResults that carry the given label."""
    return [lr for lr in labeled if label in lr.labels]
