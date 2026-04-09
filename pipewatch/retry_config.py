"""Load retry policy settings from pipewatch-retry.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.retry import RetryPolicy, make_retry_policy

_DEFAULT_PATH = Path("pipewatch-retry.yaml")


def load_retry_config(path: Path = _DEFAULT_PATH) -> RetryPolicy:
    """Return a :class:`RetryPolicy` parsed from *path*.

    Falls back to library defaults when the file is absent or the
    ``retry`` key is missing.
    """
    if not path.exists():
        return make_retry_policy()

    with path.open() as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}

    retry_data = data.get("retry", {})
    return make_retry_policy(
        max_attempts=int(retry_data.get("max_attempts", 3)),
        delay_seconds=float(retry_data.get("delay_seconds", 1.0)),
        backoff_factor=float(retry_data.get("backoff_factor", 2.0)),
    )
