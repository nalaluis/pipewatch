"""Tests for pipewatch.cli argument parsing and exit codes."""

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.cli import build_parser, main


def test_build_parser_defaults():
    parser = build_parser()
    args = parser.parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.interval == 60
    assert args.watch is False
    assert args.verbose is False


def test_build_parser_custom_args():
    parser = build_parser()
    args = parser.parse_args(["--config", "custom.yaml", "--interval", "30", "--watch", "--verbose"])
    assert args.config == "custom.yaml"
    assert args.interval == 30
    assert args.watch is True
    assert args.verbose is True


def test_main_returns_zero_on_healthy_report():
    healthy_report = {"pipelines": [], "alerts": [], "generated_at": "2024-01-01T00:00:00"}
    with patch("pipewatch.cli.run_once", return_value=healthy_report):
        code = main(["--config", "fake.yaml"])
    assert code == 0


def test_main_returns_two_on_critical_alert():
    critical_report = {
        "pipelines": [],
        "alerts": [{"level": "critical", "message": "failure rate critical"}],
        "generated_at": "2024-01-01T00:00:00",
    }
    with patch("pipewatch.cli.run_once", return_value=critical_report):
        code = main(["--config", "fake.yaml"])
    assert code == 2


def test_main_returns_one_on_file_not_found():
    with patch("pipewatch.cli.run_once", side_effect=FileNotFoundError("no such file")):
        code = main(["--config", "missing.yaml"])
    assert code == 1


def test_main_watch_calls_run_loop():
    with patch("pipewatch.cli.run_loop", side_effect=KeyboardInterrupt) as mock_loop:
        code = main(["--watch", "--interval", "10"])
    mock_loop.assert_called_once_with(config_path="pipewatch.yaml", interval=10)
    assert code == 0
