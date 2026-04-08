"""CLI entry point for pipewatch using argparse."""

import argparse
import logging
import sys

from pipewatch.watcher import run_once, run_loop


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on ETL pipeline health in real time.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to the pipewatch YAML config file (default: pipewatch.yaml).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between watch cycles when using --watch (default: 60).",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run continuously instead of a single check.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        if args.watch:
            run_loop(config_path=args.config, interval=args.interval)
        else:
            report = run_once(config_path=args.config)
            has_critical = any(
                a.get("level") == "critical"
                for a in report.get("alerts", [])
            )
            return 2 if has_critical else 0
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\npipewatch stopped.")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
