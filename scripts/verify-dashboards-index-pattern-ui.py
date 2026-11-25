#!/usr/bin/env python3
"""Drive the OpenSearch Dashboards index-pattern creation UI with Selenium."""

from __future__ import annotations

import argparse
from dashboards_ui_lib import (
    dashboards_status,
    find_index_patterns,
    resolve_index,
    run_index_pattern_creation_workflow,
)


def log(message: str) -> None:
    print(f"[dashboards-ui] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create an OpenSearch Dashboards index pattern/data view through the UI "
            "using Selenium."
        )
    )
    parser.add_argument(
        "--dashboards-url",
        default="http://localhost:5601",
        help="Base URL for OpenSearch Dashboards (default: %(default)s)",
    )
    parser.add_argument(
        "--index-pattern",
        default="ui-e2e-a",
        help="Exact existing index name/dataset name to create in Dashboards",
    )
    parser.add_argument(
        "--time-field",
        default="@timestamp",
        help="Time field to select on step 2 (default: %(default)s)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome headless instead of visibly",
    )
    parser.add_argument(
        "--delete-existing",
        action="store_true",
        help="Delete existing index-pattern saved objects with the same title first",
    )
    parser.add_argument(
        "--slow-seconds",
        type=float,
        default=0.0,
        help="Pause this many seconds between major UI actions",
    )
    parser.add_argument(
        "--keep-open-seconds",
        type=float,
        default=0.0,
        help="Keep the browser open for this many seconds after success",
    )
    parser.add_argument(
        "--window-size",
        default="1440,1200",
        help="Chrome window size as WIDTH,HEIGHT (default: %(default)s)",
    )
    parser.add_argument(
        "--chrome-binary",
        default="",
        help="Optional explicit path to the Chrome binary",
    )
    parser.add_argument(
        "--chromedriver",
        default="",
        help="Optional explicit path to ChromeDriver",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="Timeout for UI waits (default: %(default)s)",
    )
    parser.add_argument(
        "--failure-screenshot",
        default="/tmp/dashboards-index-pattern-ui-failure.png",
        help="Path to write a screenshot on failure",
    )
    parser.add_argument(
        "--precheck-only",
        action="store_true",
        help="Only run the HTTP prechecks and duplicate check, not Selenium",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.precheck_only:
        if "*" in args.index_pattern:
            log("Wildcard index patterns are intentionally not supported by this script.")
            return 2
        overall_state = dashboards_status(args.dashboards_url)
        log(f"Dashboards status: {overall_state}")
        if overall_state not in {"green", "available"}:
            raise RuntimeError(
                f"Dashboards is not ready; /api/status reported {overall_state!r}"
            )
        resolved_indices = resolve_index(args.dashboards_url, args.index_pattern)
        log(f"resolve_index returned: {resolved_indices}")
        if args.index_pattern not in resolved_indices:
            raise RuntimeError(
                f"Dashboards does not currently resolve exact index {args.index_pattern!r}"
            )
        existing = find_index_patterns(args.dashboards_url, args.index_pattern)
        if existing:
            log(
                "Existing saved object id(s): "
                + ", ".join(obj.get("id", "<unknown>") for obj in existing)
            )
        log("Prechecks succeeded")
        return 0

    run_index_pattern_creation_workflow(
        dashboards_url=args.dashboards_url,
        index_pattern=args.index_pattern,
        time_field=args.time_field,
        delete_existing=args.delete_existing,
        headless=args.headless,
        window_size=args.window_size,
        chrome_binary=args.chrome_binary,
        chromedriver=args.chromedriver,
        timeout_seconds=args.timeout_seconds,
        slow_seconds=args.slow_seconds,
        keep_open_seconds=args.keep_open_seconds,
        failure_screenshot=args.failure_screenshot,
        logger=log,
    )
    log("UI flow succeeded")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        log(f"ERROR: {error}")
        raise SystemExit(1)
