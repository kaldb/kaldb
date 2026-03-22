#!/usr/bin/env python3
"""Browser verification for the enterprise fraud demo dashboard and Discover views."""

from __future__ import annotations

import argparse
import time
from urllib.parse import quote

from dashboards_ui_lib import (
    build_driver,
    dashboards_status,
    find_index_patterns,
    import_selenium,
    resolve_index,
    save_failure_screenshot,
)


def log(message: str) -> None:
    print(f"[enterprise-fraud-ui] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Open the enterprise fraud dashboard and saved searches in Dashboards "
            "and verify the expected UI state."
        )
    )
    parser.add_argument(
        "--dashboards-url",
        default="http://localhost:5601",
        help="Base URL for OpenSearch Dashboards (default: %(default)s)",
    )
    parser.add_argument(
        "--index",
        required=True,
        help="Exact enterprise demo dataset/index name",
    )
    parser.add_argument(
        "--dashboard-id",
        default="enterprise-fraud-investigation",
        help="Saved object id for the enterprise dashboard (default: %(default)s)",
    )
    parser.add_argument(
        "--conflicts-id",
        default="enterprise-fraud-conflict-examples",
        help="Saved object id for the conflict saved search (default: %(default)s)",
    )
    parser.add_argument(
        "--historical-id",
        default="enterprise-fraud-historical-window",
        help="Saved object id for the historical saved search (default: %(default)s)",
    )
    parser.add_argument(
        "--expect-historical",
        choices=("hidden", "visible"),
        default="hidden",
        help="Whether the historical saved search should still be empty or restored",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome headless instead of visibly",
    )
    parser.add_argument(
        "--window-size",
        default="1440,1400",
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
        default=40,
        help="Timeout for UI waits (default: %(default)s)",
    )
    parser.add_argument(
        "--keep-open-seconds",
        type=float,
        default=0.0,
        help="Keep the final browser open after success for this many seconds",
    )
    parser.add_argument(
        "--failure-screenshot",
        default="/tmp/enterprise-fraud-demo-ui-failure.png",
        help="Path to write a screenshot on failure",
    )
    return parser.parse_args()


def dashboard_url(base_url: str, dashboard_id: str) -> str:
    return f"{base_url}/app/dashboards#/view/{dashboard_id}"


def discover_url(base_url: str, saved_search_id: str) -> str:
    time_state = quote("(time:(from:'now-90d',to:'now+15m'))", safe="()':,")
    return f"{base_url}/app/discover#/view/{saved_search_id}?_g={time_state}"


def body_text(driver: object, by: object) -> str:
    return driver.find_element(by.TAG_NAME, "body").text  # type: ignore[attr-defined]


def wait_for_text(driver: object, wait: object, by: object, expected: list[str]) -> str:
    wait.until(  # type: ignore[attr-defined]
        lambda d: all(token in d.find_element(by.TAG_NAME, "body").text for token in expected)
    )
    return body_text(driver, by)


def assert_dashboard_ui(driver: object, dashboards_url: str, dashboard_id: str, wait: object) -> None:
    by = import_selenium()["By"]
    driver.get(dashboard_url(dashboards_url, dashboard_id))  # type: ignore[attr-defined]
    text = wait_for_text(
        driver,
        wait,
        by,
        [
            "Enterprise Fraud Investigation",
            "Fraud activity over time",
            "Events by business unit",
            "Visible windows",
            "Schema variants by producer",
            "Exposure by channel",
            "Field conflict examples",
            "Historical attack window",
        ],
    )
    if "Application Not Found" in text:
        raise RuntimeError("Dashboard URL did not resolve inside Dashboards")


def assert_conflicts_ui(
    driver: object, dashboards_url: str, saved_search_id: str, wait: object
) -> None:
    by = import_selenium()["By"]
    driver.get(discover_url(dashboards_url, saved_search_id))  # type: ignore[attr-defined]
    text = wait_for_text(
        driver,
        wait,
        by,
        [
            "Field conflict examples",
            "risk_score_keyword",
            "customer_tier_integer",
            "mfa_required_keyword",
            "schema_variant",
        ],
    )
    if "No results found" in text or "No documents found" in text:
        raise RuntimeError("Conflict saved search loaded without example documents")


def assert_historical_ui(
    driver: object,
    dashboards_url: str,
    saved_search_id: str,
    wait: object,
    expect_historical: str,
) -> None:
    by = import_selenium()["By"]
    driver.get(discover_url(dashboards_url, saved_search_id))  # type: ignore[attr-defined]

    wait.until(  # type: ignore[attr-defined]
        lambda d: "Historical attack window"
        in d.find_element(by.TAG_NAME, "body").text
    )

    empty_markers = [
        "No results found",
        "No documents found",
        "No results match your search criteria",
    ]
    visible_marker = "during historical investigation"
    deadline = time.time() + 40
    last_text = ""
    while time.time() < deadline:
        last_text = body_text(driver, by)
        has_empty = any(marker in last_text for marker in empty_markers)
        has_visible = visible_marker in last_text
        if expect_historical == "hidden" and has_empty:
            return
        if expect_historical == "hidden" and has_visible:
            raise RuntimeError("Historical saved search already shows restored documents")
        if expect_historical == "visible" and has_visible and not has_empty:
            return
        time.sleep(1)

    if expect_historical == "hidden":
        raise RuntimeError(
            "Historical saved search did not settle into the expected hidden state"
        )
    raise RuntimeError("Historical saved search did not show restored documents")


def main() -> int:
    args = parse_args()

    overall_state = dashboards_status(args.dashboards_url)
    log(f"Dashboards status: {overall_state}")
    if overall_state not in {"green", "available"}:
        log("Continuing despite non-green Dashboards status because this demo uses a custom gateway")

    resolved_indices = resolve_index(args.dashboards_url, args.index)
    log(f"resolve_index returned: {resolved_indices}")
    if args.index not in resolved_indices:
        raise RuntimeError(f"Dashboards does not currently resolve exact index {args.index!r}")

    matches = find_index_patterns(args.dashboards_url, args.index)
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one Dashboards data view titled {args.index!r}, got {len(matches)}"
        )
    log(f"Using data view id {matches[0]['id']}")

    selenium = import_selenium()
    driver = None
    try:
        driver = build_driver(
            headless=args.headless,
            window_size=args.window_size,
            chrome_binary=args.chrome_binary,
            chromedriver=args.chromedriver,
            selenium=selenium,
        )
        wait = selenium["WebDriverWait"](driver, args.timeout_seconds)

        log("Opening enterprise dashboard")
        assert_dashboard_ui(driver, args.dashboards_url, args.dashboard_id, wait)
        log("Dashboard loaded")

        log("Opening field conflict saved search")
        assert_conflicts_ui(driver, args.dashboards_url, args.conflicts_id, wait)
        log("Conflict saved search loaded")

        log(f"Opening historical saved search expecting it to be {args.expect_historical}")
        assert_historical_ui(
            driver,
            args.dashboards_url,
            args.historical_id,
            wait,
            args.expect_historical,
        )
        log("Historical saved search matched the expected state")

        if args.keep_open_seconds > 0:
            log(f"Keeping browser open for {args.keep_open_seconds:.1f}s")
            time.sleep(args.keep_open_seconds)
    except Exception:
        if driver is not None:
            save_failure_screenshot(driver, args.failure_screenshot, log)
        raise
    finally:
        if driver is not None:
            driver.quit()

    log("Enterprise fraud UI verification succeeded")
    log(f"Dashboard URL: {dashboard_url(args.dashboards_url, args.dashboard_id)}")
    log(f"Conflicts URL: {discover_url(args.dashboards_url, args.conflicts_id)}")
    log(f"Historical URL: {discover_url(args.dashboards_url, args.historical_id)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        log(f"ERROR: {error}")
        raise SystemExit(1)
