#!/usr/bin/env python3
"""
End-to-end Selenium smoke test for the Astra admin UI.

This script expects a manager instance with the admin UI already running.

Example:
  python3 tools/admin_ui_selenium_test.py --base-url http://127.0.0.1:8080/admin/

Requirements:
  pip install selenium

Optional environment variables:
  CHROME_BINARY   Override the Chrome/Chromium binary path
  CHROMEDRIVER    Override the ChromeDriver path
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4


DEFAULT_TIMEOUT_SECONDS = 20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8080/admin/",
        help="Admin UI URL. If you pass the host root, /admin/ is appended automatically.",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run Chrome in headless mode (default: true).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Explicit wait timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--chrome-binary",
        default=os.environ.get("CHROME_BINARY"),
        help="Path to the Chrome/Chromium binary.",
    )
    parser.add_argument(
        "--chromedriver",
        default=os.environ.get("CHROMEDRIVER"),
        help="Path to ChromeDriver. Defaults to PATH or ~/.cache/selenium/chromedriver/**/chromedriver.",
    )
    parser.add_argument(
        "--screenshot-dir",
        default="tmp/admin-ui-selenium",
        help="Directory for failure screenshots (default: tmp/admin-ui-selenium).",
    )
    return parser.parse_args()


def normalize_admin_url(raw_url: str) -> str:
    split = urlsplit(raw_url)
    path = split.path or "/"
    if path in ("", "/"):
        path = "/admin/"
    elif path.endswith("/admin"):
        path += "/"
    elif not path.endswith("/"):
        path += "/"
    return urlunsplit((split.scheme, split.netloc, path, split.query, split.fragment))


def fail(message: str) -> None:
    raise AssertionError(message)


def log_step(message: str) -> None:
    print(f"[admin-ui] {message}", flush=True)


def load_selenium() -> tuple:
    try:
        from selenium import webdriver
        from selenium.common.exceptions import (
            ElementClickInterceptedException,
            StaleElementReferenceException,
            TimeoutException,
        )
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import Select, WebDriverWait
    except ModuleNotFoundError as exc:
        print(
            "selenium is not installed. Install it with: pip install selenium",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc

    return (
        webdriver,
        ElementClickInterceptedException,
        StaleElementReferenceException,
        TimeoutException,
        Options,
        Service,
        By,
        EC,
        Select,
        WebDriverWait,
    )


def resolve_chrome_binary(explicit_path: str | None) -> str | None:
    if explicit_path:
        return explicit_path

    for candidate in ("google-chrome", "chromium", "chromium-browser"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def resolve_chromedriver(explicit_path: str | None) -> str:
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if path.is_file():
            return str(path)
        fail(f"ChromeDriver not found at {path}")

    path_candidate = shutil.which("chromedriver")
    if path_candidate:
        return path_candidate

    cached = sorted(
        Path.home().glob(".cache/selenium/chromedriver/*/*/chromedriver"),
        key=lambda item: str(item),
    )
    if cached:
        return str(cached[-1])

    fail(
        "Could not find ChromeDriver. Set --chromedriver or CHROMEDRIVER, "
        "or install ChromeDriver locally."
    )


def make_driver(args: argparse.Namespace):
    webdriver, _, _, _, Options, Service, _, _, _, _ = load_selenium()

    chrome_binary = resolve_chrome_binary(args.chrome_binary)
    chromedriver = resolve_chromedriver(args.chromedriver)

    options = Options()
    options.add_argument("--window-size=1440,1200")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    if args.headless:
        options.add_argument("--headless=new")
    if chrome_binary:
        options.binary_location = chrome_binary

    service = Service(executable_path=chromedriver)
    return webdriver.Chrome(service=service, options=options)


def run_test(args: argparse.Namespace) -> None:
    (
        _,
        ElementClickInterceptedException,
        StaleElementReferenceException,
        TimeoutException,
        _,
        _,
        By,
        EC,
        Select,
        WebDriverWait,
    ) = load_selenium()
    driver = make_driver(args)
    wait = WebDriverWait(driver, args.timeout)

    base_url = normalize_admin_url(args.base_url)
    suffix = uuid4().hex
    dataset_name = f"selenium-dataset-{suffix}"
    dataset_owner = f"selenium-owner-{suffix}"
    updated_owner = f"selenium-owner-updated-{suffix}"
    service_pattern = f"svc-{suffix}*"
    updated_service_pattern = f"svc-updated-{suffix}*"
    redaction_name = f"selenium-redaction-{suffix}"
    field_name = f"field_{suffix}"
    partition_seed = int(uuid4().hex[:8], 16) % 100000000
    initial_partition_ids = [str(partition_seed), str(partition_seed + 1)]
    updated_partition_ids = [str(partition_seed + 2), str(partition_seed + 3)]
    all_partition_ids = initial_partition_ids + updated_partition_ids

    screenshot_dir = Path(args.screenshot_dir)
    screenshot_dir.mkdir(parents=True, exist_ok=True)

    def wait_for_toast(text: str) -> None:
        text_lower = text.lower()

        def has_toast_text() -> bool:
            for toast in driver.find_elements(By.CSS_SELECTOR, "#toast-container .toast"):
                try:
                    if text_lower in toast.text.lower():
                        return True
                except StaleElementReferenceException:
                    continue
            return False

        wait.until(lambda d: has_toast_text())

    def wait_for_toasts_to_clear() -> None:
        def has_visible_toasts() -> bool:
            for toast in driver.find_elements(By.CSS_SELECTOR, "#toast-container .toast"):
                try:
                    if toast.is_displayed():
                        return True
                except StaleElementReferenceException:
                    continue
            return False

        wait.until(lambda d: not has_visible_toasts())

    def current_grid_page_marker(d, grid_id: str) -> tuple[str, str]:
        try:
            current_buttons = d.find_elements(
                By.CSS_SELECTOR,
                f"#{grid_id} .gridjs-pagination .gridjs-pages button.gridjs-currentPage",
            )
            current_page = current_buttons[0].text if current_buttons else ""
            rows = d.find_elements(By.CSS_SELECTOR, f"#{grid_id} tbody tr")
            first_row_text = rows[0].text if rows else ""
            return current_page, first_row_text
        except StaleElementReferenceException:
            return "", ""

    def current_grid_row(d, grid_id: str, text: str):
        for row in d.find_elements(By.CSS_SELECTOR, f"#{grid_id} tbody tr"):
            try:
                if text in row.text:
                    return row
            except StaleElementReferenceException:
                continue
        return None

    def grid_edge_button(d, grid_id: str, *, next_page: bool):
        buttons = d.find_elements(
            By.CSS_SELECTOR,
            f"#{grid_id} .gridjs-pagination .gridjs-pages button",
        )
        if len(buttons) < 2:
            return None
        return buttons[-1] if next_page else buttons[0]

    def move_grid_to_first_page(grid_id: str) -> None:
        while True:
            try:
                prev_button = grid_edge_button(driver, grid_id, next_page=False)
                if prev_button is None or not prev_button.is_enabled():
                    return
                before = current_grid_page_marker(driver, grid_id)
                prev_button.click()
            except StaleElementReferenceException:
                continue
            wait.until(lambda d, b=before: current_grid_page_marker(d, grid_id) != b)

    def find_grid_row(grid_id: str, text: str):
        move_grid_to_first_page(grid_id)
        while True:
            row = current_grid_row(driver, grid_id, text)
            if row is not None:
                return row

            try:
                next_button = grid_edge_button(driver, grid_id, next_page=True)
                if next_button is None or not next_button.is_enabled():
                    return None
                before = current_grid_page_marker(driver, grid_id)
                next_button.click()
            except StaleElementReferenceException:
                continue
            wait.until(lambda d, b=before: current_grid_page_marker(d, grid_id) != b)

    def wait_for_grid_row(grid_id: str, text: str) -> None:
        wait.until(lambda d: find_grid_row(grid_id, text) is not None)

    def wait_for_grid_row_text(grid_id: str, row_text: str, expected_text: str) -> None:
        expected_text_lower = expected_text.lower()

        def row_contains_text() -> bool:
            row = find_grid_row(grid_id, row_text)
            if row is None:
                return False
            try:
                return expected_text_lower in row.text.lower()
            except StaleElementReferenceException:
                return False

        wait.until(lambda d: row_contains_text())

    def wait_for_grid_row_absent(grid_id: str, text: str) -> None:
        wait.until(lambda d: find_grid_row(grid_id, text) is None)

    def assert_grid_header_tooltip(grid_id: str, label: str, expected_text: str) -> None:
        label_lower = label.lower()

        def matching_header():
            for header in driver.find_elements(By.CSS_SELECTOR, f"#{grid_id} th.gridjs-th"):
                try:
                    aria_label = header.get_attribute("aria-label") or ""
                    visible_label = header.text.strip().split("\n")[0]
                    if (
                        aria_label.lower().startswith(f"{label_lower}:")
                        or visible_label.lower() == label_lower
                    ):
                        return header
                except StaleElementReferenceException:
                    continue
            return None

        header = wait.until(
            lambda d: matching_header(),
            message=f"Could not find {label!r} header in {grid_id}",
        )
        tooltip = header.get_attribute("title") or ""
        if expected_text not in tooltip:
            fail(f"{grid_id} {label!r} tooltip {tooltip!r} did not contain {expected_text!r}")

    def wait_for_dataset_history_rows(min_rows: int) -> None:
        wait.until(
            lambda d: len(
                d.find_elements(By.CSS_SELECTOR, "#dataset-assignment-history tbody tr")
            )
            >= min_rows
        )

    def click_grid_row_action(grid_id: str, row_text: str, action: str) -> None:
        def resolve_action_button():
            row = find_grid_row(grid_id, row_text)
            if row is None:
                return None
            try:
                return row.find_element(By.CSS_SELECTOR, f"[data-action='{action}']")
            except StaleElementReferenceException:
                return None

        for _ in range(3):
            action_button = wait.until(
                lambda d: resolve_action_button(),
                message=f"Could not find row containing {row_text!r} in {grid_id}",
            )
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", action_button)
                action_button.click()
                return
            except StaleElementReferenceException:
                continue
            except ElementClickInterceptedException:
                wait_for_toasts_to_clear()

        action_button = wait.until(
            lambda d: resolve_action_button(),
            message=f"Could not re-find row containing {row_text!r} in {grid_id}",
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", action_button)
        action_button.click()

    def click(locator: tuple[str, str]) -> None:
        for _ in range(3):
            try:
                wait.until(EC.element_to_be_clickable(locator)).click()
                return
            except StaleElementReferenceException:
                continue
            except ElementClickInterceptedException:
                wait_for_toasts_to_clear()
        wait.until(EC.element_to_be_clickable(locator)).click()

    def create_partition(partition_id: str, max_capacity: str = "2000") -> None:
        click((By.ID, "btn-new-partition"))
        wait.until(
            lambda d: "open"
            in d.find_element(By.ID, "modal-create-partition").get_attribute("class")
        )
        driver.find_element(
            By.CSS_SELECTOR, "#form-create-partition [name='partition_id']"
        ).send_keys(partition_id)
        driver.find_element(
            By.CSS_SELECTOR, "#form-create-partition [name='max_capacity']"
        ).send_keys(max_capacity)
        click((By.CSS_SELECTOR, "#form-create-partition button[type='submit']"))
        wait_for_toast("Partition created")
        click((By.ID, "btn-refresh-partitions"))
        wait_for_grid_row("partitions-grid", partition_id)
        wait_for_grid_row_text("partitions-grid", partition_id, "Empty")

    def select_value(locator: tuple[str, str], value: str) -> None:
        element = wait.until(EC.presence_of_element_located(locator))
        Select(element).select_by_value(value)

    def capture_failure(name: str) -> None:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        target = screenshot_dir / f"{timestamp}-{name}.png"
        driver.save_screenshot(str(target))
        print(f"Saved failure screenshot to {target}", file=sys.stderr)

    try:
        log_step(f"Opening {base_url}")
        driver.get(base_url)

        wait.until(EC.visibility_of_element_located((By.ID, "btn-new-dataset")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#datasets-grid .gridjs-container")))
        assert_grid_header_tooltip("datasets-grid", "Throughput", "Allowed ingest throughput")
        assert_grid_header_tooltip("datasets-grid", "History", "Number of assignment windows")

        catalog_supported = False
        log_step("Checking partition catalog support")
        click((By.CSS_SELECTOR, ".tab[data-tab='partitions']"))
        wait.until(lambda d: d.find_element(By.ID, "partitions").is_displayed())
        wait.until(
            lambda d: d.find_element(By.ID, "partitions-empty-state").is_displayed()
            or d.find_elements(By.CSS_SELECTOR, "#partitions-grid .gridjs-container")
        )
        empty_state = driver.find_element(By.ID, "partitions-empty-state")
        if empty_state.is_displayed():
            empty_title = empty_state.find_element(By.TAG_NAME, "h3").text.strip()
            catalog_supported = empty_title != "Partition catalog unavailable"
        else:
            catalog_supported = True

        if not catalog_supported:
            fail("Dataset CRUD quota flow requires partition catalog support")

        assert_grid_header_tooltip("partitions-grid", "Available", "Unreserved capacity")
        assert_grid_header_tooltip("partitions-grid", "Occupancy", "Current catalog state")

        log_step("Creating catalog partitions")
        for partition_id in initial_partition_ids:
            create_partition(partition_id)

        log_step("Creating dataset with quota")
        click((By.CSS_SELECTOR, ".tab[data-tab='datasets']"))
        wait.until(lambda d: d.find_element(By.ID, "datasets").is_displayed())
        click((By.ID, "btn-new-dataset"))
        wait.until(lambda d: d.find_element(By.ID, "dataset-form-page").is_displayed())
        name_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='name']")
        owner_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='owner']")
        pattern_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='service_name_pattern']")
        throughput_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='throughput_bytes']")
        name_input.clear()
        name_input.send_keys(dataset_name)
        owner_input.clear()
        owner_input.send_keys(dataset_owner)
        pattern_input.clear()
        pattern_input.send_keys(service_pattern)
        throughput_input.clear()
        throughput_input.send_keys("1000")
        select_value((By.CSS_SELECTOR, "#form-dataset [name='partition_mode']"), "dedicated")
        select_value((By.CSS_SELECTOR, "#form-dataset [name='assignment_strategy']"), "auto")
        click((By.CSS_SELECTOR, "#form-dataset button[type='submit']"))
        wait_for_toast("Dataset created")
        wait_for_grid_row("datasets-grid", dataset_name)
        wait_for_grid_row_text("datasets-grid", dataset_name, dataset_owner)
        wait_for_grid_row_text("datasets-grid", dataset_name, service_pattern)
        wait_for_grid_row_text("datasets-grid", dataset_name, "1000")
        wait_for_grid_row_text("datasets-grid", dataset_name, "Dedicated")

        log_step("Creating replacement catalog partition")
        click((By.CSS_SELECTOR, ".tab[data-tab='partitions']"))
        wait.until(lambda d: d.find_element(By.ID, "partitions").is_displayed())
        for partition_id in updated_partition_ids:
            create_partition(partition_id)

        log_step("Editing dataset and quota")
        click((By.CSS_SELECTOR, ".tab[data-tab='datasets']"))
        wait_for_grid_row("datasets-grid", dataset_name)
        click_grid_row_action("datasets-grid", dataset_name, "edit")
        wait.until(lambda d: d.find_element(By.ID, "dataset-form-page").is_displayed())
        wait_for_dataset_history_rows(1)
        owner_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='owner']")
        pattern_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='service_name_pattern']")
        throughput_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='throughput_bytes']")
        owner_input.clear()
        owner_input.send_keys(updated_owner)
        pattern_input.clear()
        pattern_input.send_keys(updated_service_pattern)
        throughput_input.clear()
        throughput_input.send_keys("1234")
        select_value((By.CSS_SELECTOR, "#form-dataset [name='partition_mode']"), "dedicated")
        select_value((By.CSS_SELECTOR, "#form-dataset [name='assignment_strategy']"), "manual")
        partition_ids_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='partition_ids']")
        partition_ids_input.clear()
        partition_ids_input.send_keys(", ".join(updated_partition_ids))
        click((By.CSS_SELECTOR, "#form-dataset button[type='submit']"))
        wait_for_toast("Dataset updated")
        wait_for_grid_row_text("datasets-grid", dataset_name, updated_owner)
        wait_for_grid_row_text("datasets-grid", dataset_name, updated_service_pattern)
        wait_for_grid_row_text("datasets-grid", dataset_name, "1234")
        wait_for_grid_row_text("datasets-grid", dataset_name, updated_partition_ids[0])
        wait_for_grid_row_text("datasets-grid", dataset_name, "Dedicated")

        log_step("Checking dataset assignment history")
        click_grid_row_action("datasets-grid", dataset_name, "edit")
        wait.until(lambda d: d.find_element(By.ID, "dataset-form-page").is_displayed())
        wait_for_dataset_history_rows(2)
        history_text = driver.find_element(By.ID, "dataset-assignment-history").text
        if "Current" not in history_text or "Historical" not in history_text:
            fail("Dataset assignment history did not show current and historical windows")
        for partition_id in updated_partition_ids:
            if partition_id not in history_text:
                fail(f"Dataset assignment history did not include partition {partition_id}")
        if not any(partition_id in history_text for partition_id in initial_partition_ids):
            fail("Dataset assignment history did not include the previous assignment")
        click((By.ID, "btn-dataset-cancel"))

        click((By.CSS_SELECTOR, ".tab[data-tab='partitions']"))
        wait_for_grid_row("partitions-grid", updated_partition_ids[0])
        wait_for_grid_row_text("partitions-grid", updated_partition_ids[0], "Dedicated")
        wait_for_grid_row_text("partitions-grid", updated_partition_ids[0], dataset_name)

        log_step("Creating redaction")
        click((By.CSS_SELECTOR, ".tab[data-tab='redactions']"))
        wait.until(lambda d: d.find_element(By.ID, "redactions").is_displayed())
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#redactions-grid .gridjs-container")))
        assert_grid_header_tooltip("redactions-grid", "Field", "Field key")
        assert_grid_header_tooltip("redactions-grid", "Start", "Start of redaction window")
        click((By.ID, "btn-new-redaction"))
        wait.until(lambda d: "open" in d.find_element(By.ID, "modal-redaction").get_attribute("class"))
        driver.find_element(By.CSS_SELECTOR, "#form-redaction [name='name']").send_keys(redaction_name)
        driver.find_element(By.CSS_SELECTOR, "#form-redaction [name='field_name']").send_keys(field_name)
        driver.find_element(By.CSS_SELECTOR, "#form-redaction [name='start_time_epoch_ms']").send_keys("1000")
        driver.find_element(By.CSS_SELECTOR, "#form-redaction [name='end_time_epoch_ms']").send_keys("2000")
        click((By.CSS_SELECTOR, "#form-redaction button[type='submit']"))
        wait_for_toast("Redaction created")
        wait_for_grid_row("redactions-grid", redaction_name)

        log_step("Deleting redaction")
        click_grid_row_action("redactions-grid", redaction_name, "delete-redaction")
        wait.until(lambda d: "open" in d.find_element(By.ID, "modal-confirm").get_attribute("class"))
        click((By.ID, "confirm-ok"))
        wait_for_toast("Redaction deleted")
        wait_for_grid_row_absent("redactions-grid", redaction_name)

        log_step("Checking operations tab")
        click((By.CSS_SELECTOR, ".tab[data-tab='operations']"))
        wait.until(lambda d: d.find_element(By.ID, "operations").is_displayed())
        wait.until(EC.visibility_of_element_located((By.ID, "form-restore-replica")))
        wait.until(EC.visibility_of_element_located((By.ID, "form-restore-replica-ids")))
        wait.until(EC.visibility_of_element_located((By.ID, "form-reset-partition")))

        log_step("Deleting dataset with danger confirmation")
        click((By.CSS_SELECTOR, ".tab[data-tab='datasets']"))
        wait_for_grid_row("datasets-grid", dataset_name)
        click_grid_row_action("datasets-grid", dataset_name, "delete")
        wait.until(
            lambda d: "open" in d.find_element(By.ID, "modal-danger-confirm").get_attribute("class")
        )
        confirm_input = driver.find_element(By.ID, "danger-confirm-input")
        if driver.find_element(By.ID, "danger-confirm-ok").is_enabled():
            fail("Danger confirmation button should start disabled")
        confirm_input.send_keys("wrong-name")
        wait.until(
            lambda d: d.find_element(By.ID, "danger-confirm-input").get_attribute("value")
            == "wrong-name"
            and not d.find_element(By.ID, "danger-confirm-ok").is_enabled()
        )
        if driver.find_element(By.ID, "danger-confirm-ok").is_enabled():
            fail("Danger confirmation button enabled for the wrong dataset name")
        confirm_input.clear()
        confirm_input.send_keys(dataset_name)
        wait.until(lambda d: d.find_element(By.ID, "danger-confirm-ok").is_enabled())
        click((By.ID, "danger-confirm-ok"))
        wait_for_toast("Dataset deleted")
        wait_for_grid_row_absent("datasets-grid", dataset_name)

        if catalog_supported:
            log_step("Deleting catalog partitions")
            click((By.CSS_SELECTOR, ".tab[data-tab='partitions']"))
            wait.until(lambda d: d.find_element(By.ID, "partitions").is_displayed())
            for partition_id in all_partition_ids:
                wait_for_grid_row("partitions-grid", partition_id)
                click_grid_row_action("partitions-grid", partition_id, "delete-partition")
                wait.until(
                    lambda d: "open" in d.find_element(By.ID, "modal-confirm").get_attribute("class")
                )
                click((By.ID, "confirm-ok"))
                wait_for_toast("Deleted partition")
                wait_for_grid_row_absent("partitions-grid", partition_id)

        log_step("Admin UI Selenium test passed")
    except TimeoutException as exc:
        capture_failure("timeout")
        raise AssertionError(f"Timed out while waiting for the admin UI: {exc}") from exc
    except Exception:
        capture_failure("failure")
        raise
    finally:
        driver.quit()


def main() -> int:
    args = parse_args()
    try:
        run_test(args)
    except Exception as exc:
        print(f"admin UI Selenium test failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
