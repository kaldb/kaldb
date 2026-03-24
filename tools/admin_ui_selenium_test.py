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
        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except ModuleNotFoundError as exc:
        print(
            "selenium is not installed. Install it with: pip install selenium",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc

    return webdriver, TimeoutException, Options, Service, By, EC, WebDriverWait


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
    webdriver, _, Options, Service, _, _, _ = load_selenium()

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
    _, TimeoutException, _, _, By, EC, WebDriverWait = load_selenium()
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

    screenshot_dir = Path(args.screenshot_dir)
    screenshot_dir.mkdir(parents=True, exist_ok=True)

    def wait_for_toast(text: str) -> None:
        wait.until(
            lambda d: any(
                text in toast.text
                for toast in d.find_elements(By.CSS_SELECTOR, "#toast-container .toast")
            )
        )

    def current_grid_page_marker(d, grid_id: str) -> tuple[str, str]:
        current_buttons = d.find_elements(
            By.CSS_SELECTOR,
            f"#{grid_id} .gridjs-pagination .gridjs-pages button.gridjs-currentPage",
        )
        current_page = current_buttons[0].text if current_buttons else ""
        rows = d.find_elements(By.CSS_SELECTOR, f"#{grid_id} tbody tr")
        first_row_text = rows[0].text if rows else ""
        return current_page, first_row_text

    def current_grid_row(d, grid_id: str, text: str):
        for row in d.find_elements(By.CSS_SELECTOR, f"#{grid_id} tbody tr"):
            if text in row.text:
                return row
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
            prev_button = grid_edge_button(driver, grid_id, next_page=False)
            if prev_button is None or not prev_button.is_enabled():
                return
            before = current_grid_page_marker(driver, grid_id)
            prev_button.click()
            wait.until(lambda d: current_grid_page_marker(d, grid_id) != before)

    def find_grid_row(grid_id: str, text: str):
        move_grid_to_first_page(grid_id)
        while True:
            row = current_grid_row(driver, grid_id, text)
            if row is not None:
                return row

            next_button = grid_edge_button(driver, grid_id, next_page=True)
            if next_button is None or not next_button.is_enabled():
                return None

            before = current_grid_page_marker(driver, grid_id)
            next_button.click()
            wait.until(lambda d: current_grid_page_marker(d, grid_id) != before)

    def wait_for_grid_row(grid_id: str, text: str) -> None:
        wait.until(lambda d: find_grid_row(grid_id, text) is not None)

    def wait_for_grid_row_text(grid_id: str, row_text: str, expected_text: str) -> None:
        def row_contains_text() -> bool:
            row = find_grid_row(grid_id, row_text)
            return row is not None and expected_text in row.text

        wait.until(lambda d: row_contains_text())

    def wait_for_grid_row_absent(grid_id: str, text: str) -> None:
        wait.until(lambda d: find_grid_row(grid_id, text) is None)

    def click_grid_row_action(grid_id: str, row_text: str, action: str) -> None:
        row = find_grid_row(grid_id, row_text)
        if row is None:
            fail(f"Could not find row containing {row_text!r} in {grid_id}")
        action_button = row.find_element(By.CSS_SELECTOR, f"[data-action='{action}']")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", action_button)
        action_button.click()

    def click(locator: tuple[str, str]) -> None:
        wait.until(EC.element_to_be_clickable(locator)).click()

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

        log_step("Creating dataset")
        click((By.ID, "btn-new-dataset"))
        wait.until(lambda d: d.find_element(By.ID, "dataset-form-page").is_displayed())
        name_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='name']")
        owner_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='owner']")
        pattern_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='service_name_pattern']")
        name_input.clear()
        name_input.send_keys(dataset_name)
        owner_input.clear()
        owner_input.send_keys(dataset_owner)
        pattern_input.clear()
        pattern_input.send_keys(service_pattern)
        click((By.CSS_SELECTOR, "#form-dataset button[type='submit']"))
        wait_for_toast("Dataset created")
        wait_for_grid_row("datasets-grid", dataset_name)
        wait_for_grid_row_text("datasets-grid", dataset_name, dataset_owner)
        wait_for_grid_row_text("datasets-grid", dataset_name, service_pattern)

        log_step("Editing dataset")
        click_grid_row_action("datasets-grid", dataset_name, "edit")
        wait.until(lambda d: d.find_element(By.ID, "dataset-form-page").is_displayed())
        owner_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='owner']")
        pattern_input = driver.find_element(By.CSS_SELECTOR, "#form-dataset [name='service_name_pattern']")
        owner_input.clear()
        owner_input.send_keys(updated_owner)
        pattern_input.clear()
        pattern_input.send_keys(updated_service_pattern)
        click((By.CSS_SELECTOR, "#form-dataset button[type='submit']"))
        wait_for_toast("Dataset updated")
        wait_for_grid_row_text("datasets-grid", dataset_name, updated_owner)
        wait_for_grid_row_text("datasets-grid", dataset_name, updated_service_pattern)

        log_step("Updating capacity")
        click_grid_row_action("datasets-grid", dataset_name, "partitions")
        wait.until(lambda d: "open" in d.find_element(By.ID, "modal-partition").get_attribute("class"))
        throughput_input = driver.find_element(By.CSS_SELECTOR, "#form-partition [name='throughput_bytes']")
        partition_ids_input = driver.find_element(By.CSS_SELECTOR, "#form-partition [name='partition_ids']")
        throughput_input.clear()
        throughput_input.send_keys("1234")
        partition_ids_input.clear()
        partition_ids_input.send_keys("partition-a, partition-b")
        click((By.CSS_SELECTOR, "#form-partition button[type='submit']"))
        wait_for_toast("Partitions assigned")
        wait_for_grid_row_text("datasets-grid", dataset_name, "1234")
        wait_for_grid_row_text("datasets-grid", dataset_name, "partition-a")
        wait_for_grid_row_text("datasets-grid", dataset_name, "partition-b")

        log_step("Creating redaction")
        click((By.CSS_SELECTOR, ".tab[data-tab='redactions']"))
        wait.until(lambda d: d.find_element(By.ID, "redactions").is_displayed())
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
        confirm_button = driver.find_element(By.ID, "danger-confirm-ok")
        if confirm_button.is_enabled():
            fail("Danger confirmation button should start disabled")
        confirm_input.send_keys("wrong-name")
        time.sleep(0.2)
        if confirm_button.is_enabled():
            fail("Danger confirmation button enabled for the wrong dataset name")
        confirm_input.clear()
        confirm_input.send_keys(dataset_name)
        wait.until(lambda d: d.find_element(By.ID, "danger-confirm-ok").is_enabled())
        click((By.ID, "danger-confirm-ok"))
        wait_for_toast("Dataset deleted")
        wait_for_grid_row_absent("datasets-grid", dataset_name)

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
