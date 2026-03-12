#!/usr/bin/env python3
"""Shared helpers for OpenSearch Dashboards UI verification scripts."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable


Logger = Callable[[str], None]


def http_request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: int = 20,
) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def http_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: int = 20,
) -> tuple[int, Any]:
    status, payload = http_request(
        url, method=method, headers=headers, body=body, timeout=timeout
    )
    text = payload.decode("utf-8", errors="replace")
    if not text.strip():
        return status, None
    return status, json.loads(text)


def dashboards_status(base_url: str) -> str:
    status, payload = http_json(f"{base_url}/api/status")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"Dashboards status check failed with HTTP {status}")
    return (
        payload.get("status", {}).get("overall", {}).get("state")
        or payload.get("overall", {}).get("state")
        or payload.get("status", {}).get("overall", {}).get("level")
        or "unknown"
    )


def resolve_index(base_url: str, index_pattern: str) -> list[str]:
    quoted = urllib.parse.quote(index_pattern, safe="")
    status, payload = http_json(
        f"{base_url}/internal/index-pattern-management/resolve_index/{quoted}"
    )
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"resolve_index failed with HTTP {status}")
    indices = payload.get("indices", [])
    return [entry.get("name", "") for entry in indices if isinstance(entry, dict)]


def find_saved_objects(
    base_url: str,
    *,
    object_type: str,
    title: str,
    search_field: str = "title",
) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode(
        {
            "type": object_type,
            "search_fields": search_field,
            "search": title,
            "per_page": "1000",
        }
    )
    status, payload = http_json(f"{base_url}/api/saved_objects/_find?{params}")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"saved object search failed with HTTP {status}")
    saved_objects = payload.get("saved_objects", [])
    matches: list[dict[str, Any]] = []
    for saved_object in saved_objects:
        if not isinstance(saved_object, dict):
            continue
        attributes = saved_object.get("attributes", {})
        if isinstance(attributes, dict) and attributes.get(search_field) == title:
            matches.append(saved_object)
    return matches


def find_index_patterns(base_url: str, title: str) -> list[dict[str, Any]]:
    return find_saved_objects(base_url, object_type="index-pattern", title=title)


def delete_saved_object(base_url: str, object_type: str, object_id: str) -> None:
    quoted_id = urllib.parse.quote(object_id, safe="")
    status, _ = http_request(
        f"{base_url}/api/saved_objects/{object_type}/{quoted_id}",
        method="DELETE",
        headers={"osd-xsrf": "true"},
    )
    if status not in (200, 404):
        raise RuntimeError(
            f"failed deleting saved object {object_type}/{object_id!r}; HTTP {status}"
        )


def delete_index_pattern(base_url: str, object_id: str) -> None:
    delete_saved_object(base_url, "index-pattern", object_id)


def import_selenium() -> dict[str, Any]:
    try:
        from selenium import webdriver
        from selenium.common.exceptions import TimeoutException, WebDriverException
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import Select, WebDriverWait
    except ImportError as error:
        raise SystemExit(
            "Selenium is not installed. Create a virtualenv and run "
            "`.venv/bin/pip install selenium` first."
        ) from error

    return {
        "webdriver": webdriver,
        "TimeoutException": TimeoutException,
        "WebDriverException": WebDriverException,
        "Options": Options,
        "Service": Service,
        "By": By,
        "EC": EC,
        "Select": Select,
        "WebDriverWait": WebDriverWait,
    }


def build_driver(
    *,
    headless: bool,
    window_size: str,
    chrome_binary: str,
    chromedriver: str,
    selenium: dict[str, Any],
):
    options = selenium["Options"]()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--window-size={window_size}")
    if headless:
        options.add_argument("--headless=new")
    if chrome_binary:
        options.binary_location = chrome_binary

    service = (
        selenium["Service"](executable_path=chromedriver)
        if chromedriver
        else selenium["Service"]()
    )
    return selenium["webdriver"].Chrome(service=service, options=options)


def wait_for_index_pattern_saved(
    base_url: str, title: str, timeout_seconds: int
) -> list[dict[str, Any]]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        matches = find_index_patterns(base_url, title)
        if matches:
            return matches
        time.sleep(1)
    raise RuntimeError(
        f"timed out waiting for Dashboards to save index pattern {title!r}"
    )


def save_failure_screenshot(driver: Any, path: str, logger: Logger) -> None:
    if not path:
        return
    try:
        driver.save_screenshot(path)
        logger(f"Wrote failure screenshot to {path}")
    except Exception:
        pass


def set_input_value(driver: Any, element: Any, value: str) -> None:
    driver.execute_script(
        """
        const input = arguments[0];
        const nextValue = arguments[1];
        const setter = Object.getOwnPropertyDescriptor(
          HTMLInputElement.prototype,
          'value'
        ).set;
        setter.call(input, nextValue);
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        input.blur();
        """,
        element,
        value,
    )


def sleep_if_needed(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def run_index_pattern_creation_workflow(
    *,
    dashboards_url: str,
    index_pattern: str,
    time_field: str,
    delete_existing: bool,
    headless: bool,
    window_size: str,
    chrome_binary: str,
    chromedriver: str,
    timeout_seconds: int,
    slow_seconds: float,
    keep_open_seconds: float,
    failure_screenshot: str,
    logger: Logger,
) -> tuple[list[dict[str, Any]], str]:
    if "*" in index_pattern:
        raise RuntimeError("Wildcard index patterns are intentionally not supported.")

    overall_state = dashboards_status(dashboards_url)
    logger(f"Dashboards status: {overall_state}")
    if overall_state not in {"green", "available"}:
        raise RuntimeError(
            f"Dashboards is not ready; /api/status reported {overall_state!r}"
        )

    resolved_indices = resolve_index(dashboards_url, index_pattern)
    logger(f"resolve_index returned: {resolved_indices}")
    if index_pattern not in resolved_indices:
        raise RuntimeError(
            f"Dashboards does not currently resolve exact index {index_pattern!r}"
        )

    existing = find_index_patterns(dashboards_url, index_pattern)
    if existing and delete_existing:
        logger(
            "Deleting existing saved object id(s): "
            + ", ".join(obj.get("id", "<unknown>") for obj in existing)
        )
        for saved_object in existing:
            delete_index_pattern(dashboards_url, saved_object["id"])
    elif existing:
        ids = ", ".join(obj.get("id", "<unknown>") for obj in existing)
        raise RuntimeError(
            f"Dashboards already has an index pattern titled {index_pattern!r} "
            f"(saved object id(s): {ids}). Rerun with delete_existing enabled."
        )

    selenium = import_selenium()
    driver = None
    try:
        driver = build_driver(
            headless=headless,
            window_size=window_size,
            chrome_binary=chrome_binary,
            chromedriver=chromedriver,
            selenium=selenium,
        )
        wait = selenium["WebDriverWait"](driver, timeout_seconds)
        by = selenium["By"]
        ec = selenium["EC"]
        select_cls = selenium["Select"]

        logger("Opening Dashboards create index pattern page")
        driver.get(
            f"{dashboards_url}/app/management/opensearch-dashboards/indexPatterns/create"
        )

        name_input = wait.until(
            ec.visibility_of_element_located(
                (by.CSS_SELECTOR, '[data-test-subj="createIndexPatternNameInput"]')
            )
        )
        sleep_if_needed(slow_seconds)

        logger(f"Typing exact index pattern {index_pattern!r}")
        name_input.click()
        set_input_value(driver, name_input, index_pattern)

        logger("Waiting for Next step to become enabled")
        next_button = wait.until(
            lambda d: (
                button
                if (button := d.find_element(
                    by.CSS_SELECTOR,
                    '[data-test-subj="createIndexPatternGoToStep2Button"]',
                )).is_enabled()
                else False
            )
        )
        sleep_if_needed(slow_seconds)

        logger("Proceeding to time-field step")
        next_button.click()

        time_field_select = wait.until(
            ec.presence_of_element_located(
                (by.CSS_SELECTOR, '[data-test-subj="createIndexPatternTimeFieldSelect"]')
            )
        )

        wait.until(
            lambda d: time_field
            in [
                option.get_attribute("value")
                for option in d.find_elements(
                    by.CSS_SELECTOR,
                    '[data-test-subj="createIndexPatternTimeFieldSelect"] option',
                )
            ]
        )
        sleep_if_needed(slow_seconds)

        logger(f"Selecting time field {time_field!r}")
        select_cls(time_field_select).select_by_value(time_field)

        create_button = wait.until(
            ec.element_to_be_clickable(
                (by.CSS_SELECTOR, '[data-test-subj="createIndexPatternButton"]')
            )
        )
        sleep_if_needed(slow_seconds)

        logger("Submitting index pattern creation")
        create_button.click()

        matches = wait_for_index_pattern_saved(
            dashboards_url, index_pattern, timeout_seconds
        )
        logger(
            "Saved object created with id(s): "
            + ", ".join(match.get("id", "<unknown>") for match in matches)
        )
        logger(f"Current browser URL: {driver.current_url}")

        if keep_open_seconds > 0:
            logger(f"Keeping browser open for {keep_open_seconds:.1f}s")
            time.sleep(keep_open_seconds)
        return matches, driver.current_url
    except selenium["TimeoutException"] as error:
        if driver is not None:
            save_failure_screenshot(driver, failure_screenshot, logger)
            try:
                status_text = driver.find_element(
                    selenium["By"].CSS_SELECTOR,
                    '[data-test-subj="createIndexPatternStatusMessage"]',
                ).text
                logger(f"Current status message: {status_text}")
            except Exception:
                pass
        raise RuntimeError(
            "Timed out waiting for the Dashboards UI flow to complete."
        ) from error
    except selenium["WebDriverException"] as error:
        if driver is not None:
            save_failure_screenshot(driver, failure_screenshot, logger)
        raise RuntimeError(f"Selenium {error.__class__.__name__}: {error}") from error
    except Exception:
        if driver is not None:
            save_failure_screenshot(driver, failure_screenshot, logger)
        raise
    finally:
        if driver is not None:
            driver.quit()
