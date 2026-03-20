#!/usr/bin/env python3
"""End-to-end Dashboards smoke flow from data-view creation to UI results."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.parse
from pathlib import Path

from dashboards_ui_lib import (
    build_driver,
    dashboards_status,
    find_index_patterns,
    http_json,
    http_request,
    import_selenium,
    resolve_index,
    run_index_pattern_creation_workflow,
    save_failure_screenshot,
)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
MAX_TIME = "9223372036854775807"


def log(message: str) -> None:
    print(f"[dashboards-ui-e2e] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a full Dashboards smoke flow against the local stack: "
            "fixture ingest, UI data-view creation, Discover verification, and "
            "dashboard aggregation verification."
        )
    )
    parser.add_argument(
        "--dashboards-url",
        default="http://localhost:5601",
        help="Base URL for OpenSearch Dashboards (default: %(default)s)",
    )
    parser.add_argument(
        "--manager-url",
        default="http://localhost:8083",
        help="Base URL for the Manager API (default: %(default)s)",
    )
    parser.add_argument(
        "--bulk-health-url",
        default="http://localhost:8086/health",
        help="Bulk ingest health URL (default: %(default)s)",
    )
    parser.add_argument(
        "--start",
        action="store_true",
        help="Start the local stack with scripts/start-dashboards-stack.sh",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Start from a clean rebuild (implies --start)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser verification headless instead of visibly",
    )
    parser.add_argument(
        "--slow-seconds",
        type=float,
        default=0.0,
        help="Pause this many seconds between major browser actions",
    )
    parser.add_argument(
        "--keep-open-seconds",
        type=float,
        default=0.0,
        help="Keep the final browser open after success for this many seconds",
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
        default=40,
        help="Timeout for UI waits (default: %(default)s)",
    )
    parser.add_argument(
        "--failure-screenshot",
        default="/tmp/dashboards-full-ui-e2e-failure.png",
        help="Path to write a screenshot on browser failure",
    )
    parser.add_argument(
        "--owner",
        default="ui-e2e@test",
        help="Owner used when the script must create exact datasets",
    )
    parser.add_argument(
        "--smoke-run-id",
        default="",
        help="Optional fixed smoke_run id for the fixture",
    )
    parser.add_argument(
        "--dataset-name",
        default="test",
        help="Exact dataset/index name used for the main Dashboards data view (default: %(default)s)",
    )
    parser.add_argument(
        "--noise-dataset-name",
        default="ui-test",
        help="Exact dataset/index name used for noise/leakage checks (default: %(default)s)",
    )
    return parser.parse_args()


def wait_for_http(name: str, url: str, attempts: int = 60, sleep_secs: int = 2) -> None:
    log(f"Waiting for {name} at {url} ...")
    for _ in range(attempts):
        status, _ = http_request(url)
        if 200 <= status < 300:
            return
        time.sleep(sleep_secs)
    raise RuntimeError(f"{name} did not become ready at {url}")


def run_repo_command(command: list[str], env: dict[str, str] | None = None) -> str:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=merged_env,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed ({completed.returncode}): {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    if completed.stdout.strip():
        log(completed.stdout.strip())
    return completed.stdout


def run_gateway_command(
    path: str,
    *,
    body: dict[str, object] | None = None,
    method: str = "GET",
) -> dict[str, object]:
    command = [
        "docker",
        "exec",
        "-i",
        "dep_opensearch_dashboards",
        "curl",
        "-sS",
        "-X",
        method,
    ]
    if body is not None:
        command.extend(["-H", "Content-Type: application/json"])
    command.append(f"http://astra_dashboards_gateway:9200{path}")
    if body is not None:
        command.extend(["--data-binary", "@-"])

    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        input=json.dumps(body) if body is not None else None,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Gateway command failed ({completed.returncode}): {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"Gateway returned non-JSON output for {path!r}: {completed.stdout}"
        ) from error


def manager_headers() -> dict[str, str]:
    return {"content-type": "application/json; charset=utf-8; protocol=gRPC"}


def list_dataset_metadata(manager_url: str) -> list[dict[str, object]]:
    status, payload = http_json(
        f"{manager_url}/slack.proto.astra.ManagerApiService/ListDatasetMetadata",
        method="POST",
        headers=manager_headers(),
        body=b"{}",
    )
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"ListDatasetMetadata failed with HTTP {status}")
    datasets = payload.get("datasetMetadata", [])
    return datasets if isinstance(datasets, list) else []


def active_partition_ids(dataset: dict[str, object]) -> list[str]:
    partition_configs = dataset.get("partitionConfigs", [])
    if not isinstance(partition_configs, list):
        return []
    for config in partition_configs:
        if (
            isinstance(config, dict)
            and str(config.get("endTimeEpochMs")) == MAX_TIME
            and isinstance(config.get("partitions"), list)
        ):
            return [str(partition_id) for partition_id in config["partitions"]]
    return []


def default_partition_ids(datasets: list[dict[str, object]]) -> list[str]:
    for dataset in datasets:
        partitions = active_partition_ids(dataset)
        if partitions:
            return partitions
    return ["0"]


def create_dataset_metadata(manager_url: str, name: str, owner: str) -> None:
    payload = json.dumps(
        {"name": name, "owner": owner, "serviceNamePattern": name}
    ).encode("utf-8")
    status, _ = http_request(
        f"{manager_url}/slack.proto.astra.ManagerApiService/CreateDatasetMetadata",
        method="POST",
        headers=manager_headers(),
        body=payload,
    )
    if status != 200:
        raise RuntimeError(f"CreateDatasetMetadata for {name!r} failed with HTTP {status}")


def update_partition_assignment(
    manager_url: str, name: str, partition_ids: list[str], throughput_bytes: str = "4000000"
) -> None:
    payload = json.dumps(
        {
            "name": name,
            "throughputBytes": throughput_bytes,
            "partitionIds": partition_ids,
        }
    ).encode("utf-8")
    status, _ = http_request(
        f"{manager_url}/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment",
        method="POST",
        headers=manager_headers(),
        body=payload,
    )
    if status != 200:
        raise RuntimeError(
            f"UpdatePartitionAssignment for {name!r} failed with HTTP {status}"
        )


def ensure_exact_dataset(manager_url: str, name: str, owner: str) -> None:
    datasets = list_dataset_metadata(manager_url)
    current = next((dataset for dataset in datasets if dataset.get("name") == name), None)
    if current is None:
        log(f"Creating exact dataset {name!r}")
        create_dataset_metadata(manager_url, name, owner)
        update_partition_assignment(manager_url, name, default_partition_ids(datasets))
        return

    service_name_pattern = current.get("serviceNamePattern")
    if service_name_pattern not in (None, "", name):
        raise RuntimeError(
            f"Dataset {name!r} exists with non-exact serviceNamePattern "
            f"{service_name_pattern!r}"
        )

    if not active_partition_ids(current):
        log(f"Assigning active partitions to dataset {name!r}")
        update_partition_assignment(manager_url, name, default_partition_ids(datasets))


def verify_resolve_exact_only(dashboards_url: str, index_name: str) -> None:
    exact = resolve_index(dashboards_url, index_name)
    if exact != [index_name]:
        raise RuntimeError(
            f"Expected resolve_index({index_name!r}) to return [{index_name!r}], got {exact}"
        )
    wildcard = resolve_index(dashboards_url, f"{index_name}*")
    if wildcard:
        raise RuntimeError(
            f"Expected exact-only resolve_index to return no wildcard matches, got {wildcard}"
        )


def ingest_smoke_fixture(
    smoke_run_id: str, dataset_name: str, noise_dataset_name: str
) -> None:
    run_repo_command(
        [str(SCRIPT_DIR / "ingest-ui-smoke-fixture.sh")],
        env={
            "SMOKE_RUN_ID": smoke_run_id,
            "TARGET_INDEX_NAME": dataset_name,
            "NOISE_INDEX_NAME": noise_dataset_name,
            "USE_DOCKER_EXEC": "1"
        },
    )


def gateway_search(index_name: str, body: dict[str, object]) -> dict[str, object]:
    return run_gateway_command(f"/{index_name}/_search", body=body, method="POST")


def wait_for_gateway_hits(smoke_run_id: str, dataset_name: str) -> None:
    body = {
        "size": 5,
        "query": {
            "bool": {
                "must": [{"query_string": {"query": "message:*", "analyze_wildcard": True}}],
                "filter": [{"term": {"smoke_run": smoke_run_id}}],
                "should": [],
                "must_not": [],
            }
        },
        "sort": [{"@timestamp": {"order": "desc"}}],
    }
    for _ in range(30):
        response = gateway_search(dataset_name, body)
        total = int(
            response.get("hits", {}).get("total", {}).get("value", 0)  # type: ignore[union-attr]
        )
        if total >= 2:
            return
        time.sleep(2)
    raise RuntimeError(
        f"Fixture never became searchable through the Dashboards gateway for {dataset_name!r}"
    )


def verify_gateway_behavior(smoke_run_id: str, dataset_name: str) -> None:
    query_body = {
        "size": 5,
        "query": {
            "bool": {
                "must": [{"query_string": {"query": "message:*", "analyze_wildcard": True}}],
                "filter": [{"term": {"smoke_run": smoke_run_id}}],
                "should": [],
                "must_not": [],
            }
        },
        "sort": [{"@timestamp": {"order": "desc"}}],
    }
    response = gateway_search(dataset_name, query_body)
    hits = response.get("hits", {}).get("hits", [])
    total = int(response.get("hits", {}).get("total", {}).get("value", 0))  # type: ignore[union-attr]
    unique_indices = sorted({hit.get("_index") for hit in hits if isinstance(hit, dict)})
    if total < 2:
        raise RuntimeError(
            f"Expected at least 2 gateway hits for {dataset_name!r}, got {total}"
        )
    if unique_indices != [dataset_name]:
        raise RuntimeError(
            f"Expected only {dataset_name!r} docs through gateway, got {unique_indices}"
        )

    agg_body = {
        "size": 0,
        "query": query_body["query"],
        "aggs": {"levels": {"terms": {"field": "level", "size": 10}}},
    }
    agg_response = gateway_search(dataset_name, agg_body)
    buckets = agg_response.get("aggregations", {}).get("levels", {}).get("buckets", [])
    simplified = [
        {"key": bucket.get("key"), "doc_count": bucket.get("doc_count")}
        for bucket in buckets
        if isinstance(bucket, dict)
    ]
    expected = [{"key": "ERROR", "doc_count": 1}, {"key": "INFO", "doc_count": 1}]
    if simplified != expected:
        raise RuntimeError(
            f"Expected {dataset_name!r} terms(level) buckets {expected}, got {simplified}"
        )


def post_saved_object(
    dashboards_url: str, object_type: str, object_id: str, payload: dict[str, object]
) -> None:
    status, _ = http_request(
        f"{dashboards_url}/api/saved_objects/{object_type}/{object_id}?overwrite=true",
        method="POST",
        headers={
            "osd-xsrf": "true",
            "Content-Type": "application/json",
        },
        body=json.dumps(payload).encode("utf-8"),
        timeout=30,
    )
    if status != 200:
        raise RuntimeError(
            f"Posting saved object {object_type}/{object_id!r} failed with HTTP {status}"
        )


def create_dashboard_saved_objects(
    dashboards_url: str, data_view_id: str, smoke_run_id: str
) -> str:
    search_source = json.dumps(
        {
            "query": {"query": f"smoke_run:{smoke_run_id}", "language": "lucene"},
            "filter": [],
            "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
        }
    )

    levels_vis_state = json.dumps(
        {
            "title": "Count by level (ui e2e)",
            "type": "table",
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                {
                    "id": "2",
                    "enabled": True,
                    "type": "terms",
                    "schema": "bucket",
                    "params": {
                        "field": "level",
                        "orderBy": "1",
                        "order": "desc",
                        "size": 10,
                        "otherBucket": False,
                        "missingBucket": False,
                    },
                },
            ],
            "params": {
                "perPage": 10,
                "showPartialRows": False,
                "showMetricsAtAllLevels": False,
            },
        }
    )
    levels_payload = {
        "attributes": {
            "title": "Count by level (ui e2e)",
            "visState": levels_vis_state,
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_source},
        },
        "references": [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": data_view_id,
            }
        ],
    }
    post_saved_object(dashboards_url, "visualization", "ui-e2e-levels", levels_payload)

    duration_vis_state = json.dumps(
        {
            "title": "Avg duration by host (ui e2e)",
            "type": "table",
            "aggs": [
                {
                    "id": "1",
                    "enabled": True,
                    "type": "avg",
                    "schema": "metric",
                    "params": {"field": "duration_ms"},
                },
                {
                    "id": "2",
                    "enabled": True,
                    "type": "terms",
                    "schema": "bucket",
                    "params": {
                        "field": "host",
                        "orderBy": "1",
                        "order": "desc",
                        "size": 10,
                        "otherBucket": False,
                        "missingBucket": False,
                    },
                },
            ],
            "params": {
                "perPage": 10,
                "showPartialRows": False,
                "showMetricsAtAllLevels": False,
            },
        }
    )
    duration_payload = {
        "attributes": {
            "title": "Avg duration by host (ui e2e)",
            "visState": duration_vis_state,
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_source},
        },
        "references": [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": data_view_id,
            }
        ],
    }
    post_saved_object(
        dashboards_url, "visualization", "ui-e2e-avg-duration-host", duration_payload
    )

    dashboard_payload = {
        "attributes": {
            "title": "Astra UI E2E Smoke",
            "description": "",
            "hits": 0,
            "optionsJSON": json.dumps(
                {
                    "useMargins": True,
                    "hidePanelTitles": False,
                    "syncColors": False,
                }
            ),
            "panelsJSON": json.dumps(
                [
                    {
                        "version": "2.11.1",
                        "type": "visualization",
                        "gridData": {"x": 0, "y": 0, "w": 24, "h": 15, "i": "1"},
                        "panelIndex": "1",
                        "embeddableConfig": {},
                        "panelRefName": "panel_0",
                    },
                    {
                        "version": "2.11.1",
                        "type": "visualization",
                        "gridData": {"x": 24, "y": 0, "w": 24, "h": 15, "i": "2"},
                        "panelIndex": "2",
                        "embeddableConfig": {},
                        "panelRefName": "panel_1",
                    },
                ]
            ),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-24h",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {"query": {"query": "", "language": "lucene"}, "filter": []}
                )
            },
        },
        "references": [
            {"name": "panel_0", "type": "visualization", "id": "ui-e2e-levels"},
            {
                "name": "panel_1",
                "type": "visualization",
                "id": "ui-e2e-avg-duration-host",
            },
        ],
    }
    dashboard_id = "ui-e2e-dashboard"
    post_saved_object(dashboards_url, "dashboard", dashboard_id, dashboard_payload)
    return dashboard_id


def discover_url(dashboards_url: str, data_view_id: str, smoke_run_id: str) -> str:
    global_state = "(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-24h,to:now))"
    app_state = (
        "(discover:(columns:!(message,level,host,smoke_run),isDirty:!f,sort:!()),"
        f"metadata:(indexPattern:'{data_view_id}',view:discover))"
    )
    query_state = f"(filters:!(),query:(language:lucene,query:'smoke_run:{smoke_run_id}'))"
    return (
        f"{dashboards_url}/app/data-explorer/discover#/?"
        f"_g={urllib.parse.quote(global_state, safe='()!,:@')}"
        f"&_a={urllib.parse.quote(app_state, safe='()!,:@')}"
        f"&_q={urllib.parse.quote(query_state, safe='()!,:@')}"
    )


def assert_discover_ui(
    driver: object,
    dashboards_url: str,
    data_view_id: str,
    smoke_run_id: str,
    wait: object,
) -> None:
    by = import_selenium()["By"]
    driver.get(discover_url(dashboards_url, data_view_id, smoke_run_id))  # type: ignore[attr-defined]
    wait.until(  # type: ignore[attr-defined]
        lambda d: "Test dataset log one" in d.find_element(by.TAG_NAME, "body").text
    )
    text = driver.find_element(by.TAG_NAME, "body").text  # type: ignore[attr-defined]
    required = ["Test dataset log one", "Test dataset log two", "smoke_run:" + smoke_run_id]
    for value in required:
        if value not in text:
            raise RuntimeError(f"Discover UI did not show expected text {value!r}")
    forbidden = ["UI test dataset log one", "UI test dataset log two"]
    for value in forbidden:
        if value in text:
            raise RuntimeError(f"Discover UI leaked non-test doc text {value!r}")


def assert_dashboard_ui(driver: object, dashboards_url: str, dashboard_id: str, wait: object) -> None:
    by = import_selenium()["By"]
    driver.get(f"{dashboards_url}/app/dashboards#/view/{dashboard_id}")
    wait.until(  # type: ignore[attr-defined]
        lambda d: "Astra UI E2E Smoke" in d.find_element(by.TAG_NAME, "body").text
        and "Count by level (ui e2e)" in d.find_element(by.TAG_NAME, "body").text
        and "Avg duration by host (ui e2e)" in d.find_element(by.TAG_NAME, "body").text
        and "INFO" in d.find_element(by.TAG_NAME, "body").text
        and "ERROR" in d.find_element(by.TAG_NAME, "body").text
        and "host-a" in d.find_element(by.TAG_NAME, "body").text
        and "host-b" in d.find_element(by.TAG_NAME, "body").text
    )
    text = driver.find_element(by.TAG_NAME, "body").text  # type: ignore[attr-defined]
    if "No results found" in text:
        raise RuntimeError("Dashboard UI still showed 'No results found'")


def main() -> int:
    args = parse_args()
    smoke_run_id = args.smoke_run_id or f"dashboards-ui-e2e-{int(time.time())}"

    if args.dataset_name == args.noise_dataset_name:
        raise RuntimeError("--dataset-name and --noise-dataset-name must be different")

    if args.clean:
        args.start = True

    if args.start:
        command = [str(SCRIPT_DIR / "start-dashboards-stack.sh")]
        if args.clean:
            command.append("--clean")
        log("Starting local stack ...")
        run_repo_command(command)

    wait_for_http("OpenSearch Dashboards", f"{args.dashboards_url}/api/status")
    wait_for_http("Bulk ingest", args.bulk_health_url)

    if dashboards_status(args.dashboards_url) not in {"green", "available"}:
        raise RuntimeError("Dashboards is not green")

    ensure_exact_dataset(args.manager_url, args.dataset_name, args.owner)
    ensure_exact_dataset(args.manager_url, args.noise_dataset_name, args.owner)
    verify_resolve_exact_only(args.dashboards_url, args.dataset_name)

    log(f"Ingesting smoke fixture with smoke_run {smoke_run_id!r}")
    ingest_smoke_fixture(smoke_run_id, args.dataset_name, args.noise_dataset_name)
    wait_for_gateway_hits(smoke_run_id, args.dataset_name)
    verify_gateway_behavior(smoke_run_id, args.dataset_name)

    run_index_pattern_creation_workflow(
        dashboards_url=args.dashboards_url,
        index_pattern=args.dataset_name,
        time_field="@timestamp",
        delete_existing=True,
        headless=args.headless,
        window_size=args.window_size,
        chrome_binary=args.chrome_binary,
        chromedriver=args.chromedriver,
        timeout_seconds=args.timeout_seconds,
        slow_seconds=args.slow_seconds,
        keep_open_seconds=0.0,
        failure_screenshot=args.failure_screenshot,
        logger=lambda message: log(f"data-view: {message}"),
    )

    matches = find_index_patterns(args.dashboards_url, args.dataset_name)
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one Dashboards data view titled {args.dataset_name!r}, got {len(matches)}"
        )
    data_view_id = matches[0]["id"]
    log(f"Using Dashboards data view id {data_view_id}")

    dashboard_id = create_dashboard_saved_objects(
        args.dashboards_url, data_view_id, smoke_run_id
    )
    log(f"Created dashboard saved objects at id {dashboard_id}")

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
        log("Opening Discover with the UI-created data view")
        assert_discover_ui(driver, args.dashboards_url, data_view_id, smoke_run_id, wait)
        log(
            "Discover UI showed the expected target logs without "
            f"{args.noise_dataset_name!r} leakage"
        )
        log("Opening dashboard backed by the UI-created data view")
        assert_dashboard_ui(driver, args.dashboards_url, dashboard_id, wait)
        log("Dashboard UI showed the expected aggregation results")
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

    log("Full Dashboards UI E2E flow succeeded")
    log(f"Smoke run id: {smoke_run_id}")
    log(f"Discover URL: {discover_url(args.dashboards_url, data_view_id, smoke_run_id)}")
    log(f"Dashboard URL: {args.dashboards_url}/app/dashboards#/view/{dashboard_id}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        log(f"ERROR: {error}")
        raise SystemExit(1)
