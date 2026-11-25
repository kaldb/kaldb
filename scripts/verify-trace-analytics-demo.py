#!/usr/bin/env python3
"""Verify native Trace Analytics routes against real spans stored in KalDB."""

import argparse
import json
from datetime import datetime
from urllib.request import Request, urlopen


def main():
    """Find a multi-service trace and verify the native waterfall contract."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dashboards-url", default="http://localhost:5601")
    parser.add_argument("--opensearch-url", default="http://localhost:9200")
    parser.add_argument("--trace-id")
    parser.add_argument("--trace-group", default="user_browse_product",
                        help="Choose a demo transaction without the quote service's clock skew")
    args = parser.parse_args()

    def post(route, body=None):
        request = Request(args.dashboards_url + route, method="POST",
                          data=json.dumps(body or {}).encode(),
                          headers={"osd-xsrf": "true", "Content-Type": "application/json"})
        with urlopen(request, timeout=40) as response:
            result = json.load(response)
        if isinstance(result, dict):
            assert not result.get("timed_out"), result
            assert result.get("_shards", {}).get("failed", 0) == 0, result
            if "_shards" in result:
                assert result["_shards"]["total"] > 0, "KalDB has no available query shards yet"
        return result

    def search(body):
        body.setdefault("index", "otel-v1-apm-span-*")
        return post("/api/observability/trace_analytics/query", body)

    def trace_query(trace_id):
        return {"bool": {"must": [{"term": {"traceId": trace_id}}]}}

    assert post("/api/observability/trace_analytics/data_prepper_indices") is True
    # Select candidates using KalDB aggregations, rather than guessing an ID or seeding data.
    if not args.trace_id:
        candidates = search({"size": 0, "query": {"bool": {"must": [
            {"range": {"startTime": {"gte": "now-15m", "lte": "now"}}},
            {"term": {"parentSpanId": ""}}, {"term": {"name": args.trace_group}}]}},
            "aggs": {"candidates": {"terms": {"field": "traceId", "size": 1}}}})
        buckets = candidates["aggregations"]["candidates"]["buckets"]
        assert buckets, f"No recent {args.trace_group} trace; keep the demo load generator running"
        args.trace_id = buckets[0]["key"]

    trace_aggregations = {
        "traces": {"terms": {"field": "traceId", "size": 200}, "aggs": {
            "trace_group": {"terms": {"field": "traceGroup", "size": 1}},
            "latency": {"max": {"script": {"source":
                "doc['traceGroupFields.durationInNanos'].size() == 0 ? 0 : "
                "doc['traceGroupFields.durationInNanos'].value / 1000000.0"}}},
            "last_updated": {"max": {"field": "traceGroupFields.endTime"}},
            "error_count": {"filter": {"term": {"traceGroupFields.statusCode": "2"}}}}}}
    recent = {"bool": {"must": [
        {"range": {"startTime": {"gte": "now-15m", "lte": "now"}}}]}}
    listing = search({"size": 0, "query": recent, "aggs": trace_aggregations})
    assert listing["aggregations"]["traces"]["buckets"], "Native trace list is empty"
    percentiles = search({"size": 0, "query": {"bool": {"must": [
        *recent["bool"]["must"], {"term": {"parentSpanId": {"value": ""}}}]}},
        "aggs": {"trace_group_name": {"terms": {"field": "name", "size": 10000},
                    "aggs": {"percentiles": {"percentiles": {"field": "durationInNanos",
                                "percents": list(range(101))}}}}}})
    assert percentiles["aggregations"]["trace_group_name"]["buckets"], "Native trace percentiles are empty"
    overview = search({"size": 0, "query": trace_query(args.trace_id), "aggs": trace_aggregations})
    root = overview["aggregations"]["traces"]["buckets"]
    assert root and root[0]["latency"]["value"] > 0, "Missing root span duration"

    result = search({"size": 3000, "query": {"bool": {"must": [
        {"term": {"traceId": args.trace_id}}, {"exists": {"field": "serviceName"}}]}},
        "sort": [{"startTime": {"order": "desc"}}], "_source": {"includes": [
            "serviceName", "name", "startTime", "endTime", "spanId", "status.code", "durationInNanos"]}})
    hits = result["hits"]["hits"]
    assert result["hits"]["total"]["value"] == len(hits), "Waterfall query truncated the trace"
    assert len(hits) < 3000, "Trace exceeds the native view's span limit"
    spans = [hit["_source"] for hit in hits]
    services = sorted({span["serviceName"] for span in spans})
    assert len(services) >= 3, services
    starts = [datetime.fromisoformat(span["startTime"]) for span in spans]
    assert (max(starts) - min(starts)).total_seconds() < 60, "Source clocks are skewed; choose another trace group"
    ids = {span["spanId"] for span in spans}
    assert any(span["parentSpanId"] in ids for span in spans), "No parent/child links"
    for hit in hits:
        span = hit["_source"]
        assert span["traceId"] == args.trace_id
        assert hit["sort"][0] > 10**18, "Gantt sort timestamps must be nanoseconds"
        if "duration" in span:
            assert span["durationInNanos"] == span["duration"] * 1000

    breakdown = search({"size": 0, "query": trace_query(args.trace_id), "aggs": {
        "service_type": {"terms": {"field": "serviceName", "size": 100}, "aggs": {
            "total_latency_nanos": {"sum": {"field": "durationInNanos"}},
            "total_latency": {"bucket_script": {"buckets_path": {
                "latency": "total_latency_nanos.value"},
                "script": "Math.round(params.latency / 10000) / 100.0"}}}}}})
    assert len(breakdown["aggregations"]["service_type"]["buckets"]) >= 3
    flyout = search({"size": 1, "query": {"bool": {"must": [
        {"term": {"spanId": spans[0]["spanId"]}}]}}})
    assert flyout["hits"]["hits"][0]["_source"]["spanId"] == spans[0]["spanId"]
    with urlopen(args.opensearch_url + "/_cat/indices/otel*?format=json", timeout=10) as response:
        indices = json.load(response)
    assert not indices, f"Unexpected trace copy in OpenSearch: {indices}"
    print(json.dumps({"traceId": args.trace_id, "spans": len(spans), "services": services,
                      "latencyMs": root[0]["latency"]["value"],
                      "url": args.dashboards_url + "/app/observability-traces#/traces/" + args.trace_id},
                     indent=2))


if __name__ == "__main__":
    main()
