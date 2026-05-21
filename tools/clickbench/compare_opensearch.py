#!/usr/bin/env python3
"""Compare ClickBench-shaped OpenSearch DSL responses against Astra.

The default mode starts a disposable OpenSearch container, creates a small
ClickBench-shaped index, loads deterministic fixture documents, and prints a
43-item ClickBench compatibility report.

Pass --astra-url to run the same requests against an Astra OpenSearch-compatible
query endpoint and compare the normalized response bodies.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CONTAINER_NAME = "kaldb-clickbench-opensearch-compare"
DEFAULT_IMAGE = "opensearchproject/opensearch:2.11.1"
DEFAULT_PORT = 19200
DEFAULT_INDEX = "hits"
DEFAULT_ASTRA_QUERY_URL = "http://localhost:8081"
DEFAULT_ASTRA_BULK_URL = "http://localhost:8086/_bulk"
DEFAULT_ASTRA_TIMEOUT_SECS = 300
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ASTRA_SERVICES = [
    "zookeeper",
    "kafka",
    "s3",
    "openzipkin",
    "astra_preprocessor",
    "astra_index",
    "astra_manager",
    "astra_query",
]

# The fixed `container_name` for each started service (from docker-compose.yml).
# These names are global, so a leftover stack from a *different* compose project
# (e.g. a git-worktree run, whose project name is the worktree dir) collides by
# name even though `compose down` only cleans its own project. We force-remove
# these by name before `up`. Keep in sync with ASTRA_SERVICES.
ASTRA_CONTAINER_NAMES = [
    "dep_zookeeper",
    "dep_kafka",
    "dep_s3",
    "dep_openzipkin",
    "astra_preprocessor",
    "astra_index",
    "astra_manager",
    "astra_query",
]
# Astra routes distributed searches through live-snapshot time ranges before Lucene sees the query.
# Keep fixture timestamps ahead of stack startup so date-filtered benchmark queries reach the live chunk.
FIXTURE_BASE_TIME = (
    dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0) + dt.timedelta(minutes=45)
)
IGNORED_RESPONSE_KEYS = {
    "_id",
    "_index",
    "_score",
    "_shards",
    "_type",
    "doc_count_error_upper_bound",
    "sum_other_doc_count",
    "timed_out",
    "took",
}


MAPPING = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "AdvEngineID": {"type": "integer"},
            "ClientIP": {"type": "integer"},
            "CounterID": {"type": "integer"},
            "DontCountHits": {"type": "integer"},
            "EventDate": {"type": "date"},
            "EventTime": {"type": "date"},
            "IsDownload": {"type": "integer"},
            "IsLink": {"type": "integer"},
            "IsRefresh": {"type": "integer"},
            "MobilePhone": {"type": "integer"},
            "MobilePhoneModel": {"type": "keyword"},
            "Referer": {"type": "keyword"},
            "RefererHash": {"type": "long"},
            "RegionID": {"type": "integer"},
            "ResolutionWidth": {"type": "integer"},
            "RunID": {"type": "keyword"},
            "SearchEngineID": {"type": "integer"},
            "SearchPhrase": {"type": "keyword"},
            "Title": {"type": "keyword"},
            "TraficSourceID": {"type": "integer"},
            "URL": {"type": "keyword"},
            "URLHash": {"type": "long"},
            "UserID": {"type": "long"},
            "WatchID": {"type": "long"},
            "WindowClientHeight": {"type": "integer"},
            "WindowClientWidth": {"type": "integer"},
        }
    }
}


def timestamp(minute: int, second: int = 0) -> str:
    return (
        FIXTURE_BASE_TIME + dt.timedelta(minutes=minute, seconds=second)
    ).isoformat().replace("+00:00", "Z")


def fixture_documents(run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    next_id = 1

    def add_rows(
        url: str,
        count: int,
        search_phrase: str,
        model: str,
        minute: int,
        title: str | None = None,
    ) -> None:
        nonlocal next_id
        for i in range(count):
            event_time = timestamp(minute, i)
            rows.append(
                {
                    "id": next_id,
                    "@timestamp": event_time,
                    "AdvEngineID": next_id % 4,
                    "ClientIP": 1000 + next_id,
                    "CounterID": 62,
                    "DontCountHits": 0,
                    "EventDate": timestamp(0),
                    "EventTime": event_time,
                    "IsDownload": 0,
                    "IsLink": 1,
                    "IsRefresh": 0,
                    "MobilePhone": next_id % 3,
                    "MobilePhoneModel": model,
                    "Referer": f"https://ref{next_id % 3}.example/path",
                    # Q40 filters on RefererHash = 3594120000172545465. Seed that literal
                    # onto the docs whose TraficSourceID is -1 (next_id % 3 == 0), so they
                    # also satisfy Q40's TraficSourceID IN (-1, 6) and page-view filters.
                    # All values must exceed Integer.MAX_VALUE: Astra infers numeric width
                    # per-value from JSON (int-range -> INTEGER, else LONG) and the field's
                    # type is fixed by the first doc, so a small first value would register
                    # RefererHash as INTEGER and make the 64-bit term query overflow.
                    "RefererHash": 3594120000172545465 if next_id % 3 == 0 else 5_000_000_000 + next_id,
                    "RegionID": next_id % 5,
                    "ResolutionWidth": 1000 + next_id,
                    "RunID": run_id,
                    "SearchEngineID": next_id % 2,
                    "SearchPhrase": search_phrase,
                    "Title": title if title is not None else f"title {search_phrase}",
                    "TraficSourceID": (next_id % 3) - 1,
                    "URL": url,
                    "URLHash": 2868770270353813622 if url.endswith("/a") else 1000 + next_id,
                    "UserID": 100000 + (next_id % 4),
                    "WatchID": 900000 + next_id,
                    "WindowClientHeight": 600 + (next_id % 2) * 300,
                    "WindowClientWidth": 800 + (next_id % 3) * 400,
                }
            )
            next_id += 1

    add_rows("https://google.example/a", 5, "alpha", "iPhone", 0)
    add_rows("https://google.example/b", 4, "beta", "Android", 1)
    # Q22 needs Title LIKE '%Google%' on docs whose URL is NOT on a google domain
    # (no ".google." substring) and whose SearchPhrase is non-empty. These two
    # non-google groups carry "Google" in the Title to make that query non-empty.
    add_rows("https://slack.example/c", 3, "gamma", "Android", 2, title="About Google Search")
    add_rows("https://example.com/d", 2, "delta", "Pixel", 3, title="Google News roundup")
    add_rows("https://example.com/e", 1, "", "", 4)

    rows.append(
        {
            "id": next_id,
            "@timestamp": timestamp(5),
            "AdvEngineID": 7,
            "ClientIP": 2000,
            "CounterID": 61,
            "DontCountHits": 0,
            "EventDate": timestamp(0),
            "EventTime": timestamp(5),
            "IsDownload": 0,
            "IsLink": 1,
            "IsRefresh": 0,
            "MobilePhone": 9,
            "MobilePhoneModel": "Filtered",
            "Referer": "https://ref.filtered.example/x",
            "RefererHash": 5_000_000_999,
            "RegionID": 9,
            "ResolutionWidth": 1600,
            "RunID": run_id,
            "SearchEngineID": 9,
            "SearchPhrase": "filtered",
            "Title": "filtered",
            "TraficSourceID": 6,
            "URL": "https://filtered.example",
            "URLHash": 9999,
            "UserID": 9999,
            "WatchID": 9999,
            "WindowClientHeight": 1080,
            "WindowClientWidth": 1920,
        }
    )
    return rows


def add_run_filter(query: dict[str, Any], run_id: str) -> dict[str, Any]:
    scoped = copy.deepcopy(query)
    run_filter = {"term": {"RunID": run_id}}
    existing_query = scoped.get("query", {"match_all": {}})

    if existing_query == {"match_all": {}}:
        scoped["query"] = {"bool": {"filter": [run_filter]}}
        return scoped

    bool_query = existing_query.get("bool") if isinstance(existing_query, dict) else None
    if isinstance(bool_query, dict):
        existing_filter = bool_query.setdefault("filter", [])
        if isinstance(existing_filter, list):
            existing_filter.append(run_filter)
        else:
            bool_query["filter"] = [existing_filter, run_filter]
        return scoped

    scoped["query"] = {"bool": {"filter": [run_filter, existing_query]}}
    return scoped


@dataclass(frozen=True)
class ClickBenchQuery:
    query_id: str
    sql: str
    dsl: dict[str, Any]


CLICKBENCH_SQL = [
    "SELECT COUNT(*) FROM hits",
    "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
    "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    "SELECT AVG(UserID) FROM hits",
    "SELECT COUNT(DISTINCT UserID) FROM hits",
    "SELECT COUNT(DISTINCT SearchPhrase) FROM hits",
    "SELECT MIN(EventDate), MAX(EventDate) FROM hits",
    "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
    "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
    "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    "SELECT UserID FROM hits WHERE UserID = 435090932899640449",
    "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
    "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    "SELECT "
    + ", ".join(
        ["SUM(ResolutionWidth)"]
        + [f"SUM(ResolutionWidth + {offset})" for offset in range(1, 90)]
    )
    + " FROM hits",
    "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
    "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
    "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000",
]


def non_empty_query(field: str) -> dict[str, Any]:
    return {"bool": {"must_not": [{"term": {field: ""}}]}}


def clickbench_date_filter() -> dict[str, Any]:
    return {"range": {"@timestamp": {"gte": timestamp(-1), "lte": timestamp(10)}}}


def page_view_filters(extra_filters: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    filters = [{"term": {"CounterID": 62}}, clickbench_date_filter()]
    if extra_filters is not None:
        filters.extend(extra_filters)
    return {"bool": {"filter": filters, "must_not": [{"term": {"IsRefresh": 1}}]}}


def benchmark_queries(run_id: str) -> list[ClickBenchQuery]:
    dsl_by_id: dict[str, dict[str, Any]] = {
        "Q0": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {"row_count": {"value_count": {"field": "@timestamp"}}},
        },
        "Q1": {
            "size": 0,
            "query": {"bool": {"must_not": [{"term": {"AdvEngineID": 0}}]}},
            "aggs": {"row_count": {"value_count": {"field": "@timestamp"}}},
        },
        "Q2": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "sum_adv_engine_id": {"sum": {"field": "AdvEngineID"}},
                "row_count": {"value_count": {"field": "@timestamp"}},
                "avg_resolution_width": {"avg": {"field": "ResolutionWidth"}},
            },
        },
        "Q3": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {"avg_user_id": {"avg": {"field": "UserID"}}},
        },
        "Q4": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {"distinct_user_id": {"cardinality": {"field": "UserID"}}},
        },
        "Q5": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {"distinct_search_phrase": {"cardinality": {"field": "SearchPhrase"}}},
        },
        "Q6": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "min_event_time": {"min": {"field": "@timestamp"}},
                "max_event_time": {"max": {"field": "@timestamp"}},
            },
        },
        "Q7": {
            "size": 0,
            "query": {"bool": {"must_not": [{"term": {"AdvEngineID": 0}}]}},
            "aggs": {
                "adv_engines": {
                    "terms": {
                        "field": "AdvEngineID",
                        "size": 10,
                        "order": {"_count": "desc"},
                    }
                }
            },
        },
        "Q8": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "regions": {
                    "terms": {"field": "RegionID", "size": 10, "order": {"users": "desc"}},
                    "aggs": {"users": {"cardinality": {"field": "UserID"}}},
                }
            },
        },
        "Q9": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "regions": {
                    "terms": {"field": "RegionID", "size": 10, "order": {"row_count": "desc"}},
                    "aggs": {
                        "sum_adv_engine_id": {"sum": {"field": "AdvEngineID"}},
                        "row_count": {"value_count": {"field": "@timestamp"}},
                        "avg_resolution_width": {"avg": {"field": "ResolutionWidth"}},
                        "users": {"cardinality": {"field": "UserID"}},
                    },
                }
            },
        },
        "Q10": {
            "size": 0,
            "query": non_empty_query("MobilePhoneModel"),
            "aggs": {
                "phones": {
                    "terms": {
                        "field": "MobilePhoneModel",
                        "size": 10,
                        "order": {"users": "desc"},
                    },
                    "aggs": {"users": {"cardinality": {"field": "UserID"}}},
                }
            },
        },
        "Q11": {
            "size": 0,
            "query": {"bool": {"must_not": [{"term": {"MobilePhoneModel": ""}}]}},
            "aggs": {
                "phones": {
                    "multi_terms": {
                        "terms": [{"field": "MobilePhone"}, {"field": "MobilePhoneModel"}],
                        "size": 10,
                        "order": {"users": "desc"},
                    },
                    "aggs": {"users": {"cardinality": {"field": "UserID"}}},
                },
            },
        },
        "Q12": {
            "size": 0,
            "query": non_empty_query("SearchPhrase"),
            "aggs": {
                "phrases": {
                    "terms": {"field": "SearchPhrase", "size": 10, "order": {"_count": "desc"}}
                }
            },
        },
        "Q13": {
            "size": 0,
            "query": non_empty_query("SearchPhrase"),
            "aggs": {
                "phrases": {
                    "terms": {"field": "SearchPhrase", "size": 10, "order": {"users": "desc"}},
                    "aggs": {"users": {"cardinality": {"field": "UserID"}}},
                }
            },
        },
        "Q14": {
            "size": 0,
            "query": non_empty_query("SearchPhrase"),
            "aggs": {
                "phrases": {
                    "multi_terms": {
                        "terms": [{"field": "SearchEngineID"}, {"field": "SearchPhrase"}],
                        "size": 10,
                        "order": {"_count": "desc"},
                    }
                }
            },
        },
        "Q15": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "users": {"terms": {"field": "UserID", "size": 10, "order": {"_count": "desc"}}}
            },
        },
        "Q16": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "users": {
                    "multi_terms": {
                        "terms": [{"field": "UserID"}, {"field": "SearchPhrase"}],
                        "size": 10,
                        "order": {"_count": "desc"},
                    }
                }
            },
        },
        "Q17": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "users": {
                    "multi_terms": {
                        "terms": [{"field": "UserID"}, {"field": "SearchPhrase"}],
                        "size": 10,
                    }
                }
            },
        },
        # ClickBench's literal UserID (435090932899640449) is from the real 100M-row
        # hits table; our synthetic fixture only has UserIDs {9999, 100000..100003}.
        # Use 9999 (held by exactly one fixture doc) so this point lookup actually
        # returns a hit and exercises term-match + _source projection on both backends.
        "Q19": {
            "size": 10,
            "query": {"term": {"UserID": 9999}},
            "_source": ["UserID"],
        },
        "Q20": {
            "size": 0,
            "query": {"wildcard": {"URL": {"value": "*google*"}}},
            "aggs": {"row_count": {"value_count": {"field": "@timestamp"}}},
        },
        "Q23": {
            "size": 10,
            "query": {"wildcard": {"URL": {"value": "*google*"}}},
            "sort": [{"@timestamp": {"order": "asc"}}],
            "_source": ["URL", "@timestamp", "SearchPhrase"],
        },
        "Q24": {
            "size": 10,
            "query": non_empty_query("SearchPhrase"),
            "sort": [{"@timestamp": {"order": "asc"}}],
            "_source": ["SearchPhrase", "@timestamp"],
        },
        "Q25": {
            "size": 10,
            "query": {"bool": {"must_not": [{"term": {"SearchPhrase": ""}}]}},
            "sort": [{"SearchPhrase": {"order": "asc"}}],
            "_source": ["SearchPhrase", "@timestamp"],
        },
        "Q26": {
            "size": 10,
            "query": non_empty_query("SearchPhrase"),
            "sort": [{"@timestamp": {"order": "asc"}}, {"SearchPhrase": {"order": "asc"}}],
            "_source": ["SearchPhrase", "@timestamp"],
        },
        "Q30": {
            "size": 0,
            "query": non_empty_query("SearchPhrase"),
            "aggs": {
                "pairs": {
                    "multi_terms": {
                        "terms": [{"field": "SearchEngineID"}, {"field": "ClientIP"}],
                        "size": 10,
                        "order": {"row_count": "desc"},
                    },
                    "aggs": {
                        "row_count": {"value_count": {"field": "@timestamp"}},
                        "sum_refresh": {"sum": {"field": "IsRefresh"}},
                        "avg_resolution_width": {"avg": {"field": "ResolutionWidth"}},
                    },
                }
            },
        },
        "Q31": {
            "size": 0,
            "query": non_empty_query("SearchPhrase"),
            "aggs": {
                "pairs": {
                    "multi_terms": {
                        "terms": [{"field": "WatchID"}, {"field": "ClientIP"}],
                        "size": 10,
                        "order": {"row_count": "desc"},
                    },
                    "aggs": {
                        "row_count": {"value_count": {"field": "@timestamp"}},
                        "sum_refresh": {"sum": {"field": "IsRefresh"}},
                        "avg_resolution_width": {"avg": {"field": "ResolutionWidth"}},
                    },
                }
            },
        },
        "Q32": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "pairs": {
                    "multi_terms": {
                        "terms": [{"field": "WatchID"}, {"field": "ClientIP"}],
                        "size": 10,
                        "order": {"row_count": "desc"},
                    },
                    "aggs": {
                        "row_count": {"value_count": {"field": "@timestamp"}},
                        "sum_refresh": {"sum": {"field": "IsRefresh"}},
                        "avg_resolution_width": {"avg": {"field": "ResolutionWidth"}},
                    },
                }
            },
        },
        "Q33": {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "urls": {"terms": {"field": "URL", "size": 10, "order": {"_count": "desc"}}}
            },
        },
        "Q36": {
            "size": 0,
            "query": page_view_filters(
                [{"term": {"DontCountHits": 0}}, {"bool": {"must_not": [{"term": {"URL": ""}}]}}]
            ),
            "aggs": {
                "urls": {"terms": {"field": "URL", "size": 10, "order": {"_count": "desc"}}}
            },
        },
        "Q37": {
            "size": 0,
            "query": page_view_filters(
                [{"term": {"DontCountHits": 0}}, {"bool": {"must_not": [{"term": {"Title": ""}}]}}]
            ),
            "aggs": {
                "titles": {
                    "terms": {"field": "Title", "size": 10, "order": {"_count": "desc"}}
                }
            },
        },
        "Q38": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"CounterID": 62}},
                        {"range": {"@timestamp": {"gte": timestamp(-1), "lte": timestamp(10)}}},
                        {"term": {"IsDownload": 0}},
                    ],
                    "must_not": [{"term": {"IsRefresh": 1}}, {"term": {"IsLink": 0}}],
                }
            },
            "aggs": {
                "urls": {
                    "terms": {"field": "URL", "size": 5, "order": {"_count": "desc"}},
                    "aggs": {
                        "page": {
                            "bucket_sort": {
                                "sort": [{"_count": {"order": "desc"}}],
                                "from": 2,
                                "size": 2,
                            }
                        }
                    },
                },
            },
        },
        "Q40": {
            "size": 0,
            "query": page_view_filters(
                [
                    {"terms": {"TraficSourceID": [-1, 6]}},
                    {"term": {"RefererHash": 3594120000172545465}},
                ]
            ),
            "aggs": {
                "urls": {
                    "multi_terms": {
                        "terms": [{"field": "URLHash"}, {"field": "@timestamp"}],
                        "size": 10,
                        "order": {"_count": "desc"},
                    },
                    "aggs": {
                        "page": {
                            "bucket_sort": {
                                "sort": [{"_count": {"order": "desc"}}],
                                "from": 2,
                                "size": 2,
                            }
                        }
                    },
                }
            },
        },
        "Q41": {
            "size": 0,
            "query": page_view_filters(
                [
                    {"term": {"DontCountHits": 0}},
                    {"term": {"URLHash": 2868770270353813622}},
                ]
            ),
            "aggs": {
                "windows": {
                    "multi_terms": {
                        "terms": [
                            {"field": "WindowClientWidth"},
                            {"field": "WindowClientHeight"},
                        ],
                        "size": 10,
                        "order": {"_count": "desc"},
                    },
                    "aggs": {
                        "page": {
                            "bucket_sort": {
                                "sort": [{"_count": {"order": "desc"}}],
                                "from": 2,
                                "size": 2,
                            }
                        }
                    },
                }
            },
        },
        "Q42": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"CounterID": 62}},
                        {"range": {"@timestamp": {"gte": timestamp(-1), "lte": timestamp(10)}}},
                    ],
                    "must_not": [{"term": {"IsRefresh": 1}}, {"term": {"DontCountHits": 1}}],
                }
            },
            "aggs": {
                "minutes": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "fixed_interval": "1m",
                        "min_doc_count": 1,
                        "order": {"_key": "asc"},
                    },
                    "aggs": {
                        "page": {
                            "bucket_sort": {
                                "sort": [{"_key": {"order": "asc"}}],
                                "from": 2,
                                "size": 2,
                            }
                        }
                    },
                },
            },
        },
        # --- Backfilled queries (previously skipped as "unsupported"). These
        # are best-effort OpenSearch-idiomatic translations; some are expected
        # to mismatch until the matching engine work lands (bucket_selector
        # HAVING, bucket_sort offset, Painless regex). OFFSET is scaled to the
        # tiny fixture (from=2), matching the other paginated queries.
        "Q18": {
            "size": 0,
            "aggs": {
                "groups": {
                    "multi_terms": {
                        "terms": [
                            {"field": "UserID"},
                            {
                                "script": {"source": "doc['@timestamp'].value.getMinute()"},
                                "value_type": "long",
                            },
                            {"field": "SearchPhrase"},
                        ],
                        "size": 10,
                        "order": {"_count": "desc"},
                    }
                }
            },
        },
        "Q21": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [{"wildcard": {"URL": "*google*"}}],
                    "must_not": [{"term": {"SearchPhrase": ""}}],
                }
            },
            "aggs": {
                "phrases": {
                    "terms": {"field": "SearchPhrase", "size": 10, "order": {"_count": "desc"}},
                    "aggs": {
                        "min_url": {
                            "terms": {"field": "URL", "size": 1, "order": {"_key": "asc"}}
                        }
                    },
                }
            },
        },
        "Q22": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [{"wildcard": {"Title": "*Google*"}}],
                    "must_not": [
                        {"wildcard": {"URL": "*.google.*"}},
                        {"term": {"SearchPhrase": ""}},
                    ],
                }
            },
            "aggs": {
                "phrases": {
                    "terms": {"field": "SearchPhrase", "size": 10, "order": {"_count": "desc"}},
                    "aggs": {
                        "min_url": {
                            "terms": {"field": "URL", "size": 1, "order": {"_key": "asc"}}
                        },
                        "min_title": {
                            "terms": {"field": "Title", "size": 1, "order": {"_key": "asc"}}
                        },
                        "distinct_users": {"cardinality": {"field": "UserID"}},
                    },
                }
            },
        },
        "Q27": {
            "size": 0,
            "query": {"bool": {"must_not": [{"term": {"URL": ""}}]}},
            "aggs": {
                "counters": {
                    "terms": {"field": "CounterID", "size": 25, "order": {"l": "desc"}},
                    "aggs": {
                        "l": {"avg": {"script": {"source": "doc['URL'].value.length()"}}},
                        "having": {
                            "bucket_selector": {
                                "buckets_path": {"c": "_count"},
                                "script": "params.c > 2",
                            }
                        },
                    },
                }
            },
        },
        "Q28": {
            "size": 0,
            "query": {"bool": {"must_not": [{"term": {"Referer": ""}}]}},
            "aggs": {
                "domains": {
                    "terms": {
                        "script": {
                            "source": (
                                "def m = /^https?:\\/\\/(?:www\\.)?([^\\/]+)\\/.*$/"
                                ".matcher(doc['Referer'].value); "
                                "if (m.matches()) { return m.group(1) } "
                                "else { return doc['Referer'].value }"
                            )
                        },
                        "size": 25,
                        "order": {"l": "desc"},
                    },
                    "aggs": {
                        "l": {"avg": {"script": {"source": "doc['Referer'].value.length()"}}},
                        "min_ref": {
                            "terms": {"field": "Referer", "size": 1, "order": {"_key": "asc"}}
                        },
                        "having": {
                            "bucket_selector": {
                                "buckets_path": {"c": "_count"},
                                "script": "params.c > 2",
                            }
                        },
                    },
                }
            },
        },
        "Q29": {
            "size": 0,
            "aggs": {
                "sum_0": {"sum": {"field": "ResolutionWidth"}},
                # Parameterize so all 89 sums share ONE compiled script; distinct
                # source strings would trip the 75/5m script-compilation limit.
                **{
                    f"sum_{n}": {
                        "sum": {
                            "script": {
                                "source": "doc['ResolutionWidth'].value + params.n",
                                "params": {"n": n},
                            }
                        }
                    }
                    for n in range(1, 90)
                },
            },
        },
        "Q34": {
            "size": 0,
            "aggs": {
                "groups": {
                    "multi_terms": {
                        "terms": [
                            {
                                "script": {"source": "1"},
                            },
                            {"field": "URL"},
                        ],
                        "size": 10,
                        "order": {"_count": "desc"},
                    }
                }
            },
        },
        "Q35": {
            "size": 0,
            "aggs": {
                "groups": {
                    "multi_terms": {
                        "terms": [
                            {"field": "ClientIP"},
                            {
                                "script": {"source": "doc['ClientIP'].value - 1"},
                            },
                            {
                                "script": {"source": "doc['ClientIP'].value - 2"},
                            },
                            {
                                "script": {"source": "doc['ClientIP'].value - 3"},
                            },
                        ],
                        "size": 10,
                        "order": {"_count": "desc"},
                    }
                }
            },
        },
        "Q39": {
            "size": 0,
            "query": page_view_filters(),
            "aggs": {
                "groups": {
                    "multi_terms": {
                        "terms": [
                            {"field": "TraficSourceID"},
                            {"field": "SearchEngineID"},
                            {"field": "AdvEngineID"},
                            {
                                "script": {
                                    "source": (
                                        "if (doc['SearchEngineID'].value == 0 "
                                        "&& doc['AdvEngineID'].value == 0) "
                                        "{ return doc['Referer'].value } else { return '' }"
                                    )
                                },
                            },
                            {"field": "URL"},
                        ],
                        "size": 10,
                        "order": {"_count": "desc"},
                    },
                    "aggs": {
                        "page": {
                            "bucket_sort": {
                                "sort": [{"_count": {"order": "desc"}}],
                                "from": 2,
                                "size": 2,
                            }
                        }
                    },
                }
            },
        },
    }
    return [
        ClickBenchQuery(
            query_id=f"Q{index}",
            sql=sql,
            dsl=add_run_filter(dsl_by_id[f"Q{index}"], run_id),
        )
        for index, sql in enumerate(CLICKBENCH_SQL, start=0)
    ]


@dataclass
class Response:
    status: int
    body: dict[str, Any]


def request(
    method: str,
    url: str,
    body: Any | None = None,
    content_type: str = "application/json",
) -> Response:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8") if not isinstance(body, bytes) else body
        headers["Content-Type"] = content_type
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return Response(response.status, json.loads(raw) if raw else {})
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"raw": raw}
        return Response(error.code, parsed)


def run(
    command: list[str],
    check: bool = True,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        cwd=cwd,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n{result.stdout}"
        )
    return result


def docker_prefix() -> list[str]:
    direct = run(["docker", "ps"], check=False)
    if direct.returncode == 0:
        return []

    sudo = run(["sudo", "-n", "docker", "ps"], check=False)
    if sudo.returncode == 0:
        return ["sudo", "-n"]

    raise RuntimeError(f"Docker is not available. docker ps output: {direct.stdout.strip()}")


def start_opensearch(image: str, port: int) -> str:
    prefix = docker_prefix()
    run([*prefix, "docker", "rm", "-f", CONTAINER_NAME], check=False)
    result = run(
        [
            *prefix,
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-p",
            f"{port}:9200",
            "-e",
            "discovery.type=single-node",
            "-e",
            "DISABLE_SECURITY_PLUGIN=true",
            "-e",
            "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m",
            "--ulimit",
            "nofile=65536:65536",
            image,
        ]
    )
    output_lines = result.stdout.strip().splitlines()
    return output_lines[-1] if output_lines else ""


def wait_for_endpoint(base_url: str) -> None:
    deadline = time.time() + 120
    last_body: Any = None
    while time.time() < deadline:
        try:
            response = request("GET", base_url)
            last_body = response.body
            if response.status == 200 and "cluster_name" in response.body:
                return
        except Exception as exc:  # noqa: BLE001
            last_body = str(exc)
        time.sleep(2)
    raise RuntimeError(f"OpenSearch did not become ready. Last response: {last_body}")


def wait_for_port(host: str, port: int, timeout_secs: int) -> None:
    deadline = time.time() + timeout_secs
    last_error: str | None = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as exc:
            last_error = str(exc)
            time.sleep(2)
    raise RuntimeError(f"{host}:{port} did not become ready. Last error: {last_error}")


def wait_for_container_log(container_name: str, pattern: str, timeout_secs: int) -> None:
    deadline = time.time() + timeout_secs
    last_output = ""
    command = [*docker_prefix(), "docker", "logs", container_name]
    while time.time() < deadline:
        result = run(command, check=False)
        last_output = result.stdout
        if pattern in last_output:
            return
        time.sleep(2)
    tail = "\n".join(last_output.splitlines()[-40:])
    raise RuntimeError(
        f"Timed out waiting for {container_name} logs to contain {pattern!r}.\n"
        f"Last log lines:\n{tail}"
    )


def bulk_body(index: str, docs: list[dict[str, Any]]) -> bytes:
    lines: list[str] = []
    for doc in docs:
        lines.append(
            json.dumps({"index": {"_index": index, "_id": doc["id"]}}, separators=(",", ":"))
        )
        lines.append(json.dumps(doc, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode("utf-8")


def load_fixture(base_url: str, index: str, docs: list[dict[str, Any]]) -> None:
    request("DELETE", f"{base_url}/{index}")
    create = request("PUT", f"{base_url}/{index}", MAPPING)
    if create.status not in (200, 201):
        raise RuntimeError(f"Failed to create {index}: {create.body}")

    bulk = request(
        "POST",
        f"{base_url}/_bulk?refresh=true",
        bulk_body(index, docs),
        content_type="application/x-ndjson",
    )
    if bulk.status != 200 or bulk.body.get("errors"):
        raise RuntimeError(f"Failed to bulk-load fixture: {bulk.body}")


def compose_command(compose_file: Path) -> list[str]:
    return [*docker_prefix(), "docker", "compose", "-f", str(compose_file)]


def astra_dataset_metadata(index: str) -> str:
    return json.dumps(
        {
            "name": index,
            "owner": "clickbench-local",
            "throughputBytes": "1000000",
            "partitionConfigs": [
                {
                    "startTimeEpochMs": "1",
                    "endTimeEpochMs": "9223372036854775807",
                    "partitions": ["0"],
                }
            ],
            "serviceNamePattern": index,
        },
        separators=(",", ":"),
    )


def zk_create_or_set(path: str, value: str) -> None:
    base = [
        *docker_prefix(),
        "docker",
        "exec",
        "dep_zookeeper",
        "zkCli.sh",
        "-server",
        "localhost:2181",
    ]
    create = run([*base, "create", path, value], check=False)
    if create.returncode == 0:
        return
    set_result = run([*base, "set", path, value], check=False)
    if set_result.returncode != 0:
        raise RuntimeError(
            f"Failed to create or set ZooKeeper node {path}\n"
            f"create output:\n{create.stdout}\nset output:\n{set_result.stdout}"
        )


def provision_astra_dataset(index: str, zk_prefix: str) -> None:
    zk_create_or_set(f"/{zk_prefix}", "")
    zk_create_or_set(f"/{zk_prefix}/service", "")
    zk_create_or_set(f"/{zk_prefix}/service/{index}", astra_dataset_metadata(index))


def load_astra_fixture(bulk_url: str, index: str, docs: list[dict[str, Any]]) -> None:
    bulk = request(
        "POST",
        bulk_url,
        bulk_body(index, docs),
        content_type="application/x-ndjson",
    )
    if bulk.status != 200 or bulk.body.get("failedDocs", 0) not in (0, "0"):
        raise RuntimeError(
            f"Failed to bulk-load Astra fixture: status={bulk.status} body={bulk.body}"
        )


def wait_for_astra_fixture(
    astra_url: str,
    index: str,
    run_id: str,
    expected_count: int,
    timeout_secs: int,
) -> None:
    hit_query = {
        "size": expected_count,
        "query": {"bool": {"filter": [{"term": {"RunID": run_id}}]}},
        "_source": ["RunID"],
    }
    deadline = time.time() + timeout_secs
    last_body: Any = None
    while time.time() < deadline:
        try:
            response = search(astra_url.rstrip("/"), index, hit_query)
            last_body = response.body
            hits = response.body.get("hits", {}).get("hits", [])
            if response.status == 200 and len(hits) == expected_count:
                return
        except Exception as exc:  # noqa: BLE001
            last_body = str(exc)
        time.sleep(2)
    raise RuntimeError(
        f"Astra did not index {expected_count} fixture docs for run_id={run_id}. "
        f"Last response: {last_body}"
    )


def start_and_load_astra(args: argparse.Namespace, docs: list[dict[str, Any]]) -> str:
    compose = compose_command(args.astra_compose_file)
    # Always tear down any existing stack first; the tool leaves the stack
    # running after a run, so a leftover container would otherwise make the
    # next `up -d` fail with a name conflict. `--reset-astra` also drops
    # volumes (local Astra/Kafka/ZK state); the default keeps them.
    if args.reset_astra:
        print("resetting Astra Compose stack and volumes")
        run([*compose, "down", "-v", "--remove-orphans"], cwd=REPO_ROOT)
    else:
        print("stopping any existing Astra Compose stack before recreating")
        run([*compose, "down", "--remove-orphans"], cwd=REPO_ROOT)

    if not args.skip_astra_build:
        print("building slackhq/astra Docker image")
        run([*docker_prefix(), "docker", "build", "-t", "slackhq/astra", "."], cwd=REPO_ROOT)

    # Clear any leftover containers holding these fixed names, including ones
    # owned by another compose project that `compose down` above won't touch.
    prefix = docker_prefix()
    for name in ASTRA_CONTAINER_NAMES:
        run([*prefix, "docker", "rm", "-f", name], check=False)

    print("starting Astra local services with Docker Compose")
    run([*compose, "up", "-d", *ASTRA_SERVICES], cwd=REPO_ROOT)
    wait_for_port("127.0.0.1", 8086, args.astra_timeout_secs)
    wait_for_port("127.0.0.1", 8080, args.astra_timeout_secs)
    wait_for_port("127.0.0.1", 8081, args.astra_timeout_secs)
    wait_for_container_log("astra_index", "Started Astra indexer.", args.astra_timeout_secs)

    print(f"provisioning Astra dataset metadata for {args.index}")
    provision_astra_dataset(args.index, args.astra_zk_prefix)
    wait_for_container_log(
        "astra_preprocessor",
        f"Rate limiter initialized for {args.index}",
        args.astra_timeout_secs,
    )

    print(f"loading {len(docs)} fixture docs into Astra through {args.astra_bulk_url}")
    load_astra_fixture(args.astra_bulk_url, args.index, docs)
    wait_for_astra_fixture(
        args.astra_url,
        args.index,
        args.run_id,
        len(docs),
        args.astra_timeout_secs,
    )
    return args.astra_url


def normalize(value: Any) -> Any:
    if isinstance(value, dict):
        if "error" in value:
            return {"error": value["error"]}
        result: dict[str, Any] = {}
        if value.get("status") not in (None, 200):
            result["status"] = value["status"]
        hits_value = value.get("hits")
        if isinstance(hits_value, dict):
            result["hits"] = [
                {"source": hit.get("_source"), "sort": hit.get("sort")}
                for hit in hits_value.get("hits", [])
            ]
        elif hits_value is not None:
            result["hits"] = hits_value
        if value.get("aggregations") is not None:
            result["aggregations"] = normalize(value["aggregations"])
        if "buckets" in value:
            result["buckets"] = [normalize(bucket) for bucket in value["buckets"]]
        for key in ("key", "key_as_string", "doc_count", "value"):
            if key in value:
                result[key] = value[key]
        for key, child in sorted(value.items()):
            if key in IGNORED_RESPONSE_KEYS:
                continue
            if key not in {
                "hits",
                "aggregations",
                "buckets",
                "key",
                "key_as_string",
                "doc_count",
                "value",
                "status",
            }:
                child_normalized = normalize(child)
                if child_normalized not in ({}, [], None):
                    result[key] = child_normalized
        return result
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def search(base_url: str, index: str, query: dict[str, Any]) -> Response:
    return request("POST", f"{base_url}/{index}/_search", query)


def write_response_file(out_dir: str, query_id: str, backend: str, response: Response) -> None:
    """Persist one backend's response for a query: HTTP status, raw body, and the
    normalized form used for the diff. One file per backend so the pair is easy to
    diff directly (e.g. `diff out/Q18.opensearch.json out/Q18.astra.json`)."""
    path = Path(out_dir) / f"{query_id}.{backend}.json"
    path.write_text(
        json.dumps(
            {
                "query_id": query_id,
                "backend": backend,
                "status": response.status,
                "body": response.body,
                "normalized": normalize(response.body),
            },
            indent=2,
            sort_keys=True,
        )
    )


def compare(
    open_search_url: str,
    astra_url: str | None,
    index: str,
    run_id: str,
    verbose: bool = False,
    out_dir: str | None = None,
) -> int:
    failures = 0
    results: list[tuple[str, str, int | None, int | None]] = []
    if out_dir is not None:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
    for clickbench_query in benchmark_queries(run_id):
        print(f"\n== {clickbench_query.query_id} ==")
        print(clickbench_query.sql)

        os_response = search(open_search_url, index, clickbench_query.dsl)
        os_normalized = normalize(os_response.body)
        print(f"opensearch status={os_response.status}")
        if out_dir is not None:
            write_response_file(out_dir, clickbench_query.query_id, "opensearch", os_response)

        if astra_url is None:
            if os_response.status != 200:
                failures += 1
            status = "opensearch_only" if os_response.status == 200 else "opensearch_error"
            results.append((clickbench_query.query_id, status, os_response.status, None))
            print(json.dumps(os_normalized, indent=2, sort_keys=True))
            continue

        astra_response = search(astra_url.rstrip("/"), index, clickbench_query.dsl)
        astra_normalized = normalize(astra_response.body)
        if out_dir is not None:
            write_response_file(out_dir, clickbench_query.query_id, "astra", astra_response)
        # A non-200 from either side is a failure even if the bodies happen to
        # match (e.g. both error) — an error pair must never read as green.
        is_match = (
            os_response.status == 200
            and astra_response.status == 200
            and os_normalized == astra_normalized
        )
        if is_match:
            results.append(
                (clickbench_query.query_id, "match", os_response.status, astra_response.status)
            )
            print("match")
        else:
            failures += 1
            results.append(
                (
                    clickbench_query.query_id,
                    "mismatch",
                    os_response.status,
                    astra_response.status,
                )
            )
            print(
                f"mismatch: opensearch status={os_response.status} "
                f"astra status={astra_response.status}"
            )
        if verbose or not is_match:
            print(f"opensearch (status={os_response.status}):")
            print(json.dumps(os_normalized, indent=2, sort_keys=True))
            print(f"astra (status={astra_response.status}):")
            print(json.dumps(astra_normalized, indent=2, sort_keys=True))

    print("\n== summary ==")
    if out_dir is not None:
        print(f"per-backend responses written to {out_dir}/<Qn>.opensearch.json and <Qn>.astra.json")
    matches = sum(1 for _, status, _, _ in results if status == "match")
    print(f"matches={matches} failures={failures} total={len(results)}")
    for query_id, status, os_status, astra_status in results:
        if status in ("opensearch_only", "opensearch_error"):
            print(f"{query_id}: {status} (opensearch_status={os_status})")
        else:
            print(
                f"{query_id}: {status} "
                f"(opensearch_status={os_status}, astra_status={astra_status})"
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--run-id", default=f"clickbench-{int(time.time())}")
    parser.add_argument("--opensearch-url", default=None)
    parser.add_argument(
        "--astra-url",
        default=None,
        help="Optional Astra query base URL, e.g. http://localhost:8081",
    )
    parser.add_argument(
        "--start-astra",
        action="store_true",
        help="Build/start the local Astra Docker Compose stack and load the fixture into it.",
    )
    parser.add_argument("--astra-bulk-url", default=DEFAULT_ASTRA_BULK_URL)
    parser.add_argument("--astra-compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    parser.add_argument("--astra-timeout-secs", type=int, default=DEFAULT_ASTRA_TIMEOUT_SECS)
    parser.add_argument("--astra-zk-prefix", default="ASTRA")
    parser.add_argument(
        "--skip-astra-build",
        action="store_true",
        help="Use the existing slackhq/astra image instead of rebuilding it.",
    )
    parser.add_argument(
        "--reset-astra",
        action="store_true",
        help="Run docker compose down -v before starting Astra. This deletes local Astra/Kafka/ZK state.",
    )
    parser.add_argument("--keep-container", action="store_true")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print each backend's normalized response for every query, not just on mismatch.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Write each backend's full response (status + raw body + normalized) to "
        "<out-dir>/<Qn>.opensearch.json and <Qn>.astra.json, one file per backend per query.",
    )
    args = parser.parse_args()

    started_container = False
    open_search_url = args.opensearch_url or f"http://localhost:{args.port}"
    docs = fixture_documents(args.run_id)
    try:
        if args.opensearch_url is None:
            container_id = start_opensearch(args.image, args.port)
            started_container = True
            print(f"started OpenSearch container {container_id[:12]} at {open_search_url}")
            wait_for_endpoint(open_search_url)

        load_fixture(open_search_url, args.index, docs)
        astra_url = args.astra_url
        if args.start_astra:
            args.astra_url = args.astra_url or DEFAULT_ASTRA_QUERY_URL
            astra_url = start_and_load_astra(args, docs)

        failures = compare(
            open_search_url, astra_url, args.index, args.run_id, args.verbose, args.out_dir
        )
        return 1 if failures else 0
    finally:
        if started_container and not args.keep_container:
            run([*docker_prefix(), "docker", "rm", "-f", CONTAINER_NAME], check=False)


if __name__ == "__main__":
    sys.exit(main())
