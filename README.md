# KalDB

[![CI](https://img.shields.io/github/actions/workflow/status/kaldb/kaldb/maven.yml?branch=kaldb-main&label=ci)](https://github.com/kaldb/kaldb/actions/workflows/maven.yml)
[![License](https://img.shields.io/github/license/kaldb/kaldb)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-kaldb.com-blue)](https://kaldb.com/docs/)
[![GitHub stars](https://img.shields.io/github/stars/kaldb/kaldb?style=social)](https://github.com/kaldb/kaldb)

KalDB is a cloud-native log search and analytics engine built for high-volume observability workloads. It combines OpenSearch-compatible APIs with a decoupled architecture: Kafka for durable ingest, Lucene for indexing, and S3 for indexed storage.

[Quick Start](#quick-start) • [Architecture](#architecture-overview) • [Docs](https://kaldb.com/docs/) • [Talks](#talks-and-architecture-deep-dives) • [Contributing](.github/CONTRIBUTING.md)

> KalDB builds on the Astra codebase originally open-sourced by Slack and reflects production learnings from large-scale deployments at Slack and Airbnb. The project is in transition, and parts of the codebase, APIs, and docs still use the original `Astra` name.

## Why KalDB

- Built for log-heavy workloads with spiky ingest patterns and long retention requirements.
- OpenSearch-compatible ingest and query APIs reduce migration work for existing pipelines and dashboards.
- Decoupled compute and storage lets you scale indexing and querying separately.
- Native support for logs, traces, and audit-style event data.
- Designed to work well with Grafana, Zipkin-compatible trace tooling, and cloud object storage.

## Why It Exists

KalDB was created to solve the practical problems that show up when log search becomes large enough to hurt: ingest delays during traffic spikes, schema conflicts from fast-moving services, and operational complexity from running many large search clusters. The architecture and public talks around KalDB focus on a simple idea: keep the search experience fast, but move durability and long-term storage onto systems that scale more naturally for cloud-native environments.

## Architecture Overview

KalDB is a Lucene-based system inspired by aggregator/leaf/tailer style architectures. Kafka handles durable ingest, S3 stores indexed data, and stateless indexers can be scaled independently to keep fresh data searchable during spikes.

```mermaid
flowchart TD
    producers[Log shippers / bulk ingest] --> preprocessor[Preprocessor]
    grafana[Grafana / clients] --> query[Query]

    preprocessor --> kafka[(Kafka)]
    kafka --> indexers[Indexers]
    kafka --> recovery[Recovery]

    indexers --> s3[(S3 / object storage)]
    recovery --> s3

    query --> indexers
    query --> cache[Cache]
    cache <--> s3

    manager[Manager] <--> zk[(ZooKeeper)]
    manager <--> s3
```

Core properties of the design:

- Fresh-data ingest is prioritized during peak load.
- Indexer nodes are stateless, which makes horizontal scaling straightforward.
- Storage and query paths are separated so long retention does not force expensive hot storage everywhere.
- Existing OpenSearch-oriented tooling can often be reused instead of rewritten.

## Quick Start

If you want the shortest path to a local cluster, use the helper script:

```bash
./quick_start.sh --clean
```

Local endpoints:

- Query API: `http://localhost:8081`
- Admin UI: `http://localhost:8083/admin/`
- Manager API: `http://localhost:8083`
- Preprocessor ingest API: `http://localhost:8086`
- Grafana: `http://localhost:3000/explore`

For the manual curl workflow, API examples, and the per-service setup details, see [docs/topics/Getting-started.md](docs/topics/Getting-started.md).

## What You Can Build With It

### Log search with Lucene-style queries

KalDB supports Lucene query syntax for field filters, wildcards, boolean queries, ranges, and optional full-text search. See [docs/topics/Logs.md](docs/topics/Logs.md) for examples and Grafana integration details.

### Trace search with a Zipkin-compatible API

KalDB can store and serve traces when the required span fields are indexed. See [docs/topics/Traces.md](docs/topics/Traces.md) for the expected schema and Grafana setup.

### OpenSearch-oriented migrations

KalDB exposes OpenSearch-compatible APIs for query and ingest workflows, which helps reuse existing shippers, clients, and dashboards. See [docs/topics/Migrating.md](docs/topics/Migrating.md) and [docs/topics/API-opensearch.md](docs/topics/API-opensearch.md).

## Developer Setup

### Local development

- Import the repository as a Maven project in IntelliJ.
- The `.run/` directory contains run configurations for each node role.
- Shared runtime defaults live in `config/config.yaml`.
- CI builds with JDK 21.

### Common commands

```bash
mvn package
```

### Repository map

- `astra/`: core server, APIs, indexing, query, metadata, and node implementations
- `config/`: local configuration and Grafana provisioning
- `docs/`: architecture, operations, APIs, and getting-started material
- `benchmarks/`: JMH benchmarks for indexing and API workloads
- `tools/`: helper tools such as the span generator and UI tests

## Learn More

- [KalDB website](https://kaldb.com/)
- [Documentation](https://kaldb.com/docs/)
- [Getting started guide](docs/topics/Getting-started.md)
- [Architecture guide](docs/topics/Architecture.md)
- [Cluster recommendations](docs/topics/Recommendations.md)
- [Roadmap](docs/topics/Roadmap.md)

## Talks And Architecture Deep Dives

- Monitorama 2022: KalDB: A k8s Native Log Search Platform
- Strange Loop 2022: KalDB: A Cloud Native Log Search Platform
- Berlin Buzzwords 2023: KalDB: Serverless Lucene at Petabyte Scale
- SREcon APAC 2023: Taming Spiky Log Volumes with KalDB

The public talks complement the architecture docs well: the docs explain the components, while the talks explain the operational pressure and design tradeoffs that shaped them.

## Contributing

Contributions to code, docs, tests, and developer tooling are all useful. Start with [.github/CONTRIBUTING.md](.github/CONTRIBUTING.md), check open issues, and look for a good first issue if you want a smaller entry point.

## License

KalDB is released under the [MIT License](LICENSE).
