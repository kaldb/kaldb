# Getting started

> IntelliJ: Import the project as a Maven project.

IntelliJ run configs are provided for all node types, and execute using the provided `config/config.yaml`. These
configurations are stored in the `.run` folder and should automatically be detected by IntelliJ upon importing the
project.

## Quick start

1. Build and run docker compose to bring up dependencies and Astra nodes
```bash
docker build -t slackhq/astra .

docker compose up -d
```

2. In Kafka container terminal, create input topic (preprocessor crashes if it does not exist before configuring manager in next step)
```bash
kafka-topics.sh --create --topic test-topic --if-not-exists --bootstrap-server localhost:9092
```

3. Run 2 curl commands to configure 1 partition

```bash
curl -XPOST -H 'content-type: application/json; charset=utf-8; protocol=gRPC' http://localhost:8083'/slack.proto.astra.ManagerApiService/CreateDatasetMetadata' -d '{
  "name": "test",
  "owner": "test@email.com",
  "serviceNamePattern": "_all"
}'

curl -XPOST -H 'content-type: application/json; charset=utf-8; protocol=gRPC' http://localhost:8083'/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment' -d '{
  "name": "test",
  "throughputBytes": "4000000",
  "partitionIds": ["0"]
}'
```
This can optionally be achieved in the manager UI at [http://localhost:8083/docs](http://localhost:8083/docs)

4. Add logs via bulk ingest
```bash
curl --location 'http://localhost:8086/_bulk' \
--header 'Content-type: application/x-ndjson' \
--data '{ "index" : { "_index" : "test", "_id" : "100" } }
{ "@timestamp": "2024-03-07T12:00:00.000Z", "level": "INFO", "message": "This is a log message", "service-name": "test" }
'
```

5. Example curl to read data
Note: This is similar to [ES _msearch](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-multi-search.html) but `size` is required, and the time window must be expressed as a `range` query on `@timestamp`.

```bash
curl --location 'http://localhost:8081/_msearch' \
--header 'Content-type: application/x-ndjson' \
--data '{ "index": "test"}
{"query":{"bool":{"must":[{"match_all":{}},{"range":{"@timestamp":{"gte":"2024-03-07T00:00:00Z","lte":"2100-01-01T00:00:00Z"}}}]}},"size":500}
'
```

Query via Grafana
```
http://localhost:3000/explore
```

Query via OpenSearch Dashboards
```
http://localhost:5601/app/discover
```

In OpenSearch Dashboards, create a data view named `test` and set the time field to `@timestamp`.
Dashboards queries Astra for user log data through the `astra_dashboards_gateway`, while
Dashboards' own saved objects and UI state are stored in the `opensearch` container's persistent
volume. This is the gateway model for Dashboards in this repo: the gateway is the single
OpenSearch-compatible endpoint that Dashboards talks to, and it routes user-index log/search APIs
to Astra while keeping Dashboards system-index traffic on OpenSearch. If you remove that
OpenSearch volume, your Dashboards data views and saved searches are lost, but the Astra log data
is unaffected.

For a continuous synthetic stream, run `tools/loadgen` directly:

```bash
mvn -f tools/loadgen/pom.xml -DskipTests package
INDEX=test BATCH_SIZE=25 INTERVAL_SEC=0.2 java -jar tools/loadgen/target/tools-loadgen.jar
```
