<show-structure for="chapter,procedure" depth="2"/>

# Traces

Send traces to KalDB with OpenTelemetry, then open a known trace ID in Grafana through the Zipkin
compatible API. The Zipkin service, span, and trace search endpoints return empty arrays; Grafana
cannot discover traces through those endpoints.

## Sending traces with OpenTelemetry Collector

KalDB accepts OTLP/HTTP traces at `http://kaldb:8086/v1/traces` on the preprocessor. Replace
`kaldb` with a hostname reachable from your Collector. Applications can send OTLP over gRPC or
HTTP to the Collector; the Collector forwards traces to KalDB over HTTP.

First provision a trace dataset and its Kafka partition assignments using the
[manager API](API-manager.md). This example uses the routing key `otel_traces`; set the dataset's
`serviceNamePattern` to `otel_traces` (or use an existing `_all` route). The exporter header selects
a routing key and does not create a dataset. Matching datasets are checked in descending throughput
order, so a wildcard route with higher throughput can take precedence over an exact match. See
[dataset routing](Config-options.md#request-header-override) before using overlapping routes.

Save the following as `otel-collector.yaml`. It defines a complete trace pipeline, including a
memory limiter, bounded batches, an in-memory export queue, and retries:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 256
    spike_limit_mib: 64
  batch:
    timeout: 1s
    send_batch_size: 512
    send_batch_max_size: 1024

exporters:
  otlp_http/kaldb:
    endpoint: http://kaldb:8086
    encoding: proto
    compression: gzip
    headers:
      X-KalDB-Dataset: otel_traces
    retry_on_failure:
      enabled: true
    sending_queue:
      enabled: true
      num_consumers: 4
      queue_size: 1000

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp_http/kaldb]
```

The example uses the `otlp_http` exporter name in Collector Contrib 0.160.0. Older Collectors
use `otlphttp`; change both exporter references if needed. Validate and start the Collector:

```bash
otelcol-contrib validate --config=otel-collector.yaml
otelcol-contrib --config=otel-collector.yaml
```

The [OTLP HTTP exporter](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/otlphttpexporter/README.md)
appends `/v1/traces` to `endpoint`; do not append it yourself. Protobuf avoids KalDB's extra JSON
conversion work. KalDB's HTTP server decodes gzip request bodies, so the example keeps the
exporter's default compression. Adjust batch sizes and memory limits for your span sizes
and available memory; the batch limit counts spans, not bytes.

Set your application's trace exporter to the Collector's HTTP receiver, replacing `collector`
with its reachable hostname:

```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector:4318/v1/traces
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
```

`X-KalDB-Dataset` overrides KalDB's configured default for every span in a request. Remove the
`headers` block to use `preprocessorConfig.otlpTraceDatasetName`, or `otel_traces` when that setting
is omitted or blank. The producing service is preserved as `resource.service.name`; routing
`service_name` uses the selected dataset key. See the
[dataset configuration](Config-options.md#otlptracedatasetname) for header validation and routing details.

For Collector retries on dataset rate limiting, configure the KalDB preprocessor to return HTTP
429. Its default HTTP 400 is a permanent error to the Collector and will not be retried:

```yaml
preprocessorConfig:
  rateLimitExceededErrorCode: 429
```

This endpoint accepts traces only. Keep logs and metrics in separate Collector pipelines with
their own supported destinations.

## Indexing schema

In order to render traces using the [Zipkin compatible API](API-zipkin.md), the following fields should be present at index time.

<deflist type="medium">
<def title="_timesinceepoch">Epoch time in milliseconds</def>
<def title="trace_id">Unique trace ID</def>
<def title="service_name">Dataset routing key for OTLP spans; the producing service is preserved in resource.service.name</def>
<def title="name">String identifying the span</def>
<def title="duration">Duration in microseconds</def>
</deflist>


## Grafana trace visualization

![Astra trace visualization in Grafana](../assets/images/astra_grafana_trace.png){border-effect="line" width="700" thumbnail="true"}

_Example trace from Astra, rendered in Grafana_

### Zipkin datasource configuration
When using Grafana 10 you can directly connect a Zipkin datasource to your Astra installation.

![Grafana datasource configuration example](../assets/images/traces-datasource-config.png){border-effect="line" width="700" thumbnail="true"}

<deflist type="medium">
<def title="Required fields">

**URL** - Astra address, accessible by Grafana
</def>
</deflist>

### Astra data links

To enable quick toggling between Trace logs visualizations and waterfall, add a data link to your Astra traces
datasource that contains an internal link to the Astra zipkin datasource.

![Astra datasource link](../assets/images/astra_datasource_link.png){border-effect="line" width="700" thumbnail="true"}

<deflist type="medium">
<def title="Field">
<code>trace_id</code>
</def>
<def title="Query">
<code>${__value.raw}</code>
</def>
<def title="Internal link">
<code>true</code> 
</def>
</deflist>

![Astra datasource link example](../assets/images/astra_datasource_link_demo.png){border-effect="line" width="700" thumbnail="true"}

_Example data link from Astra to Zipkin datasource_


<seealso>
       <category ref="related">
            <a href="API-zipkin.md" />
        </category>
</seealso>
