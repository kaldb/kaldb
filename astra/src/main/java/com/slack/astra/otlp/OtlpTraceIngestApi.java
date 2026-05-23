package com.slack.astra.otlp;

import static com.linecorp.armeria.common.HttpStatus.BAD_REQUEST;
import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.UNSUPPORTED_MEDIA_TYPE;

import com.google.protobuf.InvalidProtocolBufferException;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.astra.bulkIngestApi.BulkIngestKafkaProducer;
import com.slack.astra.bulkIngestApi.BulkIngestResponse;
import com.slack.astra.bulkIngestApi.DatasetRateLimitingService;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OTLP/HTTP trace ingestion endpoint for the preprocessor. */
public class OtlpTraceIngestApi {
  private static final Logger LOG = LoggerFactory.getLogger(OtlpTraceIngestApi.class);

  private static final String OTLP_TRACE_INCOMING_BYTE_TOTAL =
      "astra_preprocessor_otlp_trace_incoming_byte";
  private static final String OTLP_TRACE_INCOMING_DOCS =
      "astra_preprocessor_otlp_trace_incoming_docs";
  private static final String OTLP_TRACE_ERROR = "astra_preprocessor_otlp_trace_error";
  private static final String OTLP_TRACE_TIMER = "astra_preprocessor_otlp_trace_ingest";

  private final BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private final DatasetRateLimitingService datasetRateLimitingService;
  private final MeterRegistry meterRegistry;
  private final int rateLimitExceededErrorCode;
  private final String traceDatasetName;
  private final Schema.IngestSchema schema;
  private final Counter incomingByteTotal;
  private final Counter incomingDocsTotal;
  private final Counter otlpTraceErrorCounter;
  private final Timer otlpTraceTimer;

  /** Creates an OTLP trace ingest API that writes converted spans to the bulk ingest producer. */
  public OtlpTraceIngestApi(
      BulkIngestKafkaProducer bulkIngestKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService,
      MeterRegistry meterRegistry,
      int rateLimitExceededErrorCode,
      String traceDatasetName,
      Schema.IngestSchema schema) {
    this.bulkIngestKafkaProducer = bulkIngestKafkaProducer;
    this.datasetRateLimitingService = datasetRateLimitingService;
    this.meterRegistry = meterRegistry;
    this.rateLimitExceededErrorCode =
        rateLimitExceededErrorCode <= 0 || rateLimitExceededErrorCode > 599
            ? BAD_REQUEST.code()
            : rateLimitExceededErrorCode;
    this.traceDatasetName = OtlpTraceRequestParser.normalizeTraceDatasetName(traceDatasetName);
    this.schema = schema;
    this.incomingByteTotal = meterRegistry.counter(OTLP_TRACE_INCOMING_BYTE_TOTAL);
    this.incomingDocsTotal = meterRegistry.counter(OTLP_TRACE_INCOMING_DOCS);
    this.otlpTraceErrorCounter = meterRegistry.counter(OTLP_TRACE_ERROR);
    this.otlpTraceTimer = meterRegistry.timer(OTLP_TRACE_TIMER);
  }

  /** Handles OTLP/HTTP trace export requests. */
  @Post("/v1/traces")
  public HttpResponse ingestTraces(AggregatedHttpRequest request) {
    CompletableFuture<HttpResponse> responseFuture = new CompletableFuture<>();
    Timer.Sample sample = Timer.start(meterRegistry);
    CompletableFuture<HttpResponse> timedFuture =
        responseFuture.whenComplete((response, error) -> sample.stop(otlpTraceTimer));

    try {
      byte[] body = request.content().array();
      incomingByteTotal.increment(body.length);

      Map<String, List<Trace.Span>> docs = parseRequest(request.contentType(), body);
      int totalDocs = docs.values().stream().mapToInt(List::size).sum();
      incomingDocsTotal.increment(totalDocs);

      for (Map.Entry<String, List<Trace.Span>> indexDocs : docs.entrySet()) {
        if (!datasetRateLimitingService.tryAcquire(indexDocs.getKey(), indexDocs.getValue())) {
          responseFuture.complete(
              HttpResponse.ofJson(
                  HttpStatus.valueOf(rateLimitExceededErrorCode),
                  new BulkIngestResponse(0, 0, "rate limit exceeded")));
          return HttpResponse.of(timedFuture);
        }
      }

      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  BulkIngestResponse response =
                      bulkIngestKafkaProducer.submitRequest(docs).getResponse();
                  if (response.failedDocs() > 0) {
                    completeError(responseFuture, INTERNAL_SERVER_ERROR, response.errorMsg());
                  } else {
                    responseFuture.complete(successResponse(request.contentType()));
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  LOG.error("OTLP trace ingest request interrupted", e);
                  completeError(responseFuture, INTERNAL_SERVER_ERROR, e.getMessage());
                } catch (Exception e) {
                  LOG.error("OTLP trace ingest request failed", e);
                  completeError(responseFuture, INTERNAL_SERVER_ERROR, e.getMessage());
                }
              });
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Unable to parse OTLP trace request", e);
      completeError(responseFuture, BAD_REQUEST, "Unable to parse OTLP trace request");
    } catch (UnsupportedOperationException e) {
      LOG.warn("Unsupported OTLP trace content type", e);
      completeError(responseFuture, UNSUPPORTED_MEDIA_TYPE, e.getMessage());
    } catch (Exception e) {
      LOG.error("OTLP trace ingest request failed", e);
      completeError(responseFuture, INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return HttpResponse.of(timedFuture);
  }

  private Map<String, List<Trace.Span>> parseRequest(MediaType contentType, byte[] body)
      throws InvalidProtocolBufferException {
    if (contentType == null || contentType.isProtobuf()) {
      return OtlpTraceRequestParser.parseProtobuf(body, traceDatasetName, schema);
    }
    if (contentType.isJson()) {
      return OtlpTraceRequestParser.parseJson(
          new String(body, StandardCharsets.UTF_8), traceDatasetName, schema);
    }
    throw new UnsupportedOperationException("unsupported content type: " + contentType);
  }

  private HttpResponse successResponse(MediaType requestContentType) {
    if (requestContentType != null && requestContentType.isJson()) {
      return HttpResponse.of(OK, MediaType.JSON, "{}");
    }
    return HttpResponse.of(
        OK, MediaType.X_PROTOBUF, ExportTraceServiceResponse.getDefaultInstance().toByteArray());
  }

  private void completeError(
      CompletableFuture<HttpResponse> responseFuture, HttpStatus status, String message) {
    otlpTraceErrorCounter.increment();
    responseFuture.complete(HttpResponse.ofJson(status, new BulkIngestResponse(0, 0, message)));
  }
}
