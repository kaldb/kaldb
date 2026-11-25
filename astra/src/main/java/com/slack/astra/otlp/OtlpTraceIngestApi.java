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
import com.linecorp.armeria.server.annotation.Blocking;
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
import java.util.regex.Pattern;
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
  private static final String DATASET_HEADER = "X-KalDB-Dataset";
  private static final Pattern DATASET_NAME = Pattern.compile("[a-zA-Z0-9_-]{1,256}");

  private final BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private final DatasetRateLimitingService datasetRateLimitingService;
  private final MeterRegistry meterRegistry;
  private final int rateLimitExceededErrorCode;
  private final String defaultTraceDatasetName;
  private final Schema.IngestSchema schema;
  private final Counter incomingByteTotal;
  private final Counter incomingDocsTotal;
  private final Counter otlpTraceErrorCounter;
  private final Timer otlpTraceTimer;

  /**
   * Creates an OTLP trace ingest API using the configured dataset when a request has no override.
   */
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
    this.defaultTraceDatasetName =
        OtlpTraceRequestParser.normalizeTraceDatasetName(traceDatasetName);
    this.schema = schema;
    this.incomingByteTotal = meterRegistry.counter(OTLP_TRACE_INCOMING_BYTE_TOTAL);
    this.incomingDocsTotal = meterRegistry.counter(OTLP_TRACE_INCOMING_DOCS);
    this.otlpTraceErrorCounter = meterRegistry.counter(OTLP_TRACE_ERROR);
    this.otlpTraceTimer = meterRegistry.timer(OTLP_TRACE_TIMER);
  }

  /**
   * Handles OTLP/HTTP trace exports on Armeria's shared blocking executor while waiting for Kafka.
   * A valid X-KalDB-Dataset header overrides the configured destination for this request.
   */
  @Blocking
  @Post("/v1/traces")
  public HttpResponse ingestTraces(AggregatedHttpRequest request) {
    Timer.Sample sample = Timer.start(meterRegistry);

    try {
      byte[] body = request.content().array();
      incomingByteTotal.increment(body.length);

      String datasetName = defaultTraceDatasetName;
      List<String> datasetHeaders = request.headers().getAll(DATASET_HEADER);
      if (!datasetHeaders.isEmpty()) {
        datasetName = datasetHeaders.getFirst().strip();
        if (datasetHeaders.size() != 1 || !DATASET_NAME.matcher(datasetName).matches()) {
          return errorResponse(BAD_REQUEST, "Invalid X-KalDB-Dataset header");
        }
      }

      Map<String, List<Trace.Span>> docs = parseRequest(request.contentType(), body, datasetName);
      int totalDocs = docs.values().stream().mapToInt(List::size).sum();
      incomingDocsTotal.increment(totalDocs);

      for (Map.Entry<String, List<Trace.Span>> indexDocs : docs.entrySet()) {
        if (!datasetRateLimitingService.tryAcquire(indexDocs.getKey(), indexDocs.getValue())) {
          return HttpResponse.ofJson(
              HttpStatus.valueOf(rateLimitExceededErrorCode),
              new BulkIngestResponse(0, 0, "rate limit exceeded"));
        }
      }

      BulkIngestResponse response = bulkIngestKafkaProducer.submitRequest(docs).getResponse();
      if (response.failedDocs() > 0) {
        return errorResponse(INTERNAL_SERVER_ERROR, response.errorMsg());
      }
      return successResponse(request.contentType());
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Unable to parse OTLP trace request", e);
      return errorResponse(BAD_REQUEST, "Unable to parse OTLP trace request");
    } catch (UnsupportedOperationException e) {
      LOG.warn("Unsupported OTLP trace content type", e);
      return errorResponse(UNSUPPORTED_MEDIA_TYPE, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("OTLP trace ingest request interrupted", e);
      return errorResponse(INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (Exception e) {
      LOG.error("OTLP trace ingest request failed", e);
      return errorResponse(INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      sample.stop(otlpTraceTimer);
    }
  }

  private Map<String, List<Trace.Span>> parseRequest(
      MediaType contentType, byte[] body, String datasetName)
      throws InvalidProtocolBufferException {
    if (contentType == null || contentType.isProtobuf()) {
      return OtlpTraceRequestParser.parseProtobuf(body, datasetName, schema);
    }
    if (contentType.isJson()) {
      return OtlpTraceRequestParser.parseJson(
          new String(body, StandardCharsets.UTF_8), datasetName, schema);
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

  private HttpResponse errorResponse(HttpStatus status, String message) {
    otlpTraceErrorCounter.increment();
    return HttpResponse.ofJson(status, new BulkIngestResponse(0, 0, message));
  }
}
