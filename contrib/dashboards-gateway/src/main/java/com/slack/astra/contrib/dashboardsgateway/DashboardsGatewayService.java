package com.slack.astra.contrib.dashboardsgateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.ContentTooLargeException;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpRequestDuplicator;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DashboardsGatewayService implements HttpService {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardsGatewayService.class);
  private static final Set<String> HOP_BY_HOP_HEADERS =
      Set.of(
          "connection",
          "content-length",
          "host",
          "keep-alive",
          "proxy-authenticate",
          "proxy-authorization",
          "te",
          "trailer",
          "transfer-encoding",
          "upgrade");
  private static final DashboardsGatewayRouting ROUTING = new DashboardsGatewayRouting();

  private final GatewayConfig config;
  private final Map<Upstream, WebClient> upstreamClients;

  DashboardsGatewayService(GatewayConfig config) {
    this.config = config;
    this.upstreamClients =
        Map.of(
            Upstream.ASTRA, newWebClient(config.astraUri(), config.requestTimeout()),
            Upstream.OPENSEARCH, newWebClient(config.openSearchUri(), config.requestTimeout()));
  }

  @Override
  public HttpResponse serve(ServiceRequestContext ctx, HttpRequest req) {
    String routingPath = ctx.path();
    String forwardPath = req.path();

    return ROUTING
        .syntheticResponse(routingPath)
        .orElseGet(
            () -> {
              HttpRequestDuplicator duplicator =
                  req.toDuplicator(ctx.eventLoop(), config.maxRequestBytes());
              CompletableFuture<HttpResponse> proxiedResponse =
                  duplicator
                      .duplicate()
                      .aggregate(ctx.eventLoop())
                      .thenCompose(request -> forward(request, routingPath, forwardPath))
                      .exceptionally(
                          throwable ->
                              aggregationErrorResponse(
                                  forwardPath, config.maxRequestBytes(), throwable));
              proxiedResponse.whenComplete((unused, unusedThrowable) -> duplicator.close());
              return HttpResponse.from(proxiedResponse);
            });
  }

  private CompletableFuture<HttpResponse> forward(
      AggregatedHttpRequest request, String routingPath, String forwardPath) {
    Upstream upstream = ROUTING.selectUpstream(routingPath);
    URI upstreamUri = config.uriFor(upstream).resolve(forwardPath);

    try {
      byte[] requestBody = request.content().array();
      RequestHeadersBuilder requestHeadersBuilder =
          RequestHeaders.builder(request.method(), forwardPath);
      copyRequestHeaders(request.headers(), requestHeadersBuilder);
      HttpRequest upstreamRequest =
          requestBody.length == 0
              ? HttpRequest.of(requestHeadersBuilder.build())
              : HttpRequest.of(requestHeadersBuilder.build(), HttpData.wrap(requestBody));

      LOG.debug("Forwarding {} {} to {}", request.method(), forwardPath, upstreamUri);

      return upstreamClients
          .get(upstream)
          .execute(upstreamRequest)
          .aggregate()
          .thenApply(response -> HttpResponse.of(response.headers(), response.content()))
          .exceptionally(throwable -> gatewayErrorResponse(upstreamUri, throwable));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(gatewayErrorResponse(upstreamUri, e));
    }
  }

  private static HttpResponse aggregationErrorResponse(
      String path, int maxRequestBytes, Throwable throwable) {
    if (containsCause(throwable, ContentTooLargeException.class)) {
      return payloadTooLargeResponse(path, maxRequestBytes);
    }
    return requestProcessingErrorResponse(path, throwable);
  }

  private static void copyRequestHeaders(
      RequestHeaders requestHeaders, RequestHeadersBuilder requestHeadersBuilder) {
    requestHeaders.forEach(
        header -> {
          String name = header.getKey().toString();
          String lowerCaseName = name.toLowerCase(Locale.ROOT);
          if (name.startsWith(":") || HOP_BY_HOP_HEADERS.contains(lowerCaseName)) {
            return;
          }
          requestHeadersBuilder.add(name, header.getValue());
        });
  }

  private static HttpResponse gatewayErrorResponse(URI upstreamUri, Throwable throwable) {
    String errorMessage =
        "gateway error forwarding to %s: %s%n"
            .formatted(
                upstreamUri.getAuthority(),
                throwable.getMessage() == null
                    ? throwable.getClass().getName()
                    : throwable.getMessage());
    LOG.error(errorMessage, throwable);
    return HttpResponse.of(HttpStatus.BAD_GATEWAY, MediaType.PLAIN_TEXT_UTF_8, errorMessage);
  }

  private static HttpResponse requestProcessingErrorResponse(String path, Throwable throwable) {
    String errorMessage =
        "gateway error processing %s: %s%n"
            .formatted(
                path,
                throwable.getMessage() == null
                    ? throwable.getClass().getName()
                    : throwable.getMessage());
    LOG.error(errorMessage, throwable);
    return HttpResponse.of(HttpStatus.BAD_GATEWAY, MediaType.PLAIN_TEXT_UTF_8, errorMessage);
  }

  private static HttpResponse payloadTooLargeResponse(String path, int maxRequestBytes) {
    String errorMessage =
        "gateway rejected %s because the request body exceeded the configured size limit of %s bytes%n"
            .formatted(path, maxRequestBytes);
    return HttpResponse.of(
        HttpStatus.REQUEST_ENTITY_TOO_LARGE, MediaType.PLAIN_TEXT_UTF_8, errorMessage);
  }

  private static boolean containsCause(Throwable throwable, Class<? extends Throwable> causeType) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (causeType.isInstance(current)) {
        return true;
      }
    }
    return false;
  }

  private static WebClient newWebClient(URI baseUri, Duration requestTimeout) {
    return WebClient.builder(baseUri.toString()).responseTimeout(requestTimeout).build();
  }
}
