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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DashboardsGatewayService implements HttpService {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardsGatewayService.class);
  private static final byte[] GATEWAY_UPSTREAM_ERROR_RESPONSE =
      "gateway upstream request failed\n".getBytes(StandardCharsets.UTF_8);
  private static final byte[] GATEWAY_REQUEST_PROCESSING_ERROR_RESPONSE =
      "gateway request processing failed\n".getBytes(StandardCharsets.UTF_8);
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
  private final GatewayAuthorization authorization;

  DashboardsGatewayService(GatewayConfig config) {
    this.config = config;
    this.authorization = config.security() == null ? null : new GatewayAuthorization(config);
    this.upstreamClients =
        Map.of(
            Upstream.ASTRA,
            newWebClient(config.astraUri(), config.requestTimeout()),
            Upstream.OPENSEARCH,
            authorization == null
                ? newWebClient(config.openSearchUri(), config.requestTimeout())
                : authorization.nativeClient);
  }

  @Override
  public HttpResponse serve(ServiceRequestContext ctx, HttpRequest req) {
    String routingPath = ctx.path();
    String forwardPath = req.path();

    return (authorization == null || routingPath.equals(DashboardsGatewayRouting.HEALTH_PATH)
            ? ROUTING.syntheticResponse(routingPath)
            : java.util.Optional.<HttpResponse>empty())
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
    if (authorization != null) return forwardSecured(request, routingPath, forwardPath);
    Upstream upstream = ROUTING.selectUpstream(routingPath);
    return proxy(request, upstream, forwardPath, null, null);
  }

  private CompletableFuture<HttpResponse> forwardSecured(
      AggregatedHttpRequest request, String path, String forwardPath) {
    try {
      GatewayAuthorization.userAuthorization(request);
      AuthorizedSearch.Plan plan = AuthorizedSearch.inspect(request, config.security().datasets());
      if (plan.kind() == AuthorizedSearch.Kind.NATIVE)
        return proxy(request, Upstream.OPENSEARCH, forwardPath, null, null);
      return (plan.discovery()
              ? authorization.discover(request, plan.datasets())
              : authorization.authorize(request, plan.datasets()))
          .thenCompose(
              decision -> {
                if (plan.kind() == AuthorizedSearch.Kind.SEARCH
                    || plan.kind() == AuthorizedSearch.Kind.MSEARCH) {
                  AggregatedHttpRequest prepared =
                      AuthorizedSearch.prepare(request, plan, decision);
                  return proxy(prepared, Upstream.ASTRA, forwardPath, plan, decision);
                }
                return CompletableFuture.completedFuture(
                    HttpResponse.of(
                        MediaType.JSON_UTF_8,
                        AuthorizedSearch.metadata(plan, decision).toString()));
              })
          .exceptionally(DashboardsGatewayService::authorizationFailure);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(authorizationFailure(e));
    }
  }

  private static HttpResponse authorizationFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof GatewayAuthorization.Rejected rejection) {
        return HttpResponse.of(
            rejection.status,
            MediaType.JSON_UTF_8,
            "{\"error\":\"" + rejection.getMessage() + "\"}");
      }
    }
    return HttpResponse.of(
        HttpStatus.SERVICE_UNAVAILABLE,
        MediaType.JSON_UTF_8,
        "{\"error\":\"native authorization unavailable\"}");
  }

  private CompletableFuture<HttpResponse> proxy(
      AggregatedHttpRequest request,
      Upstream upstream,
      String forwardPath,
      AuthorizedSearch.Plan plan,
      GatewayAuthorization.Decision decision) {
    URI upstreamUri = config.uriFor(upstream).resolve(forwardPath);

    try {
      byte[] requestBody = request.content().array();
      RequestHeadersBuilder requestHeadersBuilder =
          RequestHeaders.builder(request.method(), forwardPath);
      copyRequestHeaders(
          request.headers(),
          requestHeadersBuilder,
          authorization != null,
          upstream == Upstream.ASTRA);
      HttpRequest upstreamRequest =
          requestBody.length == 0
              ? HttpRequest.of(requestHeadersBuilder.build())
              : HttpRequest.of(requestHeadersBuilder.build(), HttpData.wrap(requestBody));

      LOG.debug("Forwarding {} {} to {}", request.method(), forwardPath, upstreamUri);

      return upstreamClients
          .get(upstream)
          .execute(upstreamRequest)
          .aggregate()
          .thenApply(
              response ->
                  plan == null
                      ? HttpResponse.of(response.headers(), response.content())
                      : HttpResponse.of(
                          MediaType.JSON_UTF_8,
                          AuthorizedSearch.filterResponse(response, plan, decision).toString()))
          .exceptionally(
              throwable ->
                  plan == null
                      ? gatewayErrorResponse(upstreamUri, throwable)
                      : authorizationFailure(throwable));
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
      RequestHeaders requestHeaders,
      RequestHeadersBuilder requestHeadersBuilder,
      boolean secured,
      boolean toKaldb) {
    requestHeaders.forEach(
        header -> {
          String name = header.getKey().toString();
          String lowerCaseName = name.toLowerCase(Locale.ROOT);
          if (name.startsWith(":") || HOP_BY_HOP_HEADERS.contains(lowerCaseName)) {
            return;
          }
          if (secured
              && (lowerCaseName.startsWith("x-proxy-")
                  || lowerCaseName.startsWith("x-forwarded-")
                  || lowerCaseName.contains("opendistro")
                  || lowerCaseName.contains("opensearch")
                  || Set.of("x-user", "x-roles", "remote-user").contains(lowerCaseName)
                  || (toKaldb
                      && Set.of("authorization", "cookie", "securitytenant")
                          .contains(lowerCaseName)))) return;
          requestHeadersBuilder.add(name, header.getValue());
        });
  }

  static HttpResponse gatewayErrorResponse(URI upstreamUri, Throwable throwable) {
    LOG.error("Gateway error forwarding to {}", upstreamUri, throwable);
    return HttpResponse.of(
        HttpStatus.BAD_GATEWAY, MediaType.PLAIN_TEXT_UTF_8, GATEWAY_UPSTREAM_ERROR_RESPONSE);
  }

  static HttpResponse requestProcessingErrorResponse(String path, Throwable throwable) {
    LOG.error("Gateway error processing {}", path, throwable);
    return HttpResponse.of(
        HttpStatus.BAD_GATEWAY,
        MediaType.PLAIN_TEXT_UTF_8,
        GATEWAY_REQUEST_PROCESSING_ERROR_RESPONSE);
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
