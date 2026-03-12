package com.slack.astra.contrib.dashboardsgateway;

import com.linecorp.armeria.server.Server;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DashboardsGatewayMain {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardsGatewayMain.class);

  private DashboardsGatewayMain() {}

  public static void main(String[] args) {
    GatewayConfig config = GatewayConfig.fromEnvironment();
    Server server =
        Server.builder()
            .http(new InetSocketAddress(config.listenHost(), config.listenPort()))
            .serviceUnder("/", new DashboardsGatewayService(config))
            .build();

    Runtime.getRuntime().addShutdownHook(new Thread(server::close, "dashboards-gateway-shutdown"));

    LOG.info(
        "Starting dashboards gateway on {}:{} with Astra={} OpenSearch={} maxRequestBytes={}",
        config.listenHost(),
        config.listenPort(),
        config.astraUri(),
        config.openSearchUri(),
        config.maxRequestBytes());
    server.start().join();
    server.whenClosed().join();
  }
}
