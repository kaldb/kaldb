package com.slack.astra.graphApi;

import com.slack.astra.zipkinApi.ZipkinEndpointResponse;
import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.util.HashMap;
import java.util.Map;

public class TestUtils {
  public static ZipkinSpanResponse createSpanWithTags(
      String id, String traceId, String parentId, Map<String, String> tags) {
    ZipkinSpanResponse span = new ZipkinSpanResponse(id, traceId);
    span.setParentId(parentId);
    span.setTags(new HashMap<>(tags));

    // Set up a default remote endpoint for configured tests
    ZipkinEndpointResponse remoteEndpoint = new ZipkinEndpointResponse();
    remoteEndpoint.setServiceName("default-service");
    span.setRemoteEndpoint(remoteEndpoint);

    return span;
  }
}
