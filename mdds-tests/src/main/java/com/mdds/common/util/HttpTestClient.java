/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class HttpTestClient {
  private final HttpClient client;
  private final String baseUrl;

  public HttpTestClient(String host, int port) {
    this.client = HttpClient.newHttpClient();
    this.baseUrl = "http://" + host + ":" + port;
  }

  public HttpResponse<String> get(String path) throws IOException, InterruptedException {
    var request = HttpRequest.newBuilder().uri(URI.create(baseUrl + path)).GET().build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> postSolve(
      String dataSourceType, String slaeSolvingMethod, Map<String, Object> params)
      throws IOException, InterruptedException {

    Map<String, Object> body = new HashMap<>();
    body.put("dataSourceType", dataSourceType);
    body.put("slaeSolvingMethod", slaeSolvingMethod);
    body.put("params", params);

    var jsonBody = JsonHelper.toJson(body);

    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/solve"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build();

    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
