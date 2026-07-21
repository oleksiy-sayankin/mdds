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
import java.nio.file.Path;
import java.util.Map;

/** Small HTTP client utility for integration tests. */
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

  public HttpResponse<String> get(String path, Map<String, String> headers)
      throws IOException, InterruptedException {
    var request = HttpRequest.newBuilder().uri(URI.create(baseUrl + path)).GET();
    for (var entry : headers.entrySet()) {
      request.header(entry.getKey(), entry.getValue());
    }
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> put(String path, Path fileToSend)
      throws IOException, InterruptedException {
    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(path))
            .PUT(HttpRequest.BodyPublishers.ofFile(fileToSend))
            .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> put(String path, Map<String, String> headers, String rawJson)
      throws IOException, InterruptedException {
    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .PUT(HttpRequest.BodyPublishers.ofString(rawJson));

    for (var entry : headers.entrySet()) {
      request.header(entry.getKey(), entry.getValue());
    }
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> patch(String path, Map<String, String> headers, String rawJson)
      throws IOException, InterruptedException {
    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .method("PATCH", HttpRequest.BodyPublishers.ofString(rawJson));

    for (var entry : headers.entrySet()) {
      request.header(entry.getKey(), entry.getValue());
    }
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> post(String path, Map<String, String> headers, Object body)
      throws IOException, InterruptedException {

    var jsonBody = JsonHelper.toJson(body);

    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody));

    for (var entry : headers.entrySet()) {
      request.header(entry.getKey(), entry.getValue());
    }
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> post(String path, Map<String, String> headers)
      throws IOException, InterruptedException {

    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .POST(HttpRequest.BodyPublishers.noBody());

    for (var entry : headers.entrySet()) {
      request.header(entry.getKey(), entry.getValue());
    }
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  public HttpResponse<String> post(String path, Map<String, String> headers, String rawJson)
      throws IOException, InterruptedException {

    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .POST(HttpRequest.BodyPublishers.ofString(rawJson));

    for (var entry : headers.entrySet()) {
      request.header(entry.getKey(), entry.getValue());
    }
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
