/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

final class PresignedUrlAssertions {
  private static final DateTimeFormatter AWS_SIGNING_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX").withZone(ZoneOffset.UTC);

  private PresignedUrlAssertions() {}

  static void assertExpiresAtMatchesSignature(
      Instant actualExpiresAt, URL presignedUrl, Duration expectedTtl) throws URISyntaxException {
    var signedAt = extractSigningTime(presignedUrl);
    var signatureDuration = extractSignatureDuration(presignedUrl);
    var expectedExpiresAt = signedAt.plus(signatureDuration);

    assertThat(signatureDuration).isEqualTo(expectedTtl);
    assertThat(actualExpiresAt)
        .isAfterOrEqualTo(expectedExpiresAt.minusSeconds(1))
        .isBeforeOrEqualTo(expectedExpiresAt.plusSeconds(1));
  }

  static void assertExpiresAtMatchesSignature(
      Instant actualExpiresAt, String presignedUrl, Duration expectedTtl) {
    var signedAt = extractSigningTime(presignedUrl);
    var signatureDuration = extractSignatureDuration(presignedUrl);
    var expectedExpiresAt = signedAt.plus(signatureDuration);

    assertThat(signatureDuration).isEqualTo(expectedTtl);
    assertThat(actualExpiresAt)
        .isAfterOrEqualTo(expectedExpiresAt.minusSeconds(1))
        .isBeforeOrEqualTo(expectedExpiresAt.plusSeconds(1));
  }

  private static Duration extractSignatureDuration(String uploadUrl) {
    var seconds = Long.parseLong(queryParameter(uploadUrl, "X-Amz-Expires"));
    return Duration.ofSeconds(seconds);
  }

  private static Instant extractSigningTime(String uploadUrl) {
    var value = queryParameter(uploadUrl, "X-Amz-Date");
    return Instant.from(AWS_SIGNING_TIME_FORMATTER.parse(value));
  }

  private static Instant extractSigningTime(URL uploadUrl) throws URISyntaxException {
    var value = queryParameter(uploadUrl, "X-Amz-Date");
    return Instant.from(AWS_SIGNING_TIME_FORMATTER.parse(value));
  }

  private static Duration extractSignatureDuration(URL uploadUrl) throws URISyntaxException {
    var seconds = Long.parseLong(queryParameter(uploadUrl, "X-Amz-Expires"));
    return Duration.ofSeconds(seconds);
  }

  private static String queryParameter(URL url, String name) throws URISyntaxException {
    return Stream.of(url.toURI().getRawQuery().split("&"))
        .map(parameter -> parameter.split("=", 2))
        .filter(parts -> parts.length == 2)
        .filter(parts -> URLDecoder.decode(parts[0], UTF_8).equals(name))
        .map(parts -> URLDecoder.decode(parts[1], UTF_8))
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError(
                    "Query parameter '" + name + "' is missing from presigned URL."));
  }

  private static String queryParameter(String url, String name) {
    return Stream.of(URI.create(url).getRawQuery().split("&"))
        .map(parameter -> parameter.split("=", 2))
        .filter(parts -> parts.length == 2)
        .filter(parts -> URLDecoder.decode(parts[0], UTF_8).equals(name))
        .map(parts -> URLDecoder.decode(parts[1], UTF_8))
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError(
                    "Query parameter '" + name + "' is missing from presigned URL."));
  }
}
