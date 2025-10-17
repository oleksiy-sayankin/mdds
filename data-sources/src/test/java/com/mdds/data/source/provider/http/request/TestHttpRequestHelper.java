/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import static com.mdds.common.util.CsvHelper.convert;
import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractMatrix;
import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractRhs;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.Part;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestHttpRequestHelper {
  private HttpServletRequest request;

  @BeforeEach
  void setUp() {
    request = mock(HttpServletRequest.class);
  }

  @Test
  void testExtractMatrix() throws ServletException, IOException {
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var is = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var part = mock(Part.class);
    when(part.getInputStream()).thenReturn(is);
    when(request.getPart("matrix")).thenReturn(part);
    var actualMatrix = extractMatrix(request);
    actualMatrix.ifPresent(am -> assertArrayEquals(convert(expectedMatrix), am));
  }

  @Test
  void testExtractRhs() throws ServletException, IOException {
    var expectedRhs = new String[] {"1.3", "2.2", "3.7"};
    var is =
        new ByteArrayInputStream(String.join(System.lineSeparator(), expectedRhs).getBytes(UTF_8));
    var part = mock(Part.class);
    when(part.getInputStream()).thenReturn(is);
    when(request.getPart("rhs")).thenReturn(part);
    var actualRhs = extractRhs(request);
    actualRhs.ifPresent(ar -> assertArrayEquals(convert(expectedRhs), ar));
  }
}
