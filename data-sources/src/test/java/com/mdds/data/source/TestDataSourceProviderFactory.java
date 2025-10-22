/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.Part;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TestDataSourceProviderFactory {

  @Test
  void testHttpRequestFromDescriptor() throws IOException, ServletException {
    var request = mock(HttpServletRequest.class);
    var expectedMatrix = new double[][] {{1.3, 2.2, 3.7}, {7.7, 2.1, 9.3}, {1.1, 4.8, 2.3}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(Arrays.stream(row).mapToObj(String::valueOf).collect(Collectors.joining(",")));
      sb.append(System.lineSeparator());
    }
    var matrixIs = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var matrixPart = mock(Part.class);
    when(matrixPart.getInputStream()).thenReturn(matrixIs);
    when(request.getPart("matrix")).thenReturn(matrixPart);

    var expectedRhs = new double[] {1.3, 2.2, 3.7};
    var rhsIs =
        new ByteArrayInputStream(
            Arrays.stream(expectedRhs)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(System.lineSeparator()))
                .getBytes(UTF_8));
    var rhsPart = mock(Part.class);
    when(rhsPart.getInputStream()).thenReturn(rhsIs);
    when(request.getPart("rhs")).thenReturn(rhsPart);

    Map<String, Object> params = new HashMap<>();

    params.put("request", request);
    var dsd = DataSourceDescriptor.of(DataSourceDescriptor.Type.HTTP_REQUEST, params);
    var dataSourceProvider = DataSourceProviderFactory.fromDescriptor(dsd);
    dataSourceProvider.ifPresent(
        dsp -> {
          dsp.loadMatrix()
              .ifPresent(actualMatrix -> assertArrayEquals(expectedMatrix, actualMatrix));
          dsp.loadRhs().ifPresent(actualRhs -> assertArrayEquals(expectedRhs, actualRhs));
        });
  }
}
