/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractMatrix;
import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractRhs;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TestHttpRequestHelper {

  @Test
  void testExtractMatrix() {
    var data = new double[][] {{1.3, 2.2, 3.7}, {7.7, 2.1, 9.3}, {1.1, 4.8, 2.3}};
    var rawData =
        List.of(
            List.of(data[0][0], data[0][1], data[0][2]),
            List.of(data[1][0], data[1][1], data[1][2]),
            List.of(data[2][0], data[2][1], data[2][2]));
    var actual = extractMatrix(rawData);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get()).isEqualTo(data);
  }

  @Test
  void testExtractRhs() {
    var data = new double[] {1.3, 2.2, 3.7};
    List<? extends Number> rawData = List.of(data[0], data[1], data[2]);
    var actual = extractRhs(rawData);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get()).isEqualTo(data);
  }
}
