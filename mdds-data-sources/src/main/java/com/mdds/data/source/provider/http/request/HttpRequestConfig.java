/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

/** Configuration class for http request data source provider. */
@Getter
public class HttpRequestConfig {
  private final List<? extends List<? extends Number>> rawMatrix;
  private final List<? extends Number> rawRhs;
  @Getter private static final Set<String> params = Set.of("matrix", "rhs");

  private HttpRequestConfig(@Nonnull Map<String, Object> params) {
    rawMatrix = (List<? extends List<? extends Number>>) params.get("matrix");
    rawRhs = (List<? extends Number>) params.get("rhs");
  }

  public static HttpRequestConfig of(@Nonnull Map<String, Object> params) {
    return new HttpRequestConfig(params);
  }
}
