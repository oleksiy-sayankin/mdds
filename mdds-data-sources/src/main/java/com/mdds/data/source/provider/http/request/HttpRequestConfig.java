/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import lombok.Getter;

/** Configuration class for http request data source provider. */
@Getter
public class HttpRequestConfig {
  private final HttpServletRequest request;

  private HttpRequestConfig(@Nonnull Map<String, Object> params) {
    request = (HttpServletRequest) params.get("request");
  }

  public static HttpRequestConfig of(@Nonnull Map<String, Object> params) {
    return new HttpRequestConfig(params);
  }
}
