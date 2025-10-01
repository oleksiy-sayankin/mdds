/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.Nonnull;

/** Helper class to convert Data Transfer Object to JSON and vise versa. */
public final class JsonHelper {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.registerModule(new JavaTimeModule());
  }

  private JsonHelper() {}

  public static <T> String toJson(T object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new JsonException("Could not convert object to JSON", e);
    }
  }

  public static <T> T fromJson(String json, @Nonnull Class<T> clazz) {
    if (json == null) {
      return null;
    }
    try {
      return objectMapper.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new JsonException("Could not convert JSON to object", e);
    }
  }
}
