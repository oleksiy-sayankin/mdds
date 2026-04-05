/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.Map;

/** This class represents key value list of job parameters. */
public class JobSetParamsRequestDTO {
  private final Map<String, JsonNode> params = new LinkedHashMap<>();

  /**
   * Puts key-value pair to Map.
   *
   * @param key key to store.
   * @param value value to store. Here we use {@code JsonNode} to store both value and value type,
   *     since {@code JsonNode} type allows that.
   */
  @JsonAnySetter
  public void put(String key, JsonNode value) {
    params.put(key, value);
  }

  public Map<String, JsonNode> params() {
    return Map.copyOf(params);
  }
}
