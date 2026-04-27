/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

import java.util.Arrays;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Possible types of job parameters. */
@RequiredArgsConstructor
public enum ParamType {
  STRING("string"),
  NUMBER("number"),
  BOOLEAN("boolean"),
  MAP("map"),
  ENUM("enum");

  @Getter private final String value;

  public static ParamType from(String raw) {
    if (raw == null || raw.isBlank()) {
      throw new UnknownParamTypeException("Parameter type must not be null or blank.");
    }
    return Arrays.stream(values())
        .filter(t -> t.value.equalsIgnoreCase(raw.trim()))
        .findFirst()
        .orElseThrow(
            () ->
                new UnknownParamTypeException(
                    "Unknown or unsupported parameter type: '" + raw + "'."));
  }
}
