/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.fasterxml.jackson.databind.node.JsonNodeType;

/** Formats Json types for messages. */
public final class JsonTypeFormatter {
  private JsonTypeFormatter() {}

  /**
   * Formats Json type.
   *
   * @param type Json type
   * @return formatted string.
   */
  public static String describeJsonType(JsonNodeType type) {
    if (type == null) {
      return "null";
    }

    return switch (type) {
      case STRING -> "string";
      case NUMBER -> "number";
      case BOOLEAN -> "boolean";
      case NULL -> "null";
      case OBJECT, POJO -> "object";
      case ARRAY -> "array";
      case BINARY -> "binary";
      case MISSING -> "missing";
    };
  }
}
