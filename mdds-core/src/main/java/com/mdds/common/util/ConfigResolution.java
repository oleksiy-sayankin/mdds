/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Here we read string and integer configuration properties. Here is the read order: 1. Read from
 * Java system properties. If there is no value then 2. Read from environment variable. If there is
 * no value then 3. Read from Properties instance. If there is no value then 4. Use default value.
 */
@Slf4j
public class ConfigResolution {
  private ConfigResolution() {}

  /**
   * Resolves string type property.
   *
   * @param sysPropKey key from system Java properties. Format is some.key.name.
   * @param envKey environmental key. Format is SOME_KEY_NAME.
   * @param props properties instance from where to read value.
   * @param defaultValue default value.
   * @return resolved property value.
   */
  public static String resolveString(
      String sysPropKey, String envKey, Properties props, String defaultValue) {
    var v = System.getProperty(sysPropKey);
    if (v != null && !v.isBlank()) {
      return v;
    }
    v = System.getenv(envKey);
    if (v != null && !v.isBlank()) {
      return v;
    }
    return props.getProperty(sysPropKey, defaultValue);
  }

  /**
   * Resolves integer type property.
   *
   * @param sysPropKey key from system Java properties. Format is some.key.name.
   * @param envKey environmental key. Format is SOME_KEY_NAME.
   * @param props properties instance from where to read value.
   * @param defaultValue default value.
   * @return resolved property value.
   */
  public static int resolveInt(
      String sysPropKey, String envKey, Properties props, int defaultValue) {
    var v = resolveString(sysPropKey, envKey, props, null);
    if (v == null) return defaultValue;
    try {
      return Integer.parseInt(v);
    } catch (NumberFormatException e) {
      log.warn("Error parsing {}", v, e);
      return defaultValue;
    }
  }
}
