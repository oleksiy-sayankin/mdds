/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import static com.github.valfirst.slf4jtest.TestLoggerFactory.getTestLogger;
import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class TestConfigResolution {
  @Test
  void testResolveStringGetSystemProperty() {
    var expected = "test.value.from.system.properties";
    System.setProperty("test.property", expected);
    var actual = resolveString("test.property", "TEST_PROPERTY", null, "default.value");
    assertEquals(expected, actual);
  }

  @Test
  @SetEnvironmentVariable(key = "TEST_PROPERTY", value = "TEST_VALUE_FROM_ENV_VAR")
  void testResolveStringGetEnvVariable() {
    var expected = "TEST_VALUE_FROM_ENV_VAR";
    var actual = resolveString("test.property", "TEST_PROPERTY", null, "default.value");
    assertEquals(expected, actual);
  }

  @Test
  void testResolveStringGetFromPropertiesFile() {
    var expected = "value";
    var actual =
        resolveString(
            "test.key", "TEST_KEY", readPropertiesOrEmpty("test.properties"), "default.value");
    assertEquals(expected, actual);
  }

  @Test
  void testResolveStringDefaultValue() {
    var expected = "default.value";
    var actual =
        resolveString(
            "test.key.not.in.properties",
            "TEST_KEY",
            readPropertiesOrEmpty("test.properties"),
            "default.value");
    assertEquals(expected, actual);
  }

  @Test
  void testResolveIntDefaultValue() {
    var expected = 123;
    var actual =
        resolveInt(
            "test.key.not.in.properties",
            "TEST_KEY",
            readPropertiesOrEmpty("test.properties"),
            123);
    assertEquals(expected, actual);
  }

  @Test
  void testResolveIntErrorInParsing() {
    var logger = getTestLogger(ConfigResolution.class);
    System.setProperty("test.property", "123-wrong-number");
    var expected = 123;
    var actual = resolveInt("test.property", "TEST_KEY", null, 123);
    assertEquals(expected, actual);
    assertTrue(
        logger.getLoggingEvents().stream().anyMatch(e -> e.getMessage().contains("Error parsing")));
  }
}
