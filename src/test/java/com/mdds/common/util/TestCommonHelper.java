/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import static com.github.valfirst.slf4jtest.TestLoggerFactory.getTestLogger;
import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static org.junit.jupiter.api.Assertions.*;

import com.github.valfirst.slf4jtest.TestLoggerFactoryExtension;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestLoggerFactoryExtension.class)
class TestCommonHelper {
  @Test
  void testReadPropertiesOrEmptyNullInFileName() {
    var logger = getTestLogger(CommonHelper.class);
    var actual = readPropertiesOrEmpty(null);
    assertNotNull(actual);
    assertTrue(
        logger.getLoggingEvents().stream()
            .anyMatch(e -> e.getMessage().contains("File name is null or blank")));
  }

  @Test
  void testReadPropertiesOrEmptyNotFoundInResources() {
    var logger = getTestLogger(CommonHelper.class);
    var actual = readPropertiesOrEmpty("not.in.resources.properties");
    assertNotNull(actual);
    assertTrue(
        logger.getLoggingEvents().stream()
            .anyMatch(e -> e.getMessage().contains("not found in resources")));
  }

  @Test
  void testReadPropertiesOrEmpty() {
    var actual = readPropertiesOrEmpty("test.properties");
    assertNotNull(actual);
    var expected = new Properties();
    expected.put("test.key", "value");
    assertEquals(expected, actual);
  }
}
