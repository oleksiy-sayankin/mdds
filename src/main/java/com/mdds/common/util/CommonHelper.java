/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class with common utilities. */
public final class CommonHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommonHelper.class);

  private CommonHelper() {}

  /**
   * Reads properties file from resources or returns empty properties instance if there is no such
   * file.
   *
   * @param fileName file to read.
   * @return instance of Properties class
   */
  public static Properties readPropertiesOrEmpty(String fileName) {
    Properties properties;
    if (fileName == null || fileName.isBlank()) {
      LOGGER.debug("File name is null or blank");
      return new Properties();
    }
    try (var input = CommonHelper.class.getClassLoader().getResourceAsStream(fileName)) {
      if (input == null) {
        LOGGER.debug("{} not found in resources", fileName);
        return new Properties();
      }
      properties = new Properties();
      properties.load(input);
    } catch (IOException e) {
      LOGGER.error("Could not load file {}", fileName, e);
      return new Properties();
    }
    return properties;
  }
}
