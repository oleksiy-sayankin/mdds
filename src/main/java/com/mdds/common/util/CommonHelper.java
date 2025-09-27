/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import java.io.IOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/** Helper class with common utilities. */
@Slf4j
public final class CommonHelper {

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
      log.debug("File name is null or blank");
      return new Properties();
    }
    try (var input = CommonHelper.class.getClassLoader().getResourceAsStream(fileName)) {
      if (input == null) {
        log.debug("{} not found in resources", fileName);
        return new Properties();
      }
      properties = new Properties();
      properties.load(input);
    } catch (IOException e) {
      log.error("Could not load file {}", fileName, e);
      return new Properties();
    }
    return properties;
  }
}
