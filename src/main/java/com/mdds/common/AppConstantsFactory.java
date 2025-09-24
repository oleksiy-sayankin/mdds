/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;

import com.mdds.common.util.ConfigResolution;

/** Factory for creating Application configurations. */
public final class AppConstantsFactory {
  private AppConstantsFactory() {}

  private static final String DEFAULT_CONF_FILE_NAME = "mdds.properties";

  /**
   * Gets application property value.
   *
   * @param constant key to get property value
   * @return property value as string.
   */
  public static String getString(AppConstants constant) {
    return ConfigResolution.resolveString(
        constant.getKey(),
        convertPropertyToEnvName(constant.getKey()),
        readPropertiesOrEmpty(DEFAULT_CONF_FILE_NAME),
        constant.getDefaultValue());
  }

  /**
   * Gets application property value.
   *
   * @param constant key to get property value
   * @return property value as integer.
   */
  public static int getInt(AppConstants constant) {
    return resolveInt(
        constant.getKey(),
        convertPropertyToEnvName(constant.getKey()),
        readPropertiesOrEmpty(DEFAULT_CONF_FILE_NAME),
        Integer.parseInt(constant.getDefaultValue()));
  }

  /**
   * Converts property name to environment variable name.
   *
   * @param propertyName property name.
   * @return environment variable name.
   */
  public static String convertPropertyToEnvName(String propertyName) {
    if (propertyName == null) {
      return null;
    }
    return propertyName.replace('.', '_').toUpperCase();
  }
}
