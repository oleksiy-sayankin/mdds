/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

import java.util.Set;

/**
 * Specifies additional job parameters.
 *
 * @param type type of the parameter.
 * @param required true if required.
 * @param allowedValues set of possible values (optional)
 */
public record JobParamSpec(ParamType type, boolean required, Set<String> allowedValues) {
  public JobParamSpec {
    allowedValues = allowedValues == null ? Set.of() : Set.copyOf(allowedValues);

    if (type == ParamType.ENUM && allowedValues.isEmpty()) {
      throw new IllegalArgumentException("ENUM parameter must define allowedValues.");
    }
    if (type != ParamType.ENUM && !allowedValues.isEmpty()) {
      throw new IllegalArgumentException("Only ENUM parameter may define allowedValues.");
    }
  }

  public JobParamSpec(ParamType type, boolean required) {
    this(type, required, Set.of());
  }
}
