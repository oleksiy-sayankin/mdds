/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

import java.util.Arrays;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Supported file formats for input/output artifacts. */
@RequiredArgsConstructor
public enum ArtifactFormat {
  CSV("csv"),
  JSON("json");

  @Getter private final String value;

  public static ArtifactFormat from(String raw) {
    if (raw == null || raw.isBlank()) {
      throw new UnknownArtifactFormatException("Artifact format must not be null or blank.");
    }
    return Arrays.stream(values())
        .filter(t -> t.value.equalsIgnoreCase(raw.trim()))
        .findFirst()
        .orElseThrow(
            () ->
                new UnknownArtifactFormatException(
                    "Unknown or unsupported artifact format: '" + raw + "'."));
  }
}
