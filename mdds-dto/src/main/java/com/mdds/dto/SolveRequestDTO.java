/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Request body for /solve endpoint. */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SolveRequestDTO {
  private String dataSourceType;
  private String slaeSolvingMethod;
  private Map<String, Object> params;
}
