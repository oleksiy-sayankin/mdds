/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestResultDTO {

  @Test
  void testSetPercentDone() {
    var result = new ResultDTO();
    Assertions.assertDoesNotThrow(() -> result.setProgress(10));
    Assertions.assertDoesNotThrow(() -> result.setProgress(0));
    Assertions.assertDoesNotThrow(() -> result.setProgress(100));
    Assertions.assertThrows(IllegalPercentValue.class, () -> result.setProgress(-1));
    Assertions.assertThrows(IllegalPercentValue.class, () -> result.setProgress(101));
  }
}
