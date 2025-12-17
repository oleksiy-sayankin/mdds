/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestResultDTO {

  @Test
  void testSetPercentDone() {
    var result = new ResultDTO();
    assertThatCode(() -> result.setProgress(10)).doesNotThrowAnyException();
    assertThatCode(() -> result.setProgress(0)).doesNotThrowAnyException();
    assertThatCode(() -> result.setProgress(100)).doesNotThrowAnyException();
    assertThatThrownBy(() -> result.setProgress(-1))
        .isInstanceOf(IllegalPercentValue.class)
        .hasMessageContaining("Progress must be between 0 and 100, but was");
    assertThatThrownBy(() -> result.setProgress(101))
        .isInstanceOf(IllegalPercentValue.class)
        .hasMessageContaining("Progress must be between 0 and 100, but was");
  }
}
