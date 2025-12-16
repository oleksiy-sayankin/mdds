/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

@WebMvcTest(ResultConsumerHealthController.class)
class TestResultConsumerApplicationHealthController {
  @Autowired private ResultConsumerHealthController healthController;

  @Test
  void testHealth() {
    assertThat(healthController.health()).isEqualTo("OK");
  }
}
