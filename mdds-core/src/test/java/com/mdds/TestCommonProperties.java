/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds;

import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.CommonConfig;
import com.mdds.common.CommonProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = CommonConfig.class)
class TestCommonProperties {
  @Autowired private CommonProperties commonProperties;

  @Test
  void testSimpleRead() {
    assertThat(commonProperties.getJobQueueName()).isEqualTo("mdds_job_queue");
    assertThat(commonProperties.getResultQueueName()).isEqualTo("mdds_result_queue");
  }
}
