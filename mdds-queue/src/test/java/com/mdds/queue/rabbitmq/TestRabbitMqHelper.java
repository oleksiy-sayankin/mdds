/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.queue.rabbitmq.RabbitMqHelper.readFromResources;
import static com.mdds.queue.rabbitmq.RabbitMqQueue.convertFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.AMQP;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class TestRabbitMqHelper {

  @Test
  void testNoConfFileExists() {
    assertThatThrownBy(() -> readFromResources("wrong.file.name"))
        .isInstanceOf(RabbitMqConnectionException.class)
        .hasMessageContaining("File not found in resources:");
  }

  @Test
  void testConvertFrom() {
    var headers = new HashMap<String, Object>();
    var key = "test_key";
    var value = new Object();
    headers.put(key, value);
    var expectedBasicProperties = new AMQP.BasicProperties.Builder().headers(headers).build();
    var actualBasicProperties = convertFrom(headers);
    assertThat(actualBasicProperties.getHeaders()).isEqualTo(expectedBasicProperties.getHeaders());
  }
}
