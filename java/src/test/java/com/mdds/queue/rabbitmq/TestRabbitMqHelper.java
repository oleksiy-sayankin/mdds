/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.queue.rabbitmq.RabbitMqHelper.readFromResources;
import static com.mdds.queue.rabbitmq.RabbitMqQueue.convertFrom;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.rabbitmq.client.AMQP;
import java.util.HashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRabbitMqHelper {

  @Test
  void testNoConfFileExists() {
    assertThrows(RabbitMqConnectionException.class, () -> readFromResources("wrong.file.name"));
  }

  @Test
  void testConvertFrom() {
    var headers = new HashMap<String, Object>();
    var key = "test_key";
    var value = new Object();
    headers.put(key, value);
    var expectedBasicProperties = new AMQP.BasicProperties.Builder().headers(headers).build();
    var actualBasicProperties = convertFrom(headers);
    Assertions.assertEquals(
        expectedBasicProperties.getHeaders(), actualBasicProperties.getHeaders());
  }
}
