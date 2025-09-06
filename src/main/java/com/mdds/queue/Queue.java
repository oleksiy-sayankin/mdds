/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

package com.mdds.queue;

/** Common interface for Task Queue and Result Queue. */
public interface Queue {
  void connect();

  void declareQueue(String queueName);

  void deleteQueue(String queueName);

  void publish(Object task, String queueName);

  void registerConsumer(
      String queueName, MddsDeliverCallback deliverCallback, MddsCancelCallback cancelCallback);

  void close();
}
