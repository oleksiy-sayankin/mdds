/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static com.mdds.common.AppConstants.RESULT_QUEUE_NAME;

import com.mdds.common.AppConstantsFactory;
import com.mdds.queue.*;
import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import com.mdds.storage.DataStorage;
import com.mdds.storage.redis.RedisStorageProvider;
import dto.ResultDTO;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

/**
 * Tomcat context listener. Here we initialize connections to Queue and DataStorage and close them
 * when context is destroyed.
 */
public class ResultConsumerAppContextListener implements ServletContextListener {
  public static final String ATTR_DATA_STORAGE = "DATA_STORAGE";
  public static final String ATTR_RESULT_QUEUE = "RESULT_QUEUE";
  public static final String ATTR_RESULT_QUEUE_SUBSCRIPTION = "RESULT_QUEUE_SUBSCRIPTION";

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    var ctx = sce.getServletContext();

    var storageProvider = new RedisStorageProvider();
    var dataStorage = storageProvider.get();
    ctx.setAttribute(ATTR_DATA_STORAGE, dataStorage);

    var queueProvider = new RabbitMqQueueProvider();
    var queue = queueProvider.get();
    var subscription =
        queue.subscribe(
            AppConstantsFactory.getString(RESULT_QUEUE_NAME),
            ResultDTO.class,
            (message, ack) -> {
              var payload = message.payload();
              dataStorage.put(payload.getTaskId(), payload);
              ack.ack();
            });
    ctx.setAttribute(ATTR_RESULT_QUEUE, queue);
    ctx.setAttribute(ATTR_RESULT_QUEUE_SUBSCRIPTION, subscription);
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    var ctx = sce.getServletContext();
    var dataStorage = (DataStorage) ctx.getAttribute(ATTR_DATA_STORAGE);
    if (dataStorage != null) {
      dataStorage.close();
    }
    var subscription = (Subscription) ctx.getAttribute(ATTR_RESULT_QUEUE_SUBSCRIPTION);
    if (subscription != null) {
      subscription.close();
    }
    var queue = (Queue) ctx.getAttribute(ATTR_RESULT_QUEUE);
    if (queue != null) {
      queue.close();
    }
  }
}
