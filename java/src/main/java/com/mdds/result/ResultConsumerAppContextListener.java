/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import com.mdds.storage.redis.RedisStorageProvider;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

/**
 * Tomcat context listener. Here we initialize Result Consumer Service and close it when context is
 * destroyed.
 */
public class ResultConsumerAppContextListener implements ServletContextListener {
  public static final String ATTR_RESULT_CONSUMER_SERVICE = "RESULT_CONSUMER_SERVICE";

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    sce.getServletContext()
        .setAttribute(
            ATTR_RESULT_CONSUMER_SERVICE,
            new ResultConsumerService(
                new RedisStorageProvider().get(), new RabbitMqQueueProvider().get()));
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    var ctx = sce.getServletContext();
    var service = (ResultConsumerService) ctx.getAttribute(ATTR_RESULT_CONSUMER_SERVICE);
    if (service != null) {
      service.close();
    }
  }
}
