/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import com.mdds.storage.redis.RedisStorageProvider;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

/**
 * Tomcat context listener. Here we initialize connections to Queue and DataStorage and close them
 * when context is destroyed.
 */
public class ServerAppContextListener implements ServletContextListener {
  public static final String ATTR_SERVER_SERVICE = "SERVER_SERVICE";

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    sce.getServletContext()
        .setAttribute(
            ATTR_SERVER_SERVICE,
            new ServerService(new RedisStorageProvider().get(), new RabbitMqQueueProvider().get()));
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    var ctx = sce.getServletContext();
    var service = (ServerService) ctx.getAttribute(ATTR_SERVER_SERVICE);
    if (service != null) {
      service.close();
    }
  }
}
