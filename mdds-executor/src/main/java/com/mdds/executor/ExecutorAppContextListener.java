/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

/**
 * Tomcat context listener. Here we initialize Executor Service and close it when context is
 * destroyed.
 */
public class ExecutorAppContextListener implements ServletContextListener {
  public static final String ATTR_EXECUTOR_SERVICE = "EXECUTOR_SERVICE";
  public static final String ATTR_MESSAGE_HANDLER = "MESSAGE_HANDLER";

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    var queue = new RabbitMqQueueProvider().get();
    var messageHandler = new ExecutorMessageHandler(queue);
    var ctx = sce.getServletContext();
    ctx.setAttribute(ATTR_EXECUTOR_SERVICE, new ExecutorService(queue, queue, messageHandler));
    ctx.setAttribute(ATTR_MESSAGE_HANDLER, messageHandler);
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    var ctx = sce.getServletContext();
    var handler = (ExecutorMessageHandler) ctx.getAttribute(ATTR_MESSAGE_HANDLER);
    if (handler != null) {
      handler.close();
    }
    var service = (ExecutorService) ctx.getAttribute(ATTR_EXECUTOR_SERVICE);
    if (service != null) {
      service.close();
    }
  }
}
