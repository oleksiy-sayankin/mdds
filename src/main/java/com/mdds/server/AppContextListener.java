/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.queue.Queue;
import com.mdds.queue.QueueFactory;
import com.mdds.queue.rabbitmq.RabbitMqConf;
import com.mdds.storage.DataStorage;
import com.mdds.storage.DataStorageFactory;
import com.mdds.storage.redis.RedisConf;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

/**
 * Tomcat context listener. Here we initialize connections to Queue and DataStorage and close them
 * when context is destroyed.
 */
@WebListener
public class AppContextListener implements ServletContextListener {
  public static final String ATTR_DATA_STORAGE = "DATA_STORAGE";
  public static final String ATTR_TASK_QUEUE = "TASK_QUEUE";

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    var ctx = sce.getServletContext();
    var redisProperties = ctx.getInitParameter("redis.properties");
    var redisConf = RedisConf.fromEnvOrProperties(redisProperties);
    var dataStorage = DataStorageFactory.createRedis(redisConf);
    ctx.setAttribute(ATTR_DATA_STORAGE, dataStorage);

    var rabbitMqProperties = ctx.getInitParameter("rabbitmq.properties");
    var rabbitMqConf = RabbitMqConf.fromEnvOrProperties(rabbitMqProperties);
    var queue = QueueFactory.createRabbitMq(rabbitMqConf);
    ctx.setAttribute(ATTR_TASK_QUEUE, queue);
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    var ctx = sce.getServletContext();
    var dataStorage = (DataStorage) ctx.getAttribute(ATTR_DATA_STORAGE);
    if (dataStorage != null) {
      dataStorage.close();
    }
    var queue = (Queue) ctx.getAttribute(ATTR_TASK_QUEUE);
    if (queue != null) {
      queue.close();
    }
  }
}
