/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.storage.DataStorage;
import com.mdds.storage.DataStorageFactory;
import com.mdds.storage.redis.RedisHelper;
import jakarta.servlet.ServletContext;
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

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    ServletContext ctx = sce.getServletContext();
    String redisProperties = ctx.getInitParameter("redis.properties"); // optional
    if (redisProperties == null) {
      redisProperties = "redis.properties";
    }
    DataStorage dataStorage =
        DataStorageFactory.createRedis(RedisHelper.readFromResources(redisProperties));
    ctx.setAttribute(ATTR_DATA_STORAGE, dataStorage);
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    ServletContext ctx = sce.getServletContext();
    var dataStorage = (DataStorage) ctx.getAttribute(ATTR_DATA_STORAGE);
    if (dataStorage != null) {
      dataStorage.close();
    }
  }
}
