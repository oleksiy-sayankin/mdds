/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

/**
 * Gets a task from the Task Queue, solves a task (solves system of a liner equations) and puts
 * result to the Result Queue.
 */
@Slf4j
public class Executor {
  public static void main(String[] args) throws LifecycleException {
    var conf = ExecutorConfFactory.fromEnvOrDefaultProperties();
    start(conf.executorHost(), conf.executorPort(), conf.webappDirLocation()).getServer().await();
  }

  /**
   * Starts Executor and returns minimal Apache Tomcat starter object.
   *
   * @param hostName hostname to start Executor.
   * @param port port to start Executor.
   * @param docBase folder with static files for web application.
   * @return minimal Apache Tomcat starter object.
   * @throws LifecycleException when we can not start Tomcat.
   */
  public static @Nonnull Tomcat start(@Nonnull String hostName, int port, @Nonnull String docBase)
      throws LifecycleException {
    var tomcat = new Tomcat();
    var host = tomcat.getHost(); // Get the default host
    host.setName(hostName);
    tomcat.setPort(port);
    tomcat.getConnector(); // ensure connector created
    tomcat.getEngine().setDefaultHost(hostName);

    var ctx = tomcat.addWebapp(host, "", docBase);
    // Register listener and servlets programmatically
    ctx.addApplicationListener(ExecutorAppContextListener.class.getName());
    Tomcat.addServlet(ctx, "executorHealthServlet", new ExecutorHealthServlet());
    ctx.addServletMappingDecoded("/health", "executorHealthServlet");
    tomcat.start();
    log.info("Executor started at http://{}:{}", host.getName(), port);
    return tomcat;
  }
}
