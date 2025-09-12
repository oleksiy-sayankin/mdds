/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import org.apache.catalina.Host;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Start point for web server. */
public class Main {
  public static final String MDDS_SERVER_DEFAULT_HOST = "localhost";
  public static final int MDDS_SERVER_DEFAULT_PORT = 8000;
  public static final String MDDS_SERVER_DEFAULT_WEB_APPLICATION_LOCATION = "mdds_client";
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws LifecycleException {
    var hostName = System.getenv().getOrDefault("MDDS_SERVER_HOST", MDDS_SERVER_DEFAULT_HOST);
    var port =
        Integer.parseInt(
            System.getenv()
                .getOrDefault("MDDS_SERVER_PORT", String.valueOf(MDDS_SERVER_DEFAULT_PORT)));
    var webappDirLocation =
        System.getenv()
            .getOrDefault(
                "MDDS_SERVER_WEB_APPLICATION_LOCATION",
                MDDS_SERVER_DEFAULT_WEB_APPLICATION_LOCATION);
    start(hostName, port, webappDirLocation).getServer().await();
  }

  /**
   * Starts web server and returns minimal Apache Tomcat starter object.
   *
   * @param hostName hostname to start web server.
   * @param port port to start web server.
   * @param webappDirLocation folder with static files for web application.
   * @return minimal Apache Tomcat starter object.
   * @throws LifecycleException when we can not start Tomcat.
   */
  public static Tomcat start(String hostName, int port, String webappDirLocation)
      throws LifecycleException {
    Tomcat tomcat = new Tomcat();
    Host host = tomcat.getHost(); // Get the default host
    host.setName(hostName);
    tomcat.setPort(port);
    tomcat.getConnector(); // ensure connector created

    var ctx = tomcat.addWebapp("", new java.io.File(webappDirLocation).getAbsolutePath());
    // Register listener and servlets programmatically
    ctx.addApplicationListener(AppContextListener.class.getName());
    Tomcat.addServlet(ctx, "rootServlet", new RootServlet());
    ctx.addServletMappingDecoded("/", "rootServlet");
    Tomcat.addServlet(ctx, "healthServlet", new HealthServlet());
    ctx.addServletMappingDecoded("/health", "healthServlet");

    tomcat.start();
    LOGGER.info("Server started at http://{}:{}", host, port);
    return tomcat;
  }
}
