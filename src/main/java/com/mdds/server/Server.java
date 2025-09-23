/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.ServerConf.fromEnvOrDefaultProperties;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Start point for web server. */
public class Server {
  private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

  public static void main(String[] args) throws LifecycleException {
    var conf = fromEnvOrDefaultProperties();
    start(conf.host(), conf.port(), conf.webappDirLocation()).getServer().await();
  }

  /**
   * Starts web server and returns minimal Apache Tomcat starter object.
   *
   * @param hostName hostname to start web server.
   * @param port port to start web server.
   * @param docBase folder with static files for web application.
   * @return minimal Apache Tomcat starter object.
   * @throws LifecycleException when we can not start Tomcat.
   */
  public static Tomcat start(String hostName, int port, String docBase) throws LifecycleException {
    var tomcat = new Tomcat();
    var host = tomcat.getHost(); // Get the default host
    host.setName(hostName);
    tomcat.setPort(port);
    tomcat.getConnector(); // ensure connector created

    var ctx = tomcat.addWebapp(host, "", docBase);
    // Register listener and servlets programmatically
    ctx.addApplicationListener(ServerAppContextListener.class.getName());
    Tomcat.addServlet(ctx, "serverRootServlet", new ServerRootServlet());
    ctx.addServletMappingDecoded("/", "serverRootServlet");
    Tomcat.addServlet(ctx, "serverHealthServlet", new ServerHealthServlet());
    ctx.addServletMappingDecoded("/health", "serverHealthServlet");
    Tomcat.addServlet(ctx, "serverResultServlet", new ServerResultServlet());
    ctx.addServletMappingDecoded("/result/*", "serverResultServlet");
    Tomcat.addServlet(ctx, "serverSolveServlet", new ServerSolveServlet());
    ctx.addServletMappingDecoded("/solve", "serverSolveServlet");
    ctx.setAllowCasualMultipartParsing(true);

    tomcat.start();
    LOGGER.info("Server started at http://{}:{}", host.getName(), port);
    return tomcat;
  }
}
