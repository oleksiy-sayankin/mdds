/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

/** Gets results from the Result Queue and puts it to storage. */
@Slf4j
public class ResultConsumer {

  public static void main(String[] args) throws LifecycleException {
    var conf = ResultConsumerConfFactory.fromEnvOrDefaultProperties();
    start(conf.host(), conf.port(), conf.webappDirLocation()).getServer().await();
  }

  /**
   * Starts Result Consumer and returns minimal Apache Tomcat starter object.
   *
   * @param hostName hostname to start Result Consumer.
   * @param port port to start Result Consumer.
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
    ctx.addApplicationListener(ResultConsumerAppContextListener.class.getName());
    Tomcat.addServlet(ctx, "resultConsumerHealthServlet", new ResultConsumerHealthServlet());
    ctx.addServletMappingDecoded("/health", "resultConsumerHealthServlet");
    tomcat.start();
    log.info("Result Consumer started at http://{}:{}", host.getName(), port);
    return tomcat;
  }
}
