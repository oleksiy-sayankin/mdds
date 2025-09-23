/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.google.common.annotations.VisibleForTesting;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

import java.io.File;

/**
 * Base class for Server configuration.
 *
 * @param host host where Result Consumer runs.
 * @param port port where Result Consumer runs.
 * @param webappDirLocation web application directory location.
 */
public record ServerConf(String host, int port, String webappDirLocation) {
  public static final String MDDS_SERVER_DEFAULT_HOST = "localhost";
  public static final int MDDS_SERVER_DEFAULT_PORT = 8000;
  public static final String MDDS_SERVER_DEFAULT_WEB_APPLICATION_LOCATION =
      System.getProperty("user.dir") + File.separator + "mdds_client";
  private static final String MDDS_SERVER_DEFAULT_CONF_FILE_NAME = "server.properties";

  @VisibleForTesting
  public static ServerConf fromEnvOrProperties(String fileName) {
    var hostName =
        resolveString(
            "mdds.server.host",
            "MDDS_SERVER_HOST",
            readPropertiesOrEmpty(fileName),
            MDDS_SERVER_DEFAULT_HOST);

    var port =
        resolveInt(
            "mdds.server.port",
            "MDDS_SERVER_PORT",
            readPropertiesOrEmpty(fileName),
            MDDS_SERVER_DEFAULT_PORT);
    var webappDirLocation =
        resolveString(
            "mdds.server.webapp.dir.location",
            "MDDS_SERVER_WEBAPP_DIR_LOCATION",
            readPropertiesOrEmpty(fileName),
            MDDS_SERVER_DEFAULT_WEB_APPLICATION_LOCATION);
    return new ServerConf(hostName, port, webappDirLocation);
  }

  public static ServerConf fromEnvOrDefaultProperties() {
    return fromEnvOrProperties(MDDS_SERVER_DEFAULT_CONF_FILE_NAME);
  }
}
