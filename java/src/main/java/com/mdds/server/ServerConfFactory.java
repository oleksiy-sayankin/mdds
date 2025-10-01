/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

import jakarta.annotation.Nonnull;
import java.io.File;

/** Factory for creating Server configurations. */
public final class ServerConfFactory {
  private ServerConfFactory() {}

  private static final String FILE_NAME = "server.properties";

  public static @Nonnull ServerConf fromEnvOrDefaultProperties() {
    var props = readPropertiesOrEmpty(FILE_NAME);
    var host = resolveString("mdds.server.host", "MDDS_SERVER_HOST", props, "localhost");
    var port = resolveInt("mdds.server.port", "MDDS_SERVER_PORT", props, 8000);
    var webappDir =
        resolveString(
            "mdds.server.webapp.dir.location",
            "MDDS_SERVER_WEBAPP_DIR_LOCATION",
            props,
            System.getProperty("user.dir") + File.separator + "mdds_client");

    return new ServerConf(host, port, webappDir);
  }
}
