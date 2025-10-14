/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

import jakarta.annotation.Nonnull;
import java.io.File;

/** Factory for creating Result Consumer configurations. */
public final class ResultConsumerConfFactory {
  private ResultConsumerConfFactory() {}

  private static final String FILE_NAME = "result.consumer.properties";

  public static @Nonnull ResultConsumerConf fromEnvOrDefaultProperties() {
    var props = readPropertiesOrEmpty(FILE_NAME);
    var hostName =
        resolveString("mdds.result.consumer.host", "MDDS_RESULT_CONSUMER_HOST", props, "localhost");
    var port = resolveInt("mdds.result.consumer.port", "MDDS_RESULT_CONSUMER_PORT", props, 8863);
    var webappDirLocation =
        resolveString(
            "mdds.result.consumer.webapp.dir.location",
            "MDDS_RESULT_CONSUMER_WEBAPP_DIR_LOCATION",
            props,
            System.getProperty("user.dir") + File.separator + "web-app");
    return new ResultConsumerConf(hostName, port, webappDirLocation);
  }
}
