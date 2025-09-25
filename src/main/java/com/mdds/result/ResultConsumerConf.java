/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

/**
 * Base class for Result Consumer configuration.
 *
 * @param host host where Result Consumer runs.
 * @param port port where Result Consumer runs.
 * @param webappDirLocation web application directory location.
 */
public record ResultConsumerConf(String host, int port, String webappDirLocation) {}
