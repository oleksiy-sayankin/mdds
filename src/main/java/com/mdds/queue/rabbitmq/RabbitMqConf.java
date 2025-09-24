/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

/**
 * Base container for RabbitMq properties.
 *
 * @param host RabbitMq host.
 * @param port RabbitMq port.
 * @param user RabbitMq user.
 * @param password RabbitMq user password.
 */
public record RabbitMqConf(String host, int port, String user, String password) {}
