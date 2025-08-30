# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Helper for creating and closing connection to RabbitMQ
"""
import logging

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

from common_logging.setup_logging import setup_logging

# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)


def connect_to_rabbit_mq(rabbit_mq_host: str, task_queue: str, result_queue: str):
    # Connect to RabbitMQ
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_mq_host)
        )
        channel = connection.channel()
        channel.queue_declare(queue=task_queue, durable=True)
        channel.queue_declare(queue=result_queue, durable=True)
        logger.info("Connected to RabbitMQ and declared queues.")
        return connection, channel
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise


def close_rabbit_mq_connection(
    rabbitmq_connection: BlockingConnection, rabbitmq_channel: BlockingChannel
):
    try:
        if rabbitmq_channel:
            rabbitmq_channel.close()
            logger.info("RabbitMQ channel closed.")
    except Exception as e:
        logger.warning(f"Error closing RabbitMQ channel: {e}")
    try:
        if rabbitmq_connection:
            rabbitmq_connection.close()
            logger.info("RabbitMQ connection closed.")
    except Exception as e:
        logger.exception(f"Error closing connection: : {e}")
