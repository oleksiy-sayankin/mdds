# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Helper for creating and closing connection to RabbitMQ
"""
import logging

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import PERSISTENT_DELIVERY_MODE

from common_logging.setup_logging import setup_logging
from mdds_dto.task_dto import TaskDTO

# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)


def connect_to_rabbit_mq(rabbit_mq_host: str):
    # TODO:
    #  1. Think about reading heartbeat and blocked_connection_timeout from configuration file;
    #  2. What about re-connect strategies? Say, if there is no connection, wait for 1s, if there is no again, then
    #     5s and then 10s and so on. Think in future.
    #  3. Think about secure connection to RabbitMQ host (ssl or whatever).

    # Connect to RabbitMQ
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_mq_host)
        )
        channel = connection.channel()
        logger.info("Connected to RabbitMQ.")
        return connection, channel
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise


def declare_queues(
    rabbitmq_channel: BlockingChannel, task_queue: str, result_queue: str
):
    rabbitmq_channel.queue_declare(queue=task_queue, durable=True)
    rabbitmq_channel.queue_declare(queue=result_queue, durable=True)
    logger.info(f"Declared queues {task_queue} and {result_queue}.")


def declare_queue(rabbitmq_channel: BlockingChannel, result_queue: str):
    rabbitmq_channel.queue_declare(queue=result_queue, durable=True)
    logger.info(f"Declared queue {result_queue}.")


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


def publish_to_queue(
    rabbitmq_channel: BlockingChannel, task_queue_name: str, task: TaskDTO
):
    rabbitmq_channel.basic_publish(
        exchange="",
        routing_key=task_queue_name,
        body=task.model_dump_json(),
        properties=pika.BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE
        ),  # make message persistent
    )
    logger.info(f"Task {task.task_id} published to RabbitMQ.")
