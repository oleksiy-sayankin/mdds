# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Result Consumer Service:
Consumes results from RabbitMQ (result_queue) and stores them in Redis.
Key = task_id, Value = JSON representation of ResultDTO.
"""

import logging
import os
import redis

from mdds_dto.result_dto import ResultDTO
from mdds_utility.redis_helper_sync import get_redis_client, close_redis_client
from mdds_utility.rabbitmq_helper import (
    connect_to_rabbit_mq,
    close_rabbit_mq_connection,
    declare_queue,
)

from common_logging.setup_logging import setup_logging

# Apply logging config
setup_logging()
logger = logging.getLogger(__name__)

# Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESULT_QUEUE_NAME = "result_queue"


def callback(channel, method, properties, body, redis_client: redis.Redis):
    try:
        # Deserialize result
        result = ResultDTO.model_validate_json(body)
        logger.info(
            f"Received result for task {result.task_id}, status={result.status}"
        )

        # Store in Redis
        redis_client.set(
            result.task_id,
            result.model_dump_json(),
        )
        logger.debug(f"Stored result in Redis under key={result.task_id}")

        # Acknowledge message
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception:
        logger.exception("Failed to process result, rejecting message")
        try:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            logger.exception("Failed to nack message")


def main():
    rabbitmq_connection = None
    rabbitmq_channel = None
    redis_client = None

    try:
        # Connect to RabbitMQ
        rabbitmq_connection, rabbitmq_channel = connect_to_rabbit_mq(RABBITMQ_HOST)
        declare_queue(rabbitmq_channel, RESULT_QUEUE_NAME)

        # Connect to Redis
        redis_client = get_redis_client(REDIS_HOST, REDIS_PORT)

        # Subscribe to result_queue
        rabbitmq_channel.basic_qos(prefetch_count=1)
        rabbitmq_channel.basic_consume(
            queue=RESULT_QUEUE_NAME,
            on_message_callback=lambda ch, method, props, body: callback(
                ch, method, props, body, redis_client
            ),
        )

        logger.info("Result Consumer waiting for results...")
        rabbitmq_channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Result Consumer shutting down by user request")
    except Exception:
        logger.exception("Unexpected error in Result Consumer")
    finally:
        close_rabbit_mq_connection(rabbitmq_connection, rabbitmq_channel)
        close_redis_client(redis_client)


if __name__ == "__main__":
    main()
