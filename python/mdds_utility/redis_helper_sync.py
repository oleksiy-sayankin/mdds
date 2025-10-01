# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Helper for creating and closing connection to Redis client
"""

import logging
import os

from redis import Redis

from common_logging.setup_logging import setup_logging
from mdds_dto.result_dto import ResultDTO


REDIS_TTL = int(os.getenv("REDIS_TTL", 86400))  # default 1 day

# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)


def get_redis_client(redis_host: str, redis_port: int) -> Redis:
    try:
        redis_client = Redis(host=redis_host, port=redis_port, decode_responses=True)
        redis_client.ping()
        logger.info("Connected to Redis.")
        return redis_client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise


def close_redis_client(redis_client: Redis):
    try:
        if redis_client:
            redis_client.close()
            logger.info("Redis connection closed.")
    except Exception as e:
        logger.warning(f"Error closing Redis connection: {e}")


def put_data_to_storage(redis_client: Redis, task_id: str, result: ResultDTO):
    redis_client.set(task_id, result.model_dump_json(), ex=REDIS_TTL)
    logger.info(f"Task {task_id} stored in Redis.")


def get_data_from_storage(redis_client: Redis, task_id: str) -> ResultDTO:
    data = redis_client.get(task_id)
    return ResultDTO.model_validate_json(data)
