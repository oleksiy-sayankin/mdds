"""
Helper for creating and closing connection to Redis client
"""

import logging

import redis
from aioredis import Redis

from common_logging.setup_logging import setup_logging


# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)


async def get_redis_client(redis_host: str, redis_port: int):
    try:
        redis_client = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True
        )
        await redis_client.ping()
        logger.info("Connected to Redis.")
        return redis_client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise


async def close_redis_client(redis_client: Redis):
    try:
        if redis_client:
            await redis_client.close()
            logger.info("Redis connection closed.")
    except Exception as e:
        logger.warning(f"Error closing Redis connection: {e}")
