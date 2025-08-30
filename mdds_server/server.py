# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Main entry point
"""
import json
import logging
import uuid
import pika
import os

from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime, UTC
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pika.adapters.blocking_connection import BlockingChannel
from aioredis import Redis
from pika.spec import PERSISTENT_DELIVERY_MODE
from common_logging.setup_logging import setup_logging
from mdds_server._csv_helper import load_matrix
from mdds_server._rabbitmq_helper import (
    connect_to_rabbit_mq,
    close_rabbit_mq_connection,
)
from mdds_server._redis_helper import get_redis_client, close_redis_client

# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)

app = FastAPI()

# FastAPI app
CLIENT_DIR = os.path.join(os.path.dirname(__file__), "..", "mdds_client")
app.mount("/static", StaticFiles(directory=CLIENT_DIR, html=True), name="static")

# Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TASK_QUEUE_NAME = "task_queue"
RESULT_QUEUE_NAME = "result_queue"
REDIS_TTL = int(os.getenv("REDIS_TTL", 86400))  # default 1 day

# Global connections
rabbitmq_channel: Optional[BlockingChannel] = None
redis_client: Optional[Redis] = None

INITIAL_TASK_STATUS = "task_in_progress"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize RabbitMQ and Redis connections at startup, close at shutdown"""
    global rabbitmq_channel, redis_client
    rabbitmq_connection, rabbitmq_channel = connect_to_rabbit_mq(
        RABBITMQ_HOST, TASK_QUEUE_NAME, RESULT_QUEUE_NAME
    )
    redis_client = await get_redis_client(REDIS_HOST, REDIS_PORT)
    yield  # <-- Application works here
    close_rabbit_mq_connection(rabbitmq_connection, rabbitmq_channel)
    await close_redis_client(redis_client)


@app.get("/")
def index():
    """
    Root endpoint.

    :return: Main page index.html
    """
    return FileResponse(os.path.join(CLIENT_DIR, "index.html"))


@app.get("/health")
def health():
    """
    Health check for server

    :return: always returns ok as health status
    """
    return {"status": "ok"}


@app.post("/solve")
async def solve_endpoint(
    matrix: UploadFile = File(...), rhs: UploadFile = File(...), method: str = Form(...)
):
    """
    Accepts SLAE input, registers task in Redis, and publishes it to RabbitMQ.
    """
    logging.info(f"Received request to solve SLAE with method: {method}")

    # Load files into numpy arrays
    matrix = load_matrix(matrix)
    rhs = load_matrix(rhs)
    logger.info(f"Loaded matrix A rows: {len(matrix)}, vector b length: {len(rhs)}")

    # Generate task_id
    task_id = str(uuid.uuid4())
    date_time_created = datetime.now(UTC).isoformat()

    # Store initial task state in Redis
    task_doc = {
        "task_id": task_id,
        "date_time_created": date_time_created,
        "status": INITIAL_TASK_STATUS,
    }

    assert redis_client is not None
    await redis_client.set(task_id, json.dumps(task_doc), ex=REDIS_TTL)
    logger.info(f"Task {task_id} stored in Redis with status '{INITIAL_TASK_STATUS}'.")

    # Publish task to RabbitMQ
    task_message = {
        "task_id": task_id,
        "date_time_created": date_time_created,
        "matrix": matrix,
        "rhs": rhs,
        "slae_solving_method": method,
    }

    assert rabbitmq_channel is not None
    rabbitmq_channel.basic_publish(
        exchange="",
        routing_key=TASK_QUEUE_NAME,
        body=json.dumps(task_message),
        properties=pika.BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE
        ),  # make message persistent
    )
    logger.info(f"Task {task_id} published to RabbitMQ.")

    # Return just task_id (client polls results later)
    return {"task_id": task_id}


@app.get("/result/{task_id}")
async def get_result_endpoint(task_id: str):
    """
    Retrieves task status/result from Redis.
    """
    task_data = await redis_client.get(task_id)
    if not task_data:
        return {"error": "no_task_with_with_provided_id"}

    return json.loads(task_data)
