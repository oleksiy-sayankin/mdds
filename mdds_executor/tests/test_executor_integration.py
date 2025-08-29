# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import csv
from datetime import datetime
import json
import os
import subprocess
import sys
import time
import uuid
import socket

import numpy as np
import pika
import pytest
from pika.spec import PERSISTENT_DELIVERY_MODE

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = 5672
TASK_QUEUE = "task_queue"
RESULT_QUEUE = "result_queue"
RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "resources")
TIME_OUT = 1  # one second
MAX_RETRY_COUNT = 10
TOLERANCE = 1e-6


def is_rabbitmq_running(
    host: str = RABBITMQ_HOST, port: int = RABBITMQ_PORT, timeout: int = 2
) -> bool:
    """Check if RabbitMQ server is accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def rabbitmq_connection():
    """Creates connection to  RabbitMQ and setups the queues."""

    if not is_rabbitmq_running():
        pytest.fail(
            f"Precondition failed: RabbitMQ server is not running or not reachable at {RABBITMQ_HOST}:{RABBITMQ_PORT}"
        )

    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    # Clear queues before test
    channel.queue_delete(queue=TASK_QUEUE)
    channel.queue_delete(queue=RESULT_QUEUE)
    # Re-declare again
    channel.queue_declare(queue=TASK_QUEUE, durable=True)
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)

    yield channel

    # Clear queues after the tests
    channel.queue_delete(queue=TASK_QUEUE)
    channel.queue_delete(queue=RESULT_QUEUE)
    connection.close()


@pytest.fixture(scope="session", autouse=True)
def executor_process(rabbitmq_connection):
    """Runs executor.py in the separate thread and stops after the tests."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "mdds_executor.executor"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(TIME_OUT)  # wait some time while executor is connecting to queue
    yield proc

    proc.terminate()
    try:
        proc.wait(timeout=TIME_OUT)
    except subprocess.TimeoutExpired:
        proc.kill()


def load_csv_as_list(path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        return [[float(x) for x in row] for row in reader]


def test_executor_solves_slae(rabbitmq_connection):
    channel = rabbitmq_connection

    # Upload input data
    matrix = load_csv_as_list(os.path.join(RESOURCES_DIR, "matrix.csv"))
    rhs = load_csv_as_list(os.path.join(RESOURCES_DIR, "rhs.csv"))
    expected_solution = load_csv_as_list(
        os.path.join(RESOURCES_DIR, "expected_solution.csv")
    )

    task_id = str(uuid.uuid4())
    task = {
        "task_id": task_id,
        "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "matrix": matrix,
        "rhs": rhs,
        "slae_solving_method": "numpy_exact_solver",
    }

    # Put task into the queue
    channel.basic_publish(
        exchange="",
        routing_key=TASK_QUEUE,
        body=json.dumps(task).encode(),
        properties=pika.BasicProperties(delivery_mode=PERSISTENT_DELIVERY_MODE),
    )

    # Wait for results from result_queue
    actual_result = None
    for _ in range(MAX_RETRY_COUNT):
        method, _, body = channel.basic_get(RESULT_QUEUE, auto_ack=True)
        if method:
            result = json.loads(body.decode())
            if result["task_id"] == task_id:
                actual_result = result
                break
        time.sleep(TIME_OUT)

    assert actual_result is not None, "No answer from executor"
    assert actual_result["status"] == "done"

    actual_solution = np.array(actual_result["solution"], dtype=float)
    if actual_solution.ndim == 1:  # reshape from (2,) to (2, 1) form
        actual_solution = actual_solution.reshape(-1, 1)

    expected_solution = np.array(expected_solution, dtype=float)

    assert (
        actual_solution.shape == expected_solution.shape
    ), f"Dimensions are different: {actual_solution.shape} vs {expected_solution.shape}"

    # Compare the actual result to expected result with given tolerance
    np.testing.assert_allclose(
        actual_solution, expected_solution, rtol=TOLERANCE, atol=TOLERANCE
    )
