# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import queue
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika
import pytest
from testcontainers.rabbitmq import RabbitMqContainer

from mdds_worker_runtime.dto.messages import JobStatusUpdateDTO
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.rabbitmq.rabbitmq_queue_client import (
    RabbitMqProperties,
    RabbitMqQueueClient,
)

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
RABBITMQ_IMAGE = "rabbitmq:3.12-management"


class StatusUpdateHandler:
    def __init__(self) -> None:
        self.received: queue.Queue = queue.Queue()
        self.received_event = threading.Event()

    def handle(self, message, ack) -> None:
        self.received.put(message)
        ack.ack()
        self.received_event.set()


@pytest.fixture(scope="session")
def rabbitmq_container():
    with RabbitMqContainer(
        RABBITMQ_IMAGE,
        username="guest",
        password="guest",
    ) as container:
        yield container


@pytest.fixture
def rabbitmq_properties(rabbitmq_container) -> RabbitMqProperties:
    connection_params = rabbitmq_container.get_connection_params()

    return RabbitMqProperties(
        host=connection_params.host,
        port=connection_params.port,
        user="guest",
        password="guest",
        connection_timeout_seconds=20.0,
    )


def test_status_publisher_publishes_status_update_to_real_rabbitmq(
    rabbitmq_properties: RabbitMqProperties,
) -> None:
    queue_name = _new_queue_name("mdds-status-queue")
    handler = StatusUpdateHandler()

    with RabbitMqQueueClient(
        rabbitmq_properties,
        clock=lambda: FIXED_TIME,
        prefetch_count=1,
    ) as client:
        subscription = client.subscribe(queue_name, JobStatusUpdateDTO, handler)

        publisher = StatusPublisher(
            worker_status_queue_name=queue_name,
            queue_client=client,
            clock=lambda: FIXED_TIME,
        )

        publisher.publish_in_progress(
            user_id=42,
            job_id="job-1",
            job_type="SOLVING_SLAE",
            worker_id="worker-1",
            progress=0,
            message="Start job execution",
        )

        assert handler.received_event.wait(
            timeout=15
        ), "RabbitMQ status update message was not consumed."

        message = handler.received.get_nowait()

        assert message.payload == JobStatusUpdateDTO(
            jobId="job-1",
            workerId="worker-1",
            status=WorkerJobStatus.IN_PROGRESS.value,
            progress=0,
            message="Start job execution",
            eventTime="2026-01-01T00:00:00Z",
        )
        assert message.payload.job_id == "job-1"
        assert message.payload.worker_id == "worker-1"
        assert message.payload.event_time == "2026-01-01T00:00:00Z"
        assert message.headers == {}
        assert message.timestamp == FIXED_TIME

        _wait_for_ack_to_be_processed()

        subscription.close()

        assert _basic_get(rabbitmq_properties, queue_name) is None

        client.delete_queue(queue_name)


def _new_queue_name(prefix: str) -> str:
    return f"{prefix}-{uuid4()}"


def _wait_for_ack_to_be_processed() -> None:
    # RabbitMqAcknowledger schedules ack/nack via connection.add_callback_threadsafe(...).
    # Give the consumer thread one more process_data_events(...) cycle before closing.
    time.sleep(0.3)


def _basic_get(
    properties: RabbitMqProperties,
    queue_name: str,
):
    connection = pika.BlockingConnection(_connection_parameters(properties))
    try:
        channel = connection.channel()
        method_frame, _header_frame, body = channel.basic_get(
            queue=queue_name,
            auto_ack=True,
        )
        return None if method_frame is None else body
    finally:
        connection.close()


def _connection_parameters(properties: RabbitMqProperties) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=properties.host,
        port=properties.port,
        credentials=pika.PlainCredentials(properties.user, properties.password),
        heartbeat=60,
        blocked_connection_timeout=properties.connection_timeout_seconds,
        connection_attempts=1,
    )
