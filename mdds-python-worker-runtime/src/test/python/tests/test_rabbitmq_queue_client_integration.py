# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

import pika
import pytest
from testcontainers.rabbitmq import RabbitMqContainer

from mdds_worker_runtime.queue.queue_client import QueueMessage
from mdds_worker_runtime.rabbitmq.rabbitmq_queue_client import (
    RabbitMqProperties,
    RabbitMqQueueClient,
)

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
RABBITMQ_IMAGE = "rabbitmq:3.12-management"


@dataclass(frozen=True)
class SubmittedJobMessage:
    manifestObjectKey: str


class AckingHandler:
    def __init__(self) -> None:
        self.received: queue.Queue = queue.Queue()
        self.received_event = threading.Event()

    def handle(self, message, ack) -> None:
        self.received.put(message)
        ack.ack()
        self.received_event.set()


class FailingHandler:
    def __init__(self) -> None:
        self.received: queue.Queue = queue.Queue()
        self.received_event = threading.Event()

    def handle(self, message, ack) -> None:
        self.received.put(message)
        self.received_event.set()
        raise RuntimeError("handler failed intentionally")


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


def test_publish_and_subscribe_message_with_real_rabbitmq(
    rabbitmq_properties: RabbitMqProperties,
) -> None:
    queue_name = _new_queue_name("queue-solving_slae")
    handler = AckingHandler()

    with RabbitMqQueueClient(
        rabbitmq_properties,
        clock=lambda: FIXED_TIME,
        prefetch_count=1,
    ) as client:
        subscription = client.subscribe(queue_name, SubmittedJobMessage, handler)

        client.publish(
            queue_name,
            QueueMessage(
                payload=SubmittedJobMessage("jobs/42/job-1/manifest.json"),
                headers={"traceId": "trace-1"},
                timestamp=FIXED_TIME,
            ),
        )

        assert handler.received_event.wait(
            timeout=15
        ), "RabbitMQ message was not consumed."

        message = handler.received.get_nowait()
        assert message.payload == SubmittedJobMessage("jobs/42/job-1/manifest.json")
        assert message.headers == {"traceId": "trace-1"}
        assert message.timestamp == FIXED_TIME

        _wait_for_ack_to_be_processed()

        subscription.close()

        assert _basic_get(rabbitmq_properties, queue_name) is None

        client.delete_queue(queue_name)


def test_handler_failure_nacks_message_without_requeue_when_configured(
    rabbitmq_properties: RabbitMqProperties,
) -> None:
    queue_name = _new_queue_name("queue-solving_slae")
    handler = FailingHandler()

    with RabbitMqQueueClient(
        rabbitmq_properties,
        clock=lambda: FIXED_TIME,
        requeue_on_handler_error=False,
        prefetch_count=1,
    ) as client:
        subscription = client.subscribe(queue_name, SubmittedJobMessage, handler)

        client.publish(
            queue_name,
            QueueMessage(
                payload=SubmittedJobMessage("jobs/42/job-2/manifest.json"),
                headers={"traceId": "trace-2"},
                timestamp=FIXED_TIME,
            ),
        )

        assert handler.received_event.wait(
            timeout=15
        ), "RabbitMQ message was not consumed."

        message = handler.received.get_nowait()
        assert message.payload == SubmittedJobMessage("jobs/42/job-2/manifest.json")
        assert message.headers == {"traceId": "trace-2"}

        _wait_for_ack_to_be_processed()

        subscription.close()

        assert _basic_get(rabbitmq_properties, queue_name) is None

        client.delete_queue(queue_name)


def test_delete_queue_removes_real_rabbitmq_queue(
    rabbitmq_properties: RabbitMqProperties,
) -> None:
    queue_name = _new_queue_name("queue-delete-test")

    with RabbitMqQueueClient(rabbitmq_properties) as client:
        client.publish(
            queue_name,
            QueueMessage(
                payload=SubmittedJobMessage("jobs/42/job-delete/manifest.json"),
                timestamp=FIXED_TIME,
            ),
        )

        assert _queue_exists(rabbitmq_properties, queue_name)

        client.delete_queue(queue_name)

        assert not _queue_exists(rabbitmq_properties, queue_name)


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
        method_frame, header_frame, body = channel.basic_get(
            queue=queue_name,
            auto_ack=True,
        )
        return None if method_frame is None else body
    finally:
        connection.close()


def _queue_exists(properties: RabbitMqProperties, queue_name: str) -> bool:
    connection = pika.BlockingConnection(_connection_parameters(properties))
    try:
        channel = connection.channel()
        try:
            channel.queue_declare(queue=queue_name, passive=True)
            return True
        except pika.exceptions.ChannelClosedByBroker:
            return False
    finally:
        if connection.is_open:
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
