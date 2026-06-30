# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import cast, Any
from unittest.mock import MagicMock, patch

import pytest

import mdds_worker_runtime.rabbitmq.rabbitmq_queue_client as rabbitmq_client
from mdds_worker_runtime.queue.queue_client import QueueMessage
from mdds_worker_runtime.rabbitmq.rabbitmq_queue_client import (
    RabbitMqAcknowledger,
    RabbitMqConnectionError,
    RabbitMqProperties,
    RabbitMqQueueClient,
    RabbitMqSerializationError,
    RabbitMqSubscription,
)

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class SubmittedJobMessage:
    manifestObjectKey: str


@dataclass(frozen=True)
class StatusUpdateMessage:
    jobId: str
    workerId: str
    status: str
    progress: int
    message: str
    eventTime: datetime


class FakeChannel:
    def __init__(self) -> None:
        self.is_open = True
        self.declared: list[dict] = []
        self.published: list[dict] = []
        self.deleted: list[str] = []
        self.acks: list[tuple[int, bool]] = []
        self.nacks: list[tuple[int, bool, bool]] = []
        self.cancelled: list[str] = []
        self.closed = False

    def queue_declare(
        self,
        queue: str,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
    ) -> None:
        self.declared.append(
            {
                "queue": queue,
                "durable": durable,
                "exclusive": exclusive,
                "auto_delete": auto_delete,
            }
        )

    def basic_publish(self, exchange, routing_key, body, properties) -> None:
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )

    def queue_delete(self, queue: str) -> None:
        self.deleted.append(queue)

    def basic_ack(self, delivery_tag: int, multiple: bool = False) -> None:
        self.acks.append((delivery_tag, multiple))

    def basic_nack(
        self,
        delivery_tag: int,
        multiple: bool = False,
        requeue: bool = True,
    ) -> None:
        self.nacks.append((delivery_tag, multiple, requeue))

    def basic_cancel(self, consumer_tag: str) -> None:
        self.cancelled.append(consumer_tag)

    def close(self) -> None:
        self.is_open = False
        self.closed = True


class FakeConnection:
    def __init__(self, channel: FakeChannel | None = None) -> None:
        self.is_open = True
        self._channel = channel or FakeChannel()
        self.callbacks = []
        self.closed = False

    def channel(self) -> FakeChannel:
        return self._channel

    def add_callback_threadsafe(self, callback) -> None:
        self.callbacks.append(callback)
        callback()

    def close(self) -> None:
        self.is_open = False
        self.closed = True


class FakeThread:
    def __init__(self) -> None:
        self.join_calls: list[float | None] = []
        self._alive = False

    def join(self, timeout: float | None = None) -> None:
        self.join_calls.append(timeout)

    def is_alive(self) -> bool:
        return self._alive


class RecordingHandler:
    def __init__(self, *, ack: bool = False, fail: bool = False) -> None:
        self.ack_message = ack
        self.fail = fail
        self.received_message = None
        self.received_acknowledger = None

    def handle(self, message, ack) -> None:
        self.received_message = message
        self.received_acknowledger = ack

        if self.fail:
            raise RuntimeError("handler failed")

        if self.ack_message:
            ack.ack()


@pytest.fixture
def rabbitmq_properties() -> RabbitMqProperties:
    return RabbitMqProperties(
        host="rabbitmq.mdds.com",
        port=5672,
        user="mdds",
        password="secret",
        connection_timeout_seconds=0.1,
    )


@pytest.fixture
def fake_client_connection(monkeypatch, rabbitmq_properties):
    channel = FakeChannel()
    connection = FakeConnection(channel)

    monkeypatch.setattr(
        rabbitmq_client,
        "_create_connection_with_retry",
        lambda properties: connection,
    )

    client = RabbitMqQueueClient(rabbitmq_properties, clock=lambda: FIXED_TIME)

    try:
        yield client, connection, channel
    finally:
        client.close()


def test_publish_declares_queue_and_publishes_json_payload(
    fake_client_connection,
) -> None:
    client, _, channel = fake_client_connection

    message = QueueMessage(
        payload=SubmittedJobMessage("jobs/42/job-1/manifest.json"),
        headers={"traceId": "trace-1"},
        timestamp=FIXED_TIME,
    )

    client.publish("queue-solving_slae", message)

    assert channel.declared == [
        {
            "queue": "queue-solving_slae",
            "durable": False,
            "exclusive": False,
            "auto_delete": False,
        }
    ]

    assert len(channel.published) == 1
    published = channel.published[0]

    assert published["exchange"] == ""
    assert published["routing_key"] == "queue-solving_slae"
    assert json.loads(published["body"].decode("utf-8")) == {
        "manifestObjectKey": "jobs/42/job-1/manifest.json"
    }
    assert published["properties"].headers == {"traceId": "trace-1"}


def test_publish_rejects_blank_queue_name(fake_client_connection) -> None:
    client, _, _ = fake_client_connection

    with pytest.raises(ValueError, match="queue_name cannot be null or blank"):
        client.publish(" ", QueueMessage(payload={"value": 1}))


def test_publish_rejects_null_message(fake_client_connection) -> None:
    client, _, _ = fake_client_connection

    with pytest.raises(ValueError, match="message cannot be null"):
        client.publish("queue-solving_slae", None)


def test_publish_wraps_channel_error(monkeypatch, rabbitmq_properties) -> None:
    channel = FakeChannel()
    connection = FakeConnection(channel)

    def fail_publish(*args, **kwargs) -> None:
        raise OSError("publish failed")

    channel.basic_publish = fail_publish

    monkeypatch.setattr(
        rabbitmq_client,
        "_create_connection_with_retry",
        lambda properties: connection,
    )

    client = RabbitMqQueueClient(rabbitmq_properties)

    try:
        with pytest.raises(RabbitMqConnectionError, match="Failed to publish"):
            client.publish("queue-solving_slae", QueueMessage(payload={"value": 1}))
    finally:
        client.close()


def test_delete_queue_deletes_queue(fake_client_connection) -> None:
    client, _, channel = fake_client_connection

    client.delete_queue("queue-solving_slae")

    assert channel.deleted == ["queue-solving_slae"]


def test_delete_queue_rejects_blank_queue_name(fake_client_connection) -> None:
    client, _, _ = fake_client_connection

    with pytest.raises(ValueError, match="queue_name cannot be null or blank"):
        client.delete_queue("")


def test_client_close_closes_subscriptions_channel_and_connection(
    fake_client_connection,
) -> None:
    client, connection, channel = fake_client_connection
    subscription = MagicMock()

    client._subscriptions.append(subscription)

    client.close()
    client.close()

    subscription.close.assert_called_once_with()
    assert channel.closed is True
    assert connection.closed is True


def test_publish_after_close_raises(fake_client_connection) -> None:
    client, _, _ = fake_client_connection

    client.close()

    with pytest.raises(RabbitMqConnectionError, match="already closed"):
        client.publish("queue-solving_slae", QueueMessage(payload={"value": 1}))


def test_subscribe_creates_and_stores_subscription(fake_client_connection) -> None:
    client, _, _ = fake_client_connection
    handler = RecordingHandler()
    expected_subscription = MagicMock()

    with patch.object(
        rabbitmq_client,
        "RabbitMqSubscription",
        return_value=expected_subscription,
    ) as subscription_type:
        actual = client.subscribe("queue-solving_slae", SubmittedJobMessage, handler)

    assert actual is expected_subscription
    assert client._subscriptions == [expected_subscription]
    subscription_type.assert_called_once()

    _, kwargs = subscription_type.call_args
    assert kwargs["properties"] is client._properties
    assert kwargs["queue_name"] == "queue-solving_slae"
    assert kwargs["payload_type"] is SubmittedJobMessage
    assert kwargs["handler"] is handler
    assert kwargs["clock"] is client._clock
    assert kwargs["requeue_on_handler_error"] is True
    assert kwargs["prefetch_count"] == 1


def test_client_rejects_null_properties() -> None:
    with pytest.raises(ValueError, match="properties cannot be null"):
        RabbitMqQueueClient(None)


def test_client_rejects_non_positive_prefetch_count(rabbitmq_properties) -> None:
    with pytest.raises(ValueError, match="prefetch_count must be greater than zero"):
        RabbitMqQueueClient(rabbitmq_properties, prefetch_count=0)


def test_acknowledger_ack_schedules_basic_ack_once() -> None:
    channel = FakeChannel()
    connection = FakeConnection(channel)
    acknowledger = RabbitMqAcknowledger(
        connection=connection,
        channel=channel,
        delivery_tag=123,
        queue_name="queue-solving_slae",
    )

    acknowledger.ack()
    acknowledger.ack()
    acknowledger.nack(requeue=True)

    assert len(connection.callbacks) == 1
    assert channel.acks == [(123, False)]
    assert channel.nacks == []


def test_acknowledger_nack_schedules_basic_nack_once() -> None:
    channel = FakeChannel()
    connection = FakeConnection(channel)
    acknowledger = RabbitMqAcknowledger(
        connection=connection,
        channel=channel,
        delivery_tag=124,
        queue_name="queue-solving_slae",
    )

    acknowledger.nack(requeue=False)
    acknowledger.ack()

    assert len(connection.callbacks) == 1
    assert channel.acks == []
    assert channel.nacks == [(124, False, False)]


def test_acknowledger_raises_when_callback_scheduling_fails() -> None:
    channel = FakeChannel()
    connection = FakeConnection(channel)

    def fail(callback) -> None:
        raise RuntimeError("scheduler failed")

    connection.add_callback_threadsafe = fail

    acknowledger = RabbitMqAcknowledger(
        connection=connection,
        channel=channel,
        delivery_tag=125,
        queue_name="queue-solving_slae",
    )

    with pytest.raises(RabbitMqConnectionError, match="Failed to schedule ack/nack"):
        acknowledger.ack()


def test_subscription_on_message_deserializes_payload_and_calls_handler() -> None:
    subscription, connection, channel = _subscription_for_on_message(
        payload_type=SubmittedJobMessage,
        handler=RecordingHandler(ack=True),
    )

    subscription._on_message(
        channel=channel,
        method=SimpleNamespace(delivery_tag=10),
        properties=SimpleNamespace(headers={"traceId": "trace-1"}),
        body=b'{"manifestObjectKey":"jobs/42/job-1/manifest.json"}',
    )

    handler = subscription._handler
    assert handler.received_message.payload == SubmittedJobMessage(
        "jobs/42/job-1/manifest.json"
    )
    assert handler.received_message.headers == {"traceId": "trace-1"}
    assert handler.received_message.timestamp == FIXED_TIME
    assert handler.received_acknowledger is not None

    assert len(connection.callbacks) == 1
    assert channel.acks == [(10, False)]
    assert channel.nacks == []


def test_subscription_on_message_nacks_invalid_json() -> None:
    handler = RecordingHandler()
    subscription, connection, channel = _subscription_for_on_message(
        payload_type=SubmittedJobMessage,
        handler=handler,
        requeue_on_handler_error=True,
    )

    subscription._on_message(
        channel=channel,
        method=SimpleNamespace(delivery_tag=11),
        properties=SimpleNamespace(headers={}),
        body=b"not-json",
    )

    assert handler.received_message is None
    assert len(connection.callbacks) == 1
    assert channel.acks == []
    assert channel.nacks == [(11, False, True)]


def test_subscription_on_message_nacks_when_handler_fails() -> None:
    handler = RecordingHandler(fail=True)
    subscription, connection, channel = _subscription_for_on_message(
        payload_type=SubmittedJobMessage,
        handler=handler,
        requeue_on_handler_error=False,
    )

    subscription._on_message(
        channel=channel,
        method=SimpleNamespace(delivery_tag=12),
        properties=SimpleNamespace(headers={}),
        body=b'{"manifestObjectKey":"jobs/42/job-1/manifest.json"}',
    )

    assert handler.received_message.payload == SubmittedJobMessage(
        "jobs/42/job-1/manifest.json"
    )
    assert len(connection.callbacks) == 1
    assert channel.acks == []
    assert channel.nacks == [(12, False, False)]


def test_subscription_close_schedules_basic_cancel() -> None:
    channel = FakeChannel()
    connection = FakeConnection(channel)

    subscription = object.__new__(RabbitMqSubscription)
    subscription._closed = threading.Event()
    subscription._lock = threading.Lock()
    subscription._connection = connection
    subscription._channel = channel
    subscription._consumer_tag = "consumer-tag-1"
    subscription._thread = FakeThread()
    subscription._queue_name = "queue-solving_slae"

    subscription.close()
    subscription.close()

    assert connection.callbacks
    assert channel.cancelled == ["consumer-tag-1"]
    assert subscription._thread.join_calls == [
        rabbitmq_client.DEFAULT_CLOSE_TIMEOUT_SECONDS
    ]


def test_serialize_payload_supports_dataclass_and_datetime() -> None:
    payload = StatusUpdateMessage(
        jobId="job-1",
        workerId="worker-1",
        status="IN_PROGRESS",
        progress=42,
        message="Worker is processing job",
        eventTime=FIXED_TIME,
    )

    serialized = rabbitmq_client._serialize_payload(payload)

    assert json.loads(serialized.decode("utf-8")) == {
        "jobId": "job-1",
        "workerId": "worker-1",
        "status": "IN_PROGRESS",
        "progress": 42,
        "message": "Worker is processing job",
        "eventTime": "2026-01-01T00:00:00Z",
    }


def test_deserialize_payload_to_dataclass() -> None:
    payload = rabbitmq_client._deserialize_payload(
        b'{"manifestObjectKey":"jobs/42/job-1/manifest.json"}',
        SubmittedJobMessage,
    )

    assert payload == SubmittedJobMessage("jobs/42/job-1/manifest.json")


def test_deserialize_payload_to_dict() -> None:
    payload = rabbitmq_client._deserialize_payload(
        b'{"manifestObjectKey":"jobs/42/job-1/manifest.json"}',
        dict,
    )

    assert payload == {"manifestObjectKey": "jobs/42/job-1/manifest.json"}


def test_deserialize_payload_rejects_invalid_json() -> None:
    with pytest.raises(RabbitMqSerializationError, match="Failed to deserialize"):
        rabbitmq_client._deserialize_payload(b"not-json", SubmittedJobMessage)


def test_deserialize_payload_rejects_wrong_shape_for_dataclass() -> None:
    with pytest.raises(RabbitMqSerializationError, match="Failed to deserialize"):
        rabbitmq_client._deserialize_payload(b'["not", "object"]', SubmittedJobMessage)


def test_declare_queue_wraps_channel_error() -> None:
    channel = FakeChannel()

    def fail_queue_declare(*args, **kwargs) -> None:
        raise OSError("queue declare failed")

    channel.queue_declare = fail_queue_declare

    with pytest.raises(RabbitMqConnectionError, match="Failed to declare"):
        rabbitmq_client._declare_queue(channel, "queue-solving_slae")


def test_connection_parameters_are_created_from_properties() -> None:
    properties = RabbitMqProperties(
        host="rabbitmq.mdds.com",
        port=5672,
        user="mdds",
        password="secret",
        max_inbound_message_body_size=4096,
        connection_timeout_seconds=7,
    )

    params = rabbitmq_client._connection_parameters(properties)

    assert params.host == "rabbitmq.mdds.com"
    assert params.port == 5672
    assert params.frame_max == 4096
    assert params.heartbeat == 60
    assert params.blocked_connection_timeout == 7


def _subscription_for_on_message(
    *,
    payload_type,
    handler,
    requeue_on_handler_error: bool = True,
):
    channel = FakeChannel()
    connection = FakeConnection(channel)

    subscription = object.__new__(RabbitMqSubscription)
    subscription._queue_name = "queue-solving_slae"
    subscription._payload_type = payload_type
    subscription._handler = handler
    subscription._clock = lambda: FIXED_TIME
    subscription._requeue_on_handler_error = requeue_on_handler_error
    subscription._lock = threading.Lock()
    subscription._connection = connection

    return subscription, connection, channel


def test_rabbitmq_queue_client_check_readiness_succeeds_when_connection_and_channel_are_open() -> (
    None
):
    client = _rabbitmq_client_for_readiness()

    client.check_readiness()

    client._connection.process_data_events.assert_called_once_with(time_limit=0)


def test_rabbitmq_queue_client_check_readiness_logs_started_and_completed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _rabbitmq_client_for_readiness()

    with caplog.at_level(
        logging.INFO,
        logger="mdds_worker_runtime.rabbitmq.rabbitmq_queue_client",
    ):
        client.check_readiness()

    events = [record.__dict__.get("event") for record in caplog.records]

    assert "rabbitmq_readiness_check_started" in events
    assert "rabbitmq_readiness_check_completed" in events


def test_rabbitmq_queue_client_check_readiness_rejects_closed_client() -> None:
    client = _rabbitmq_client_for_readiness(closed=True)

    with pytest.raises(
        RabbitMqConnectionError,
        match="RabbitMQ queue client is already closed.",
    ):
        client.check_readiness()

    client._connection.process_data_events.assert_not_called()


def test_rabbitmq_queue_client_check_readiness_rejects_closed_connection() -> None:
    client = _rabbitmq_client_for_readiness(connection_is_open=False)

    with pytest.raises(
        RabbitMqConnectionError,
        match="RabbitMQ readiness check failed: connection is not open.",
    ):
        client.check_readiness()

    client._connection.process_data_events.assert_not_called()


def test_rabbitmq_queue_client_check_readiness_rejects_closed_channel() -> None:
    client = _rabbitmq_client_for_readiness(channel_is_open=False)

    with pytest.raises(
        RabbitMqConnectionError,
        match="RabbitMQ readiness check failed: channel is not open.",
    ):
        client.check_readiness()

    client._connection.process_data_events.assert_not_called()


def test_rabbitmq_queue_client_check_readiness_wraps_process_data_events_failure() -> (
    None
):
    client = _rabbitmq_client_for_readiness()
    original_error = RuntimeError("broker connection lost")
    client._connection.process_data_events.side_effect = original_error

    with pytest.raises(
        RabbitMqConnectionError,
        match="RabbitMQ connection readiness check failed.",
    ) as error:
        client.check_readiness()

    assert error.value.__cause__ is original_error


def test_rabbitmq_queue_client_check_readiness_logs_failed_event_with_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _rabbitmq_client_for_readiness()
    client._connection.process_data_events.side_effect = RuntimeError(
        "broker connection lost"
    )

    with caplog.at_level(
        logging.ERROR,
        logger="mdds_worker_runtime.rabbitmq.rabbitmq_queue_client",
    ):
        with pytest.raises(RabbitMqConnectionError):
            client.check_readiness()

    failed_records = [
        record
        for record in caplog.records
        if record.__dict__.get("event") == "rabbitmq_readiness_check_failed"
    ]

    assert len(failed_records) == 1
    assert failed_records[0].exc_info is not None


def test_rabbitmq_queue_client_check_messaging_readiness_uses_unique_probe_queue_and_publishes_nonce() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    subscription = MagicMock(name="subscription")
    acknowledger = MagicMock(name="acknowledger")
    captured: dict[str, Any] = {}

    def subscribe(queue_name, payload_type, handler):
        captured["subscribed_queue_name"] = queue_name
        captured["payload_type"] = payload_type
        captured["handler"] = handler
        return subscription

    def publish(queue_name, message):
        captured["published_queue_name"] = queue_name
        captured["published_message"] = message

        handler = captured["handler"]
        handler.handle(QueueMessage(payload=message.payload), acknowledger)

    client.subscribe = MagicMock(side_effect=subscribe)
    client.publish = MagicMock(side_effect=publish)
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        client.check_messaging_readiness(timeout_seconds=0.1)

    assert captured["subscribed_queue_name"] == ("mdds.rabbitmq.readiness.probequeue")
    assert captured["payload_type"] is dict
    assert captured["published_queue_name"] == ("mdds.rabbitmq.readiness.probequeue")
    assert captured["published_message"].payload == {"nonce": "probenonce"}

    acknowledger.ack.assert_called_once_with()
    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_logs_started_and_completed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _rabbitmq_client_for_successful_messaging_readiness()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with caplog.at_level(
            logging.INFO,
            logger="mdds_worker_runtime.rabbitmq.rabbitmq_queue_client",
        ):
            client.check_messaging_readiness(timeout_seconds=0.1)

    events = [record.__dict__.get("event") for record in caplog.records]

    assert "rabbitmq_messaging_readiness_check_started" in events
    assert "rabbitmq_messaging_readiness_check_completed" in events


def test_rabbitmq_queue_client_check_messaging_readiness_times_out_when_probe_message_is_not_consumed() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    subscription = MagicMock(name="subscription")

    client.subscribe = MagicMock(return_value=subscription)
    client.publish = MagicMock()
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with pytest.raises(
            RabbitMqConnectionError,
            match="timed out waiting for probe message",
        ):
            client.check_messaging_readiness(timeout_seconds=0.001)

    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_rejects_unexpected_probe_payload() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    subscription = MagicMock(name="subscription")
    acknowledger = MagicMock(name="acknowledger")
    captured: dict[str, Any] = {}

    def subscribe(queue_name, payload_type, handler):
        captured["handler"] = handler
        return subscription

    def publish(queue_name, message):
        captured["handler"].handle(
            QueueMessage(payload={"nonce": "wrong-nonce"}),
            acknowledger,
        )

    client.subscribe = MagicMock(side_effect=subscribe)
    client.publish = MagicMock(side_effect=publish)
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="expectednonce"),
        ],
    ):
        with pytest.raises(
            RabbitMqConnectionError,
            match="unexpected probe payload",
        ):
            client.check_messaging_readiness(timeout_seconds=0.1)

    acknowledger.ack.assert_called_once_with()
    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_wraps_ack_failure() -> None:
    client = _rabbitmq_client_for_messaging_readiness()
    subscription = MagicMock(name="subscription")
    original_error = RuntimeError("ack scheduling failed")
    acknowledger = MagicMock(name="acknowledger")
    acknowledger.ack.side_effect = original_error
    captured: dict[str, Any] = {}

    def subscribe(queue_name, payload_type, handler):
        captured["handler"] = handler
        return subscription

    def publish(queue_name, message):
        captured["handler"].handle(message, acknowledger)

    client.subscribe = MagicMock(side_effect=subscribe)
    client.publish = MagicMock(side_effect=publish)
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with pytest.raises(
            RabbitMqConnectionError,
            match="failed to acknowledge probe message",
        ) as error:
            client.check_messaging_readiness(timeout_seconds=0.1)

    assert error.value.__cause__ is original_error
    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_wraps_subscribe_failure_and_attempts_cleanup() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    original_error = RuntimeError("subscribe failed")

    client.subscribe = MagicMock(side_effect=original_error)
    client.publish = MagicMock()
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with pytest.raises(
            RabbitMqConnectionError,
            match="RabbitMQ messaging readiness check failed.",
        ) as error:
            client.check_messaging_readiness(timeout_seconds=0.1)

    assert error.value.__cause__ is original_error
    client.publish.assert_not_called()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_wraps_publish_failure_and_cleans_up() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    subscription = MagicMock(name="subscription")
    original_error = RuntimeError("publish failed")

    client.subscribe = MagicMock(return_value=subscription)
    client.publish = MagicMock(side_effect=original_error)
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with pytest.raises(
            RabbitMqConnectionError,
            match="RabbitMQ messaging readiness check failed.",
        ) as error:
            client.check_messaging_readiness(timeout_seconds=0.1)

    assert error.value.__cause__ is original_error
    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_fails_when_subscription_close_fails_after_successful_probe() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    close_error = RuntimeError("subscription close failed")
    subscription = MagicMock(name="subscription")
    subscription.close.side_effect = close_error

    _configure_successful_messaging_probe(client, subscription=subscription)

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with pytest.raises(
            RabbitMqConnectionError,
            match="RabbitMQ messaging readiness cleanup failed.",
        ) as error:
            client.check_messaging_readiness(timeout_seconds=0.1)

    assert error.value.__cause__ is close_error
    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_fails_when_probe_queue_delete_fails_after_successful_probe() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    delete_error = RuntimeError("queue delete failed")
    subscription = MagicMock(name="subscription")

    _configure_successful_messaging_probe(client, subscription=subscription)
    client.delete_queue.side_effect = delete_error

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with pytest.raises(
            RabbitMqConnectionError,
            match="RabbitMQ messaging readiness cleanup failed.",
        ) as error:
            client.check_messaging_readiness(timeout_seconds=0.1)

    assert error.value.__cause__ is delete_error
    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_cleanup_failure_after_primary_failure_does_not_hide_primary_failure() -> (
    None
):
    client = _rabbitmq_client_for_messaging_readiness()
    primary_error = RabbitMqConnectionError("publish failed")
    cleanup_error = RuntimeError("subscription close failed")
    subscription = MagicMock(name="subscription")
    subscription.close.side_effect = cleanup_error

    client.subscribe = MagicMock(return_value=subscription)
    client.publish = MagicMock(side_effect=primary_error)
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with pytest.raises(RabbitMqConnectionError) as error:
            client.check_messaging_readiness(timeout_seconds=0.1)

    assert str(error.value) == "RabbitMQ messaging readiness check failed."
    assert error.value.__cause__ is primary_error
    subscription.close.assert_called_once_with()
    client.delete_queue.assert_called_once_with("mdds.rabbitmq.readiness.probequeue")


def test_rabbitmq_queue_client_check_messaging_readiness_logs_failed_event_with_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _rabbitmq_client_for_messaging_readiness()
    client.subscribe = MagicMock(side_effect=RuntimeError("subscribe failed"))
    client.publish = MagicMock()
    client.delete_queue = MagicMock()

    with patch.object(
        rabbitmq_client.uuid,
        "uuid4",
        side_effect=[
            SimpleNamespace(hex="probequeue"),
            SimpleNamespace(hex="probenonce"),
        ],
    ):
        with caplog.at_level(
            logging.ERROR,
            logger="mdds_worker_runtime.rabbitmq.rabbitmq_queue_client",
        ):
            with pytest.raises(RabbitMqConnectionError):
                client.check_messaging_readiness(timeout_seconds=0.1)

    failed_records = [
        record
        for record in caplog.records
        if record.__dict__.get("event") == "rabbitmq_messaging_readiness_check_failed"
    ]

    assert len(failed_records) == 1
    assert failed_records[0].exc_info is not None


def _rabbitmq_client_for_readiness(
    *,
    closed: bool = False,
    connection_is_open: bool = True,
    channel_is_open: bool = True,
) -> RabbitMqQueueClient:
    client = RabbitMqQueueClient.__new__(RabbitMqQueueClient)

    client._properties = RabbitMqProperties(
        host="rabbitmq",
        port=5672,
        user="mdds",
        password="secret",
    )
    client._connection = MagicMock(name="connection")
    client._connection.is_open = connection_is_open
    client._channel = MagicMock(name="channel")
    client._channel.is_open = channel_is_open
    client._subscriptions = []
    client._closed = closed
    client._lock = threading.RLock()

    return cast(RabbitMqQueueClient, client)


def _rabbitmq_client_for_messaging_readiness() -> RabbitMqQueueClient:
    client = RabbitMqQueueClient.__new__(RabbitMqQueueClient)

    client._properties = RabbitMqProperties(
        host="rabbitmq",
        port=5672,
        user="mdds",
        password="secret",
        connection_timeout_seconds=0.1,
    )
    client._subscriptions = []
    client._closed = False
    client._lock = threading.RLock()

    return cast(RabbitMqQueueClient, client)


def _rabbitmq_client_for_successful_messaging_readiness() -> RabbitMqQueueClient:
    client = _rabbitmq_client_for_messaging_readiness()
    _configure_successful_messaging_probe(client)
    return client


def _configure_successful_messaging_probe(
    client: RabbitMqQueueClient,
    *,
    subscription: MagicMock | None = None,
) -> MagicMock:
    resolved_subscription = subscription or MagicMock(name="subscription")
    acknowledger = MagicMock(name="acknowledger")
    captured: dict[str, Any] = {}

    def subscribe(queue_name, payload_type, handler):
        captured["handler"] = handler
        return resolved_subscription

    def publish(queue_name, message):
        captured["handler"].handle(message, acknowledger)

    client.subscribe = MagicMock(side_effect=subscribe)
    client.publish = MagicMock(side_effect=publish)
    client.delete_queue = MagicMock()

    return resolved_subscription
