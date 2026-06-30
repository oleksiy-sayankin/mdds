# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import dataclasses
import enum
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TypeVar, get_origin

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from mdds_worker_runtime.queue.queue_client import (
    Acknowledger,
    MessageHandler,
    QueueClient,
    QueueMessage,
    Subscription,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)

DEFAULT_MAX_INBOUND_MESSAGE_BODY_SIZE = 131_072
DEFAULT_CONNECTION_TIMEOUT_SECONDS = 60.0
DEFAULT_RETRY_INTERVAL_SECONDS = 2.0
DEFAULT_CLOSE_TIMEOUT_SECONDS = 5.0


class RabbitMqConnectionError(RuntimeError):
    """RabbitMQ connection or channel operation failed."""


class RabbitMqSerializationError(RuntimeError):
    """RabbitMQ message serialization or deserialization failed."""


@dataclass(frozen=True)
class RabbitMqProperties:
    """RabbitMQ connection properties."""

    host: str
    port: int
    user: str
    password: str
    max_inbound_message_body_size: int = DEFAULT_MAX_INBOUND_MESSAGE_BODY_SIZE
    connection_timeout_seconds: float = DEFAULT_CONNECTION_TIMEOUT_SECONDS


class RabbitMqAcknowledger(Acknowledger):
    """Thread-safe RabbitMQ message acknowledger.

    Pika channels are not thread-safe. The only thread-safe operation exposed by
    BlockingConnection is add_callback_threadsafe(...). For this reason ack/nack
    are always scheduled on the RabbitMQ connection thread.

    This is important for the Worker Runtime: a submitted job message may remain
    unacknowledged while a supervised child process is running, and the final ack
    may happen later from a watcher/supervisor thread.
    """

    def __init__(
        self,
        connection: pika.BlockingConnection,
        channel: BlockingChannel,
        delivery_tag: int,
        queue_name: str,
    ) -> None:
        self._connection = connection
        self._channel = channel
        self._delivery_tag = delivery_tag
        self._queue_name = queue_name
        self._completed = threading.Event()

    def ack(self) -> None:
        """Acknowledge that the message was processed successfully."""
        self._schedule_once(self._ack_on_connection_thread)

    def nack(self, requeue: bool) -> None:
        """Reject the message."""
        self._schedule_once(lambda: self._nack_on_connection_thread(requeue))

    def _schedule_once(self, action: Callable[[], None]) -> None:
        if self._completed.is_set():
            logger.debug(
                "Ignoring duplicate ack/nack for delivery tag '%s' from queue '%s'.",
                self._delivery_tag,
                self._queue_name,
            )
            return

        self._completed.set()

        try:
            if self._connection.is_open:
                self._connection.add_callback_threadsafe(action)
            else:
                logger.warning(
                    "Cannot ack/nack delivery tag '%s' from queue '%s': "
                    "RabbitMQ connection is already closed.",
                    self._delivery_tag,
                    self._queue_name,
                )
        except Exception as exc:
            raise RabbitMqConnectionError(
                f"Failed to schedule ack/nack for queue '{self._queue_name}'."
            ) from exc

    def _ack_on_connection_thread(self) -> None:
        try:
            if self._channel.is_open:
                self._channel.basic_ack(self._delivery_tag, multiple=False)
                logger.debug(
                    "Acknowledged RabbitMQ message from queue '%s', delivery tag '%s'.",
                    self._queue_name,
                    self._delivery_tag,
                )
        except Exception:
            logger.exception(
                "Failed to acknowledge RabbitMQ message from queue '%s', "
                "delivery tag '%s'.",
                self._queue_name,
                self._delivery_tag,
            )

    def _nack_on_connection_thread(self, requeue: bool) -> None:
        try:
            if self._channel.is_open:
                self._channel.basic_nack(
                    self._delivery_tag,
                    multiple=False,
                    requeue=requeue,
                )
                logger.debug(
                    "Rejected RabbitMQ message from queue '%s', "
                    "delivery tag '%s', requeue=%s.",
                    self._queue_name,
                    self._delivery_tag,
                    requeue,
                )
        except Exception:
            logger.exception(
                "Failed to reject RabbitMQ message from queue '%s', "
                "delivery tag '%s'.",
                self._queue_name,
                self._delivery_tag,
            )


class RabbitMqSubscription(Subscription):
    """Active RabbitMQ subscription running on a dedicated consumer thread."""

    def __init__(
        self,
        properties: RabbitMqProperties,
        queue_name: str,
        payload_type: type[T],
        handler: MessageHandler[T],
        clock: Callable[[], datetime],
        requeue_on_handler_error: bool,
        prefetch_count: int,
    ) -> None:
        if properties is None:
            raise ValueError("properties cannot be null.")
        if queue_name is None or queue_name.strip() == "":
            raise ValueError("queue_name cannot be null or blank.")
        if payload_type is None:
            raise ValueError("payload_type cannot be null.")
        if handler is None:
            raise ValueError("handler cannot be null.")
        if clock is None:
            raise ValueError("clock cannot be null.")
        if prefetch_count <= 0:
            raise ValueError("prefetch_count must be greater than zero.")

        self._properties = properties
        self._queue_name = queue_name
        self._payload_type = payload_type
        self._handler = handler
        self._clock = clock
        self._requeue_on_handler_error = requeue_on_handler_error
        self._prefetch_count = prefetch_count

        self._closed = threading.Event()
        self._ready = threading.Event()
        self._failed: BaseException | None = None

        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        self._consumer_tag: str | None = None
        self._lock = threading.Lock()

        self._thread = threading.Thread(
            target=self._consume_loop,
            name=f"rabbitmq-consumer-{queue_name}",
            daemon=True,
        )
        self._thread.start()

        if not self._ready.wait(timeout=properties.connection_timeout_seconds):
            self.close()
            raise RabbitMqConnectionError(
                f"Timed out waiting for RabbitMQ subscription to queue '{queue_name}'."
            )

        if self._failed is not None:
            self.close()
            raise RabbitMqConnectionError(
                f"Failed to subscribe to RabbitMQ queue '{queue_name}'."
            ) from self._failed

    def close(self) -> None:
        """Stop consuming messages and release subscription resources."""
        if self._closed.is_set():
            return

        self._closed.set()

        with self._lock:
            connection = self._connection
            channel = self._channel
            consumer_tag = self._consumer_tag

        if connection is not None and connection.is_open:

            def stop_consuming() -> None:
                try:
                    if (
                        channel is not None
                        and channel.is_open
                        and consumer_tag is not None
                    ):
                        channel.basic_cancel(consumer_tag)
                except Exception:
                    logger.exception(
                        "Failed to cancel RabbitMQ consumer for queue '%s'.",
                        self._queue_name,
                    )

            try:
                connection.add_callback_threadsafe(stop_consuming)
            except Exception:
                logger.exception(
                    "Failed to schedule RabbitMQ consumer cancellation for queue '%s'.",
                    self._queue_name,
                )

        self._thread.join(timeout=DEFAULT_CLOSE_TIMEOUT_SECONDS)

        if self._thread.is_alive():
            logger.warning(
                "RabbitMQ consumer thread for queue '%s' did not stop within %.1f seconds.",
                self._queue_name,
                DEFAULT_CLOSE_TIMEOUT_SECONDS,
            )

    def _consume_loop(self) -> None:
        try:
            connection = _create_connection_with_retry(self._properties)
            channel = connection.channel()

            with self._lock:
                self._connection = connection
                self._channel = channel

            _declare_queue(channel, self._queue_name)
            channel.basic_qos(prefetch_count=self._prefetch_count)

            consumer_tag = channel.basic_consume(
                queue=self._queue_name,
                auto_ack=False,
                on_message_callback=self._on_message,
            )

            with self._lock:
                self._consumer_tag = consumer_tag

            logger.info(
                "Subscribed to RabbitMQ queue '%s' with consumer tag '%s'.",
                self._queue_name,
                consumer_tag,
            )
            self._ready.set()

            while not self._closed.is_set() and connection.is_open:
                connection.process_data_events(time_limit=1.0)

        except BaseException as exc:
            self._failed = exc
            self._ready.set()
            logger.exception(
                "RabbitMQ subscription loop failed for queue '%s'.",
                self._queue_name,
            )
        finally:
            self._close_consumer_resources()

    def _on_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        acknowledger = RabbitMqAcknowledger(
            connection=self._require_connection(),
            channel=channel,
            delivery_tag=method.delivery_tag,
            queue_name=self._queue_name,
        )

        try:
            payload = _deserialize_payload(body, self._payload_type)
            headers = dict(properties.headers or {})
            message = QueueMessage(
                payload=payload,
                headers=headers,
                timestamp=self._clock(),
            )
            self._handler.handle(message, acknowledger)
        except Exception:
            logger.exception(
                "Failed to process RabbitMQ message from queue '%s'.",
                self._queue_name,
            )
            acknowledger.nack(requeue=self._requeue_on_handler_error)

    def _require_connection(self) -> pika.BlockingConnection:
        with self._lock:
            connection = self._connection

        if connection is None:
            raise RabbitMqConnectionError(
                f"RabbitMQ connection for queue '{self._queue_name}' is not initialized."
            )

        return connection

    def _close_consumer_resources(self) -> None:
        with self._lock:
            channel = self._channel
            connection = self._connection
            self._channel = None
            self._connection = None
            self._consumer_tag = None

        try:
            if channel is not None and channel.is_open:
                channel.close()
                logger.info(
                    "Closed RabbitMQ consumer channel for queue '%s'.", self._queue_name
                )
        except Exception:
            logger.warning(
                "Failed to close RabbitMQ consumer channel for queue '%s'.",
                self._queue_name,
                exc_info=True,
            )

        try:
            if connection is not None and connection.is_open:
                connection.close()
                logger.info(
                    "Closed RabbitMQ consumer connection for queue '%s'.",
                    self._queue_name,
                )
        except Exception:
            logger.warning(
                "Failed to close RabbitMQ consumer connection for queue '%s'.",
                self._queue_name,
                exc_info=True,
            )


class RabbitMqQueueClient(QueueClient):
    """RabbitMQ implementation of QueueClient.

    The client uses one publisher connection/channel and creates a dedicated
    connection/channel for each subscription. This avoids cross-thread usage of
    Pika channels and allows acknowledgements to be performed later from worker
    supervisor/watcher threads.
    """

    def __init__(
        self,
        properties: RabbitMqProperties,
        clock: Callable[[], datetime] | None = None,
        requeue_on_handler_error: bool = True,
        prefetch_count: int = 1,
    ) -> None:
        if properties is None:
            raise ValueError("properties cannot be null.")
        if prefetch_count <= 0:
            raise ValueError("prefetch_count must be greater than zero.")

        self._properties = properties
        self._clock = clock or _utc_now
        self._requeue_on_handler_error = requeue_on_handler_error
        self._prefetch_count = prefetch_count

        self._connection = _create_connection_with_retry(properties)
        self._channel = self._connection.channel()
        self._subscriptions: list[RabbitMqSubscription] = []
        self._closed = False
        self._lock = threading.RLock()

        logger.info(
            "Created RabbitMQ queue client for %s:%s.",
            properties.host,
            properties.port,
        )

    @classmethod
    def from_connection_properties(
        cls,
        host: str,
        port: int,
        user: str,
        password: str,
        max_inbound_message_body_size: int = DEFAULT_MAX_INBOUND_MESSAGE_BODY_SIZE,
        connection_timeout_seconds: float = DEFAULT_CONNECTION_TIMEOUT_SECONDS,
        clock: Callable[[], datetime] | None = None,
        requeue_on_handler_error: bool = True,
        prefetch_count: int = 1,
    ) -> RabbitMqQueueClient:
        """Create RabbitMQ client from primitive connection properties."""
        return cls(
            RabbitMqProperties(
                host=host,
                port=port,
                user=user,
                password=password,
                max_inbound_message_body_size=max_inbound_message_body_size,
                connection_timeout_seconds=connection_timeout_seconds,
            ),
            clock=clock,
            requeue_on_handler_error=requeue_on_handler_error,
            prefetch_count=prefetch_count,
        )

    def publish(self, queue_name: str, message: QueueMessage[T]) -> None:
        """Publish a message to a RabbitMQ queue."""
        if queue_name is None or queue_name.strip() == "":
            raise ValueError("queue_name cannot be null or blank.")
        if message is None:
            raise ValueError("message cannot be null.")

        with self._lock:
            self._raise_if_closed()
            _declare_queue(self._channel, queue_name)

            try:
                self._channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=_serialize_payload(message.payload),
                    properties=pika.BasicProperties(headers=dict(message.headers)),
                )
                logger.debug("Published RabbitMQ message to queue '%s'.", queue_name)
            except Exception as exc:
                raise RabbitMqConnectionError(
                    f"Failed to publish RabbitMQ message to queue '{queue_name}'."
                ) from exc

    def subscribe(
        self,
        queue_name: str,
        payload_type: type[T],
        handler: MessageHandler[T],
    ) -> Subscription:
        """Subscribe to a RabbitMQ queue and process messages."""
        with self._lock:
            self._raise_if_closed()

            subscription = RabbitMqSubscription(
                properties=self._properties,
                queue_name=queue_name,
                payload_type=payload_type,
                handler=handler,
                clock=self._clock,
                requeue_on_handler_error=self._requeue_on_handler_error,
                prefetch_count=self._prefetch_count,
            )
            self._subscriptions.append(subscription)
            return subscription

    def delete_queue(self, queue_name: str) -> None:
        """Delete RabbitMQ queue."""
        if queue_name is None or queue_name.strip() == "":
            raise ValueError("queue_name cannot be null or blank.")

        with self._lock:
            self._raise_if_closed()

            try:
                self._channel.queue_delete(queue=queue_name)
                logger.info("Deleted RabbitMQ queue '%s'.", queue_name)
            except Exception as exc:
                raise RabbitMqConnectionError(
                    f"Failed to delete RabbitMQ queue '{queue_name}'."
                ) from exc

    def close(self) -> None:
        """Close queue client and release RabbitMQ resources."""
        with self._lock:
            if self._closed:
                return

            self._closed = True
            subscriptions = list(self._subscriptions)
            self._subscriptions.clear()

        for subscription in subscriptions:
            subscription.close()

        with self._lock:
            try:
                if self._channel.is_open:
                    self._channel.close()
                    logger.info("Closed RabbitMQ publisher channel.")
            except Exception:
                logger.warning(
                    "Failed to close RabbitMQ publisher channel.", exc_info=True
                )

            try:
                if self._connection.is_open:
                    self._connection.close()
                    logger.info("Closed RabbitMQ publisher connection.")
            except Exception:
                logger.warning(
                    "Failed to close RabbitMQ publisher connection.", exc_info=True
                )

    def check_readiness(self) -> None:
        """Check that RabbitMQ connection and channel are ready for startup."""
        logger.info(
            "Checking RabbitMQ connection readiness.",
            extra={
                "component": "rabbitmq_queue_client",
                "event": "rabbitmq_readiness_check_started",
                "host": self._properties.host,
                "port": self._properties.port,
            },
        )

        with self._lock:
            try:
                self._raise_if_closed()

                if not self._connection.is_open:
                    raise RabbitMqConnectionError(
                        "RabbitMQ readiness check failed: connection is not open."
                    )

                if not self._channel.is_open:
                    raise RabbitMqConnectionError(
                        "RabbitMQ readiness check failed: channel is not open."
                    )

                self._connection.process_data_events(time_limit=0)

            except RabbitMqConnectionError:
                logger.exception(
                    "RabbitMQ connection readiness check failed.",
                    extra={
                        "component": "rabbitmq_queue_client",
                        "event": "rabbitmq_readiness_check_failed",
                        "host": self._properties.host,
                        "port": self._properties.port,
                    },
                )
                raise
            except Exception as exc:
                logger.exception(
                    "RabbitMQ connection readiness check failed.",
                    extra={
                        "component": "rabbitmq_queue_client",
                        "event": "rabbitmq_readiness_check_failed",
                        "host": self._properties.host,
                        "port": self._properties.port,
                    },
                )
                raise RabbitMqConnectionError(
                    "RabbitMQ connection readiness check failed."
                ) from exc

        logger.info(
            "RabbitMQ connection readiness check completed.",
            extra={
                "component": "rabbitmq_queue_client",
                "event": "rabbitmq_readiness_check_completed",
                "host": self._properties.host,
                "port": self._properties.port,
            },
        )

    def check_messaging_readiness(
        self,
        timeout_seconds: float | None = None,
    ) -> None:
        """Check RabbitMQ publish, consume, ack, subscription close, and delete."""
        resolved_timeout_seconds = (
            self._properties.connection_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )

        if resolved_timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero.")

        probe_queue_name = f"mdds.rabbitmq.readiness.{uuid.uuid4().hex}"
        probe_payload = {"nonce": uuid.uuid4().hex}

        handler = RabbitMqReadinessProbeHandler(probe_payload)
        subscription: Subscription | None = None
        primary_error: BaseException | None = None
        cleanup_error: BaseException | None

        logger.info(
            "Checking RabbitMQ messaging readiness.",
            extra={
                "component": "rabbitmq_queue_client",
                "event": "rabbitmq_messaging_readiness_check_started",
                "host": self._properties.host,
                "port": self._properties.port,
                "queueName": probe_queue_name,
            },
        )

        try:
            subscription = self.subscribe(probe_queue_name, dict, handler)
            self.publish(probe_queue_name, QueueMessage(payload=probe_payload))

            if not handler.completed.wait(timeout=resolved_timeout_seconds):
                raise RabbitMqConnectionError(
                    "RabbitMQ messaging readiness check failed: "
                    f"timed out waiting for probe message from queue "
                    f"'{probe_queue_name}'."
                )

            if handler.error is not None:
                raise handler.error

        except Exception as exc:
            primary_error = exc
        finally:
            cleanup_error = self._cleanup_messaging_readiness_probe(
                subscription,
                probe_queue_name,
            )

        if primary_error is not None:
            logger.error(
                "RabbitMQ messaging readiness check failed.",
                extra={
                    "component": "rabbitmq_queue_client",
                    "event": "rabbitmq_messaging_readiness_check_failed",
                    "host": self._properties.host,
                    "port": self._properties.port,
                    "queueName": probe_queue_name,
                },
                exc_info=(
                    type(primary_error),
                    primary_error,
                    primary_error.__traceback__,
                ),
            )

            if cleanup_error is not None:
                logger.error(
                    "RabbitMQ messaging readiness cleanup failed after primary "
                    "readiness failure.",
                    extra={
                        "component": "rabbitmq_queue_client",
                        "event": "rabbitmq_messaging_readiness_cleanup_failed",
                        "host": self._properties.host,
                        "port": self._properties.port,
                        "queueName": probe_queue_name,
                    },
                    exc_info=(
                        type(cleanup_error),
                        cleanup_error,
                        cleanup_error.__traceback__,
                    ),
                )

            if _is_messaging_readiness_error(primary_error):
                raise primary_error

            raise RabbitMqConnectionError(
                "RabbitMQ messaging readiness check failed."
            ) from primary_error

        if cleanup_error is not None:
            raise RabbitMqConnectionError(
                "RabbitMQ messaging readiness cleanup failed."
            ) from cleanup_error

        logger.info(
            "RabbitMQ messaging readiness check completed.",
            extra={
                "component": "rabbitmq_queue_client",
                "event": "rabbitmq_messaging_readiness_check_completed",
                "host": self._properties.host,
                "port": self._properties.port,
                "queueName": probe_queue_name,
            },
        )

    def _cleanup_messaging_readiness_probe(
        self,
        subscription: Subscription | None,
        probe_queue_name: str,
    ) -> BaseException | None:
        cleanup_error: BaseException | None = None

        if subscription is not None:
            try:
                subscription.close()
            except Exception as exc:
                cleanup_error = exc
                logger.exception(
                    "Failed to close RabbitMQ readiness probe subscription.",
                    extra={
                        "component": "rabbitmq_queue_client",
                        "event": "rabbitmq_messaging_readiness_subscription_close_failed",
                        "host": self._properties.host,
                        "port": self._properties.port,
                        "queueName": probe_queue_name,
                    },
                )

        try:
            self.delete_queue(probe_queue_name)
        except Exception as exc:
            if cleanup_error is None:
                cleanup_error = exc

            logger.exception(
                "Failed to delete RabbitMQ readiness probe queue.",
                extra={
                    "component": "rabbitmq_queue_client",
                    "event": "rabbitmq_messaging_readiness_queue_delete_failed",
                    "host": self._properties.host,
                    "port": self._properties.port,
                    "queueName": probe_queue_name,
                },
            )

        return cleanup_error

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise RabbitMqConnectionError("RabbitMQ queue client is already closed.")

    def __enter__(self) -> RabbitMqQueueClient:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


def _create_connection_with_retry(
    properties: RabbitMqProperties,
) -> pika.BlockingConnection:
    deadline = time.monotonic() + properties.connection_timeout_seconds
    last_error: BaseException | None = None

    while time.monotonic() <= deadline:
        try:
            logger.info(
                "Connecting to RabbitMQ at %s:%s...",
                properties.host,
                properties.port,
            )
            connection = pika.BlockingConnection(_connection_parameters(properties))
            if connection.is_open:
                logger.info(
                    "Connected to RabbitMQ at %s:%s.",
                    properties.host,
                    properties.port,
                )
                return connection
        except Exception as exc:
            last_error = exc
            logger.warning(
                "RabbitMQ is not ready at %s:%s, retrying...",
                properties.host,
                properties.port,
            )

        time.sleep(DEFAULT_RETRY_INTERVAL_SECONDS)

    raise RabbitMqConnectionError(
        f"Failed to connect to RabbitMQ at {properties.host}:{properties.port}."
    ) from last_error


def _connection_parameters(properties: RabbitMqProperties) -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(properties.user, properties.password)

    return pika.ConnectionParameters(
        host=properties.host,
        port=properties.port,
        credentials=credentials,
        frame_max=properties.max_inbound_message_body_size,
        heartbeat=60,
        blocked_connection_timeout=properties.connection_timeout_seconds,
        connection_attempts=1,
    )


def _declare_queue(channel: BlockingChannel, queue_name: str) -> None:
    try:
        channel.queue_declare(
            queue=queue_name,
            durable=False,
            exclusive=False,
            auto_delete=False,
        )
    except Exception as exc:
        raise RabbitMqConnectionError(
            f"Failed to declare RabbitMQ queue '{queue_name}'."
        ) from exc


def _serialize_payload(payload: Any) -> bytes:
    try:
        return json.dumps(
            _to_json_compatible(payload),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except Exception as exc:
        raise RabbitMqSerializationError(
            "Failed to serialize RabbitMQ payload."
        ) from exc


def _deserialize_payload(body: bytes, payload_type: type[T]) -> T:
    try:
        raw = json.loads(body.decode("utf-8"))
        return _coerce_payload(raw, payload_type)
    except Exception as exc:
        raise RabbitMqSerializationError(
            f"Failed to deserialize RabbitMQ payload as {payload_type}."
        ) from exc


def _coerce_payload(raw: Any, payload_type: type[T]) -> T:
    if payload_type is Any:
        return raw

    origin = get_origin(payload_type)
    if origin is not None:
        return raw

    if payload_type is dict:
        if not isinstance(raw, dict):
            raise TypeError(f"Expected JSON object, got {type(raw).__name__}.")
        return raw  # type: ignore[return-value]

    if payload_type is str:
        if not isinstance(raw, str):
            raise TypeError(f"Expected JSON string, got {type(raw).__name__}.")
        return raw  # type: ignore[return-value]

    if dataclasses.is_dataclass(payload_type):
        if not isinstance(raw, dict):
            raise TypeError(
                f"Expected JSON object for dataclass {payload_type.__name__}, "
                f"got {type(raw).__name__}."
            )
        return payload_type(**raw)

    if hasattr(payload_type, "from_dict"):
        return payload_type.from_dict(raw)

    if isinstance(raw, payload_type):
        return raw

    if isinstance(raw, dict):
        return payload_type(**raw)

    return payload_type(raw)


def _to_json_compatible(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return _to_json_compatible(dataclasses.asdict(value))

    if isinstance(value, datetime):
        return _format_datetime(value)

    if isinstance(value, enum.Enum):
        return value.value

    if isinstance(value, Mapping):
        return {str(key): _to_json_compatible(item) for key, item in value.items()}

    if isinstance(value, list | tuple | set):
        return [_to_json_compatible(item) for item in value]

    return value


def _format_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_messaging_readiness_error(error: BaseException) -> bool:
    return isinstance(error, RabbitMqConnectionError) and str(error).startswith(
        "RabbitMQ messaging readiness check failed"
    )


class RabbitMqReadinessProbeHandler(MessageHandler[dict[str, str]]):
    """Technical handler used only by RabbitMQ startup messaging readiness check."""

    def __init__(self, expected_payload: dict[str, str]) -> None:
        self._expected_payload = expected_payload
        self.completed = threading.Event()
        self.error: BaseException | None = None

    def handle(
        self,
        message: QueueMessage[dict[str, str]],
        ack: Acknowledger,
    ) -> None:
        """Validate probe payload and acknowledge the readiness probe message."""
        try:
            if message.payload != self._expected_payload:
                self.error = RabbitMqConnectionError(
                    "RabbitMQ messaging readiness check failed: "
                    "unexpected probe payload."
                )

            ack.ack()
        except Exception as exc:
            error = RabbitMqConnectionError(
                "RabbitMQ messaging readiness check failed: "
                "failed to acknowledge probe message."
            )
            error.__cause__ = exc
            self.error = error
        finally:
            self.completed.set()
