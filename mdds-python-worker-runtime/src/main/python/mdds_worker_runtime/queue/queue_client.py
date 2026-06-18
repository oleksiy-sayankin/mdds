# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Generic, Mapping, Protocol, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class QueueMessage(Generic[T]):
    """Represents common message for all types of queues.

    Contains message payload and metadata.
    """

    def __post_init__(self) -> None:
        if self.payload is None:
            raise ValueError("payload cannot be null.")
        if self.headers is None:
            raise ValueError("headers cannot be null.")
        if self.timestamp is None:
            raise ValueError("timestamp cannot be null.")

    payload: T
    headers: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class Acknowledger(Protocol):
    """Informs the queue that a message was processed or rejected."""

    def ack(self) -> None:
        """Acknowledge that the message was processed successfully."""

    def nack(self, requeue: bool) -> None:
        """Reject the message."""


class MessageHandler(Protocol[T]):
    """Provides callback for handling queue messages."""

    def handle(self, message: QueueMessage[T], ack: Acknowledger) -> None:
        """Process message obtained from the queue."""


class Subscription(Protocol):
    """Active queue subscription handle."""

    def close(self) -> None:
        """Stop consuming messages and release subscription resources."""


class QueueClient(Protocol):
    """Common interface for publishing to and consuming from named queues."""

    def publish(self, queue_name: str, message: QueueMessage[T]) -> None:
        """Publish a message to a queue."""

    def subscribe(
        self,
        queue_name: str,
        payload_type: type[T],
        handler: MessageHandler[T],
    ) -> Subscription:
        """Subscribe to a queue and process messages."""

    def delete_queue(self, queue_name: str) -> None:
        """Delete queue."""

    def close(self) -> None:
        """Close queue connection and release resources."""
