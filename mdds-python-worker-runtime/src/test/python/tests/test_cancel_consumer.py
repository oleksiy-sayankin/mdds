# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.dto.messages import CancelJobDTO
from mdds_worker_runtime.execution.cancel_consumer import CancelConsumer
from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.queue.queue_client import Acknowledger, QueueMessage

WORKER_ID = "worker-1"


def test_cancel_job_dto_exposes_job_id_from_job_id_payload_field() -> None:
    dto = CancelJobDTO(jobId="job-1")

    assert dto.jobId == "job-1"
    assert dto.job_id == "job-1"


def test_cancel_consumer_rejects_null_handler() -> None:
    with pytest.raises(
        ValueError,
        match="cancellation_request_handler cannot be null.",
    ):
        CancelConsumer(
            cancellation_request_handler=cast(CancellationRequestHandler, None),
            worker_id=WORKER_ID,
        )


@pytest.mark.parametrize("worker_id", [None, "", " "])
def test_cancel_consumer_rejects_blank_worker_id(worker_id: str | None) -> None:
    with pytest.raises(ValueError, match="worker_id cannot be null or blank."):
        CancelConsumer(
            cancellation_request_handler=MagicMock(spec=CancellationRequestHandler),
            worker_id=cast(str, worker_id),
        )


def test_cancel_consumer_rejects_null_ack() -> None:
    handler = MagicMock(spec=CancellationRequestHandler)
    consumer = CancelConsumer(handler, WORKER_ID)
    message = _message("job-1")

    with pytest.raises(ValueError, match="ack cannot be null."):
        consumer.handle(
            message,
            cast(Acknowledger, None),
        )

    handler.request_cancellation.assert_not_called()


def test_cancel_consumer_rejects_null_message() -> None:
    handler = MagicMock(spec=CancellationRequestHandler)
    consumer = CancelConsumer(handler, WORKER_ID)
    ack = MagicMock(spec=Acknowledger)

    with pytest.raises(ValueError, match="message cannot be null."):
        consumer.handle(
            cast(QueueMessage[CancelJobDTO], None),
            ack,
        )

    handler.request_cancellation.assert_not_called()
    ack.ack.assert_not_called()


def test_cancel_consumer_rejects_null_payload() -> None:
    handler = MagicMock(spec=CancellationRequestHandler)
    consumer = CancelConsumer(handler, WORKER_ID)
    message = _QueueMessageStub(payload=None)
    ack = MagicMock(spec=Acknowledger)

    with pytest.raises(ValueError, match="cancel message payload cannot be null."):
        consumer.handle(cast(QueueMessage[CancelJobDTO], message), ack)

    handler.request_cancellation.assert_not_called()
    ack.ack.assert_not_called()


@pytest.mark.parametrize("job_id", [None, "", " "])
def test_cancel_consumer_rejects_blank_job_id(job_id: str | None) -> None:
    handler = MagicMock(spec=CancellationRequestHandler)
    consumer = CancelConsumer(handler, WORKER_ID)
    message = QueueMessage(
        payload=CancelJobDTO(jobId=cast(str, job_id)),
    )
    ack = MagicMock(spec=Acknowledger)

    with pytest.raises(ValueError, match="cancel jobId cannot be null or blank."):
        consumer.handle(message, ack)

    handler.request_cancellation.assert_not_called()
    ack.ack.assert_not_called()


def test_cancel_consumer_trims_job_id() -> None:
    handler = MagicMock(spec=CancellationRequestHandler)
    consumer = CancelConsumer(handler, WORKER_ID)
    message = _message("  job-1  ")
    ack = MagicMock(spec=Acknowledger)

    consumer.handle(message, ack)

    handler.request_cancellation.assert_called_once_with("job-1")
    ack.ack.assert_called_once_with()


def test_cancel_consumer_delegates_valid_job_id_to_handler() -> None:
    handler = MagicMock(spec=CancellationRequestHandler)
    consumer = CancelConsumer(handler, WORKER_ID)
    message = _message("job-1")
    ack = MagicMock(spec=Acknowledger)

    consumer.handle(message, ack)

    handler.request_cancellation.assert_called_once_with("job-1")


def test_cancel_consumer_acknowledges_cancellation_message_after_handler_accepts() -> (
    None
):
    events: list[str] = []
    handler = _CancellationRequestHandlerSpy(events)
    ack = _AcknowledgerSpy(events)
    consumer = CancelConsumer(handler, WORKER_ID)

    consumer.handle(_message("job-1"), ack)

    assert events == [
        "request_cancellation:job-1",
        "ack",
    ]


def test_cancel_consumer_does_not_ack_when_handler_raises() -> None:
    handler = MagicMock(spec=CancellationRequestHandler)
    handler.request_cancellation.side_effect = RuntimeError("handler failed")
    consumer = CancelConsumer(handler, WORKER_ID)
    message = _message("job-1")
    ack = MagicMock(spec=Acknowledger)

    with pytest.raises(RuntimeError, match="handler failed"):
        consumer.handle(message, ack)

    handler.request_cancellation.assert_called_once_with("job-1")
    ack.ack.assert_not_called()


def _message(job_id: str) -> QueueMessage[CancelJobDTO]:
    return QueueMessage(
        payload=CancelJobDTO(jobId=job_id),
    )


class _CancellationRequestHandlerSpy:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def request_cancellation(self, job_id: str) -> None:
        self._events.append(f"request_cancellation:{job_id}")


class _AcknowledgerSpy:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def ack(self) -> None:
        self._events.append("ack")

    def nack(self, requeue: bool) -> None:
        self._events.append(f"nack:{requeue}")


class _QueueMessageStub:
    def __init__(self, payload: CancelJobDTO | None) -> None:
        self.payload = payload
