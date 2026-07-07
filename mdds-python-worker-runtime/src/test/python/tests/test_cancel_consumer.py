# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
from typing import cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.dto.messages import CancelJobDTO
from mdds_worker_runtime.execution.cancel_consumer import CancelConsumer
from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)
from mdds_worker_runtime.queue.queue_client import Acknowledger, QueueMessage

WORKER_ID = "worker-1"
JOB_ID = "job-1"


def test_cancel_consumer_rejects_null_job_state_transition_coordinator() -> None:
    with pytest.raises(
        ValueError,
        match="job_state_transition_coordinator cannot be null.",
    ):
        CancelConsumer(
            job_state_transition_coordinator=cast(
                JobStateTransitionCoordinator, cast(object, None)
            ),
            cancellation_request_handler=MagicMock(spec=CancellationRequestHandler),
            worker_id=WORKER_ID,
        )


def test_cancel_consumer_rejects_null_handler() -> None:
    with pytest.raises(
        ValueError,
        match="cancellation_request_handler cannot be null.",
    ):
        CancelConsumer(
            job_state_transition_coordinator=JobStateTransitionCoordinator(),
            cancellation_request_handler=cast(
                CancellationRequestHandler, cast(object, None)
            ),
            worker_id=WORKER_ID,
        )


@pytest.mark.parametrize("worker_id", [None, "", " "])
def test_cancel_consumer_rejects_blank_worker_id(worker_id: str | None) -> None:
    with pytest.raises(ValueError, match="worker_id cannot be null or blank."):
        CancelConsumer(
            job_state_transition_coordinator=JobStateTransitionCoordinator(),
            cancellation_request_handler=MagicMock(spec=CancellationRequestHandler),
            worker_id=cast(str, worker_id),
        )


def test_cancel_consumer_rejects_null_ack() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message(JOB_ID)

    with pytest.raises(ValueError, match="ack cannot be null."):
        consumer.handle(
            message,
            cast(Acknowledger, cast(object, None)),
        )

    handler.finalize_cancellation.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_cancel_consumer_rejects_null_message() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    with pytest.raises(ValueError, match="message cannot be null."):
        consumer.handle(
            cast(QueueMessage[CancelJobDTO], cast(object, None)),
            ack,
        )

    ack_mock.ack.assert_not_called()
    handler.finalize_cancellation.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_cancel_consumer_rejects_null_payload() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _QueueMessageStub(payload=None)

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    with pytest.raises(ValueError, match="cancel message payload cannot be null."):
        consumer.handle(message, ack)

    ack_mock.ack.assert_not_called()
    handler.finalize_cancellation.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


@pytest.mark.parametrize("job_id", [None, "", " "])
def test_cancel_consumer_rejects_blank_job_id(job_id: str | None) -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message(cast(str, job_id))
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    with pytest.raises(ValueError, match="cancel jobId cannot be null or blank."):
        consumer.handle(message, ack)

    ack_mock.ack.assert_not_called()
    handler.finalize_cancellation.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_cancel_consumer_accepts_request_acknowledges_and_finalizes_cancelled() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message(JOB_ID)
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    consumer.handle(message, ack)

    handler.finalize_cancellation.assert_called_once_with(job_id=JOB_ID)
    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.CANCELLED


def test_cancel_consumer_trims_job_id() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message("  job-1  ")
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    consumer.handle(message, ack)

    handler.finalize_cancellation.assert_called_once_with(job_id=JOB_ID)
    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.CANCELLED


def test_cancel_consumer_acknowledges_stale_cancellation_without_finalizing() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.DONE)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message(JOB_ID)
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    consumer.handle(message, ack)

    handler.finalize_cancellation.assert_not_called()
    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE


def test_cancel_consumer_does_not_ack_when_handler_fails() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes(
        finalization_error=RuntimeError("handler failed"),
    )
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message(JOB_ID)
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    with pytest.raises(RuntimeError, match="handler failed"):
        consumer.handle(message, ack)

    handler.finalize_cancellation.assert_called_once_with(job_id=JOB_ID)
    ack_mock.ack.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_cancel_consumer_skips_finalization_when_state_is_not_in_progress() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.INPUTS_PREPARED)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message(JOB_ID)
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    consumer.handle(message, ack)

    handler.finalize_cancellation.assert_not_called()
    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.INPUTS_PREPARED


def test_cancel_consumer_finalizes_cancelled_and_commits_state() -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    handler = _handler_that_finalizes()
    consumer = CancelConsumer(coordinator, handler, WORKER_ID)
    message = _message(JOB_ID)
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    consumer.handle(message, ack)

    handler.finalize_cancellation.assert_called_once_with(job_id=JOB_ID)
    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.CANCELLED


def _handler_that_finalizes(
    *,
    events: list[str] | None = None,
    finalization_error: Exception | None = None,
) -> MagicMock:
    handler = MagicMock(spec=CancellationRequestHandler)

    def finalize_cancellation(*, job_id: str) -> None:
        if events is not None:
            events.append("finalize_cancellation")

        if finalization_error is not None:
            raise finalization_error

    handler.finalize_cancellation.side_effect = finalize_cancellation
    return handler


def _coordinator_in_state(
    state: WorkerJobStatus = WorkerJobStatus.SUBMITTED,
    job_id: str = JOB_ID,
) -> JobStateTransitionCoordinator:
    coordinator = JobStateTransitionCoordinator()
    acknowledger = cast(Acknowledger, cast(object, MagicMock(spec=Acknowledger)))
    record = coordinator.create(job_id=job_id, submitted_ack=acknowledger)
    with record.lock:
        record.state = state
    return coordinator


def _message(job_id: str) -> QueueMessage[CancelJobDTO]:
    return QueueMessage(
        payload=CancelJobDTO(jobId=job_id),
        headers={},
        timestamp=datetime.now(timezone.utc),
    )


class _QueueMessageStub:
    def __init__(self, payload: CancelJobDTO | None) -> None:
        self.payload = payload
