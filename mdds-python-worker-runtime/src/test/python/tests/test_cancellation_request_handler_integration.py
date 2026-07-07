# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
import multiprocessing
from pathlib import Path
import time
from typing import Any, cast
from unittest.mock import MagicMock

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.dto.messages import CancelJobDTO, JobStatusUpdateDTO
from mdds_worker_runtime.execution.cancel_consumer import CancelConsumer
from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    WorkerJobStatus,
    ProcessRecord,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)
from mdds_worker_runtime.queue.queue_client import QueueMessage, Acknowledger

WORKER_ID = "worker-1"
USER_ID = 42
JOB_TYPE = "SOLVING_SLAE"
CANCELLED_JOB_ID = "job-cancelled"
OTHER_JOB_ID = "job-still-running"
FIXED_NOW = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_cancel_consumer_cancels_only_target_running_execution(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    queue_client = _QueueClientSpy()
    status_publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=queue_client,
        clock=lambda: FIXED_NOW,
    )

    coordinator = JobStateTransitionCoordinator()

    cancellation_request_handler = CancellationRequestHandler(
        execution_registry=registry,
        status_publisher=status_publisher,
        worker_id=WORKER_ID,
        cancelled_process_join_timeout_seconds=1.0,
        clock=lambda: FIXED_NOW,
    )

    cancel_consumer = CancelConsumer(
        job_state_transition_coordinator=coordinator,
        cancellation_request_handler=cancellation_request_handler,
        worker_id=WORKER_ID,
    )

    cancelled_process = _start_waiting_process()
    other_process = _start_waiting_process()

    cancelled_parent_connection, cancelled_child_connection = multiprocessing.Pipe()
    other_parent_connection, other_child_connection = multiprocessing.Pipe()

    cancelled_child_connection.close()
    other_child_connection.close()

    cancelled_ack = _SubmittedAckSpy()
    other_ack = _SubmittedAckSpy()

    cancelled_workspace = tmp_path / "jobs" / str(USER_ID) / CANCELLED_JOB_ID
    other_workspace = tmp_path / "jobs" / str(USER_ID) / OTHER_JOB_ID
    cancelled_workspace.mkdir(parents=True)
    other_workspace.mkdir(parents=True)

    cancelled_record = _record(
        job_id=CANCELLED_JOB_ID,
        workspace=cancelled_workspace,
        process=cancelled_process,
        parent_connection=cancelled_parent_connection,
    )
    other_record = _record(
        job_id=OTHER_JOB_ID,
        workspace=other_workspace,
        process=other_process,
        parent_connection=other_parent_connection,
    )

    registry.add(cancelled_record)
    registry.add(other_record)

    _seed_in_progress_state(
        coordinator,
        CANCELLED_JOB_ID,
        cancelled_ack,
    )
    _seed_in_progress_state(
        coordinator,
        OTHER_JOB_ID,
        other_ack,
    )

    cancellation_message_ack = _CancellationMessageAckSpy()

    try:
        cancel_consumer.handle(
            QueueMessage(payload=CancelJobDTO(jobId=CANCELLED_JOB_ID)),
            cancellation_message_ack,
        )

        assert cancellation_message_ack.ack_count == 1
        assert cancellation_message_ack.nack_calls == []

        assert not cancelled_process.is_alive()
        assert other_process.is_alive()

        assert registry.size() == 2
        assert registry.get(CANCELLED_JOB_ID) is cancelled_record
        assert registry.get(OTHER_JOB_ID) is other_record

        assert coordinator.get_state(CANCELLED_JOB_ID) is WorkerJobStatus.CANCELLED
        assert coordinator.get_state(OTHER_JOB_ID) is WorkerJobStatus.IN_PROGRESS

        assert cancelled_ack.ack_count == 1
        assert other_ack.ack_count == 0

        assert len(queue_client.published_messages) == 1
        queue_name, published_message = queue_client.published_messages[0]

        assert queue_name == "mdds_status_queue"
        assert isinstance(published_message, QueueMessage)

        payload = published_message.payload

        assert isinstance(payload, JobStatusUpdateDTO)
        assert payload.jobId == CANCELLED_JOB_ID
        assert payload.job_id == CANCELLED_JOB_ID
        assert payload.workerId == WORKER_ID
        assert payload.worker_id == WORKER_ID
        assert payload.status == WorkerJobStatus.CANCELLED.value
        assert payload.progress == 100
        assert "Job cancellation requested and applied" in payload.message
        assert payload.eventTime == "2026-01-01T00:00:00Z"
        assert payload.event_time == payload.eventTime

        assert cancelled_workspace.exists()
        assert cancelled_workspace.is_dir()
        assert other_workspace.exists()
        assert other_workspace.is_dir()
    finally:
        _terminate_if_alive(cancelled_process)
        _terminate_if_alive(other_process)
        _close_safely(cancelled_parent_connection)
        _close_safely(other_parent_connection)


def _seed_in_progress_state(
    job_state_transition_coordinator: JobStateTransitionCoordinator,
    job_id: str,
    submitted_ack: "_SubmittedAckSpy",
) -> None:
    record = job_state_transition_coordinator.create(
        job_id=job_id,
        submitted_ack=cast(Acknowledger, submitted_ack),
    )

    with record.lock:
        record.state = WorkerJobStatus.IN_PROGRESS


def _record(
    *,
    job_id: str,
    workspace: Path,
    process: multiprocessing.Process,
    parent_connection: Any,
) -> ExecutionRecord:
    manifest = JobManifest(
        manifest_version=1,
        user_id=USER_ID,
        job_id=job_id,
        job_type=JOB_TYPE,
        inputs={},
        params={},
        outputs={},
    )

    job_workspace = JobWorkspace(
        manifest=manifest,
        work_dir=workspace,
        input_dir=workspace / "in",
        output_dir=workspace / "out",
        worker_id=WORKER_ID,
    )

    context = MagicMock()
    context.workspace = job_workspace
    context.work_dir = job_workspace.work_dir
    context.input_dir = job_workspace.input_dir
    context.output_dir = job_workspace.output_dir
    context.user_id = job_workspace.user_id
    context.job_id = job_workspace.job_id
    context.job_type = job_workspace.job_type

    return ExecutionRecord(
        workspace=job_workspace,
        context=context,
        process_record=ProcessRecord(
            process=process,
            parent_connection=parent_connection,
            started_at=FIXED_NOW,
        ),
    )


def _start_waiting_process() -> multiprocessing.Process:
    started_event = multiprocessing.Event()
    process = multiprocessing.Process(
        target=_wait_until_terminated,
        args=(started_event,),
        daemon=False,
    )
    process.start()

    started = started_event.wait(timeout=2.0)

    assert started is True
    assert process.is_alive()

    return process


def _wait_until_terminated(started_event: Any) -> None:
    started_event.set()

    while True:
        time.sleep(0.1)


def _terminate_if_alive(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=1.0)

    if process.is_alive():
        process.kill()
        process.join(timeout=1.0)


def _close_safely(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass


class _QueueClientSpy:
    def __init__(self) -> None:
        self.published_messages: list[tuple[str, QueueMessage[Any]]] = []

    def publish(self, queue_name: str, message: QueueMessage[Any]) -> None:
        self.published_messages.append((queue_name, message))


class _SubmittedAckSpy:
    def __init__(self) -> None:
        self.ack_count = 0
        self.nack_calls: list[bool] = []

    def ack(self) -> None:
        self.ack_count += 1

    def nack(self, requeue: bool) -> None:
        self.nack_calls.append(requeue)


class _CancellationMessageAckSpy:
    def __init__(self) -> None:
        self.ack_count = 0
        self.nack_calls: list[bool] = []

    def ack(self) -> None:
        self.ack_count += 1

    def nack(self, requeue: bool) -> None:
        self.nack_calls.append(requeue)
