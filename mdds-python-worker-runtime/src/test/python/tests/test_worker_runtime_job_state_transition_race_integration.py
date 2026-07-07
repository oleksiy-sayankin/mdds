# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import threading
import time
from typing import Any, cast
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pika
import pytest
from testcontainers.minio import MinioContainer
from testcontainers.rabbitmq import RabbitMqContainer

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.dto.messages import JobStatusUpdateDTO
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.job_state import JobStateTransitionCoordinator
from mdds_worker_runtime.queue.queue_client import Acknowledger
from mdds_worker_runtime.rabbitmq.rabbitmq_queue_client import (
    RabbitMqProperties,
    RabbitMqQueueClient,
)

RABBITMQ_IMAGE = "rabbitmq:3.12-management"

# Keep the status collector from becoming the bottleneck.
#
# With prefetch_count=1, RabbitMQ will not deliver the next status message to
# this test consumer until the previous status message is acknowledged by the
# broker. Since RabbitMqAcknowledger may process the actual broker ack
# asynchronously, the test could falsely look blocked on status delivery rather
# than on the coordinator lock. These tests verify coordinator atomicity, so the
# status consumer must be able to receive more than one pending status message.
STATUS_PREFETCH_COUNT = 10

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
S3_BUCKET = "mdds-test"

WORKER_ID = "worker-transition-race-it"
JOB_TYPE = "two_numbers_sum"
USER_ID = 123
FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

WAIT_TIMEOUT_SECONDS = 15.0
SHORT_WAIT_SECONDS = 0.25

TERMINAL_STATUS_VALUES = {
    WorkerJobStatus.DONE.value,
    WorkerJobStatus.ERROR.value,
    WorkerJobStatus.CANCELLED.value,
}


class StatusCollectingHandler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._statuses: list[JobStatusUpdateDTO] = []

    def handle(self, message, ack) -> None:
        with self._lock:
            self._statuses.append(message.payload)
        ack.ack()

    def snapshot(self) -> list[JobStatusUpdateDTO]:
        with self._lock:
            return list(self._statuses)


class CountingAcknowledger:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0

    def ack(self) -> None:
        with self._lock:
            self._count += 1

    @property
    def count(self) -> int:
        with self._lock:
            return self._count


@dataclass
class _ThreadResult:
    value: Any = None
    error: BaseException | None = None


@pytest.fixture(scope="session")
def rabbitmq_container():
    with RabbitMqContainer(
        RABBITMQ_IMAGE,
        username="guest",
        password="guest",
    ) as container:
        yield container


@pytest.fixture(scope="session")
def minio_container():
    with MinioContainer(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
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


@pytest.fixture
def s3_client(minio_container):
    client = _create_s3_client(minio_container)
    _create_bucket_if_absent(client, S3_BUCKET)
    return client


def test_done_transition_blocks_concurrent_timeout_error_and_wins_atomically(
    rabbitmq_properties: RabbitMqProperties,
    s3_client,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # DONE transition blocks concurrent timeout ERROR and wins atomically.
    #
    # job state = IN_PROGRESS
    #
    # Thread A:
    #   starts DONE transition
    #   uploads output artifact to MinIO
    #   blocks before publishing DONE or during publish_done wrapper
    #
    # Thread B:
    #   tries ERROR transition as timeout watcher would do
    #
    # Expected:
    #   ERROR operation does not run while DONE transition owns per-job lock
    #   DONE status is published once to RabbitMQ
    #   output object exists in S3
    #   coordinator state is DONE
    #   submitted ack called once
    #   ERROR status is not published
    test_id = str(uuid4())
    job_id = f"job-{test_id}"
    status_queue_name = f"test.status.transition_race.done_wins.{test_id}"
    output_key = _output_key(job_id)

    _declare_queue(rabbitmq_properties, status_queue_name)

    status_handler = StatusCollectingHandler()

    try:
        with RabbitMqQueueClient(
            rabbitmq_properties,
            prefetch_count=STATUS_PREFETCH_COUNT,
        ) as status_client:
            status_subscription = status_client.subscribe(
                status_queue_name,
                JobStatusUpdateDTO,
                status_handler,
            )

            try:
                coordinator = JobStateTransitionCoordinator()
                submitted_ack = CountingAcknowledger()
                _create_in_progress_record(
                    coordinator=coordinator,
                    job_id=job_id,
                    submitted_ack=submitted_ack,
                )

                workspace = _workspace(tmp_path=tmp_path, job_id=job_id)

                done_operation_entered = threading.Event()
                release_done_operation = threading.Event()
                error_operation_entered = threading.Event()

                def done_operation() -> str:
                    _put_bytes(
                        s3_client=s3_client,
                        bucket=S3_BUCKET,
                        key=output_key,
                        body=b"42\n",
                    )
                    done_operation_entered.set()
                    _wait_for_event(
                        release_done_operation,
                        "release_done_operation",
                    )
                    _publish_done_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                    )
                    return "done-published"

                def error_operation() -> str:
                    error_operation_entered.set()
                    _publish_error_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                        message="Timeout watcher terminated the job.",
                    )
                    return "error-published"

                done_thread, done_result_holder = _start_thread(
                    "done-transition",
                    lambda: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.DONE,
                        operation=done_operation,
                    ),
                )

                _wait_for_event(done_operation_entered, "done_operation_entered")

                error_thread, error_result_holder = _start_thread(
                    "timeout-error-transition",
                    lambda: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.ERROR,
                        operation=error_operation,
                    ),
                )

                _wait_until(lambda: error_thread.is_alive())
                assert not error_operation_entered.wait(SHORT_WAIT_SECONDS)

                release_done_operation.set()

                done_result = _join_thread(done_thread, done_result_holder)
                error_result = _join_thread(error_thread, error_result_holder)

                _wait_until(
                    lambda: len(_terminal_statuses(status_handler, job_id)) == 1
                )

                terminal_statuses = _terminal_statuses(status_handler, job_id)

                assert done_result.committed
                assert not done_result.failed
                assert not done_result.stale

                assert error_result.stale
                assert not error_result.committed
                assert not error_result.failed
                assert error_result.current_state is WorkerJobStatus.DONE

                assert terminal_statuses[0].status == WorkerJobStatus.DONE.value
                assert coordinator.get_state(job_id) is WorkerJobStatus.DONE
                assert submitted_ack.count == 1

                assert _object_exists(
                    s3_client=s3_client,
                    bucket=S3_BUCKET,
                    key=output_key,
                )
                assert not error_operation_entered.is_set()

            finally:
                status_subscription.close()

    finally:
        _delete_queue_if_exists(rabbitmq_properties, status_queue_name)


def test_timeout_error_wins_first_and_prevents_late_done_side_effects(
    rabbitmq_properties: RabbitMqProperties,
    s3_client,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # Timeout ERROR wins first and prevents late DONE side effects.
    #
    # job state = IN_PROGRESS
    #
    # Thread A:
    #   starts ERROR transition
    #   publishes ERROR to RabbitMQ
    #   blocks before commit/ack or inside operation
    #
    # Thread B:
    #   tries DONE transition
    #
    # Expected:
    #   ERROR wins
    #   DONE operation does not upload output artifact
    #   RabbitMQ contains ERROR only
    #   coordinator state is ERROR
    #   submitted ack called once
    #   S3 output object is absent
    test_id = str(uuid4())
    job_id = f"job-{test_id}"
    status_queue_name = f"test.status.transition_race.error_wins.{test_id}"
    output_key = _output_key(job_id)

    _declare_queue(rabbitmq_properties, status_queue_name)

    status_handler = StatusCollectingHandler()

    try:
        with RabbitMqQueueClient(
            rabbitmq_properties,
            prefetch_count=STATUS_PREFETCH_COUNT,
        ) as status_client:
            status_subscription = status_client.subscribe(
                status_queue_name,
                JobStatusUpdateDTO,
                status_handler,
            )

            try:
                coordinator = JobStateTransitionCoordinator()
                submitted_ack = CountingAcknowledger()
                _create_in_progress_record(
                    coordinator=coordinator,
                    job_id=job_id,
                    submitted_ack=submitted_ack,
                )

                workspace = _workspace(tmp_path=tmp_path, job_id=job_id)

                error_status_published = threading.Event()
                release_error_operation = threading.Event()
                done_transition_started = threading.Event()
                done_operation_entered = threading.Event()

                def error_operation() -> str:
                    _publish_error_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                        message="Timeout watcher terminated the job.",
                    )
                    error_status_published.set()
                    _wait_for_event(
                        release_error_operation,
                        "release_error_operation",
                    )
                    return "error-published"

                def done_operation() -> str:
                    done_operation_entered.set()
                    _put_bytes(
                        s3_client=s3_client,
                        bucket=S3_BUCKET,
                        key=output_key,
                        body=b"42\n",
                    )
                    _publish_done_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                    )
                    return "done-published"

                error_thread, error_result_holder = _start_thread(
                    "timeout-error-transition",
                    lambda: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.ERROR,
                        operation=error_operation,
                    ),
                )

                _wait_for_event(error_status_published, "error_status_published")

                done_thread, done_result_holder = _start_thread(
                    "late-done-transition",
                    lambda: _run_started_transition(
                        started=done_transition_started,
                        transition=lambda: coordinator.transition(
                            job_id=job_id,
                            target_state=WorkerJobStatus.DONE,
                            operation=done_operation,
                        ),
                    ),
                )

                _wait_for_event(done_transition_started, "done_transition_started")

                # Do not call coordinator.get_state(...) here. The ERROR
                # transition intentionally holds the per-job lock while its
                # operation is blocked. Calling get_state() here may block the
                # test itself and accidentally let error_operation time out.
                release_error_operation.set()

                error_result = _join_thread(error_thread, error_result_holder)
                done_result = _join_thread(done_thread, done_result_holder)

                _wait_until(
                    lambda: len(_terminal_statuses(status_handler, job_id)) == 1
                )

                terminal_statuses = _terminal_statuses(status_handler, job_id)

                assert error_result.committed
                assert not error_result.failed
                assert not error_result.stale

                assert done_result.stale
                assert not done_result.committed
                assert not done_result.failed
                assert done_result.current_state is WorkerJobStatus.ERROR

                assert terminal_statuses[0].status == WorkerJobStatus.ERROR.value
                assert coordinator.get_state(job_id) is WorkerJobStatus.ERROR
                assert submitted_ack.count == 1

                assert not done_operation_entered.is_set()
                assert not _object_exists(
                    s3_client=s3_client,
                    bucket=S3_BUCKET,
                    key=output_key,
                )

            finally:
                status_subscription.close()

    finally:
        _delete_queue_if_exists(rabbitmq_properties, status_queue_name)


def test_two_real_error_publishers_race_and_only_one_terminal_status_appears(
    rabbitmq_properties: RabbitMqProperties,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # Two real terminal publishers race, only one terminal status appears.
    #
    # job state = IN_PROGRESS
    #
    # Thread A:
    #   execution watcher attempts ERROR
    #
    # Thread B:
    #   timeout watcher attempts ERROR
    #
    # Both operations use real RabbitMQ status publisher.
    #
    # Expected:
    #   only one operation publishes ERROR
    #   status queue contains exactly one terminal message for jobId
    #   coordinator state is ERROR
    #   submitted ack called once
    #
    # Even if both terminal targets are the same (ERROR -> ERROR), the second
    # transition must be stale and must not publish or ack again.
    test_id = str(uuid4())
    job_id = f"job-{test_id}"
    status_queue_name = f"test.status.transition_race.error_error.{test_id}"

    _declare_queue(rabbitmq_properties, status_queue_name)

    status_handler = StatusCollectingHandler()

    try:
        with RabbitMqQueueClient(
            rabbitmq_properties,
            prefetch_count=STATUS_PREFETCH_COUNT,
        ) as status_client:
            status_subscription = status_client.subscribe(
                status_queue_name,
                JobStatusUpdateDTO,
                status_handler,
            )

            try:
                coordinator = JobStateTransitionCoordinator()
                submitted_ack = CountingAcknowledger()
                _create_in_progress_record(
                    coordinator=coordinator,
                    job_id=job_id,
                    submitted_ack=submitted_ack,
                )

                workspace = _workspace(tmp_path=tmp_path, job_id=job_id)

                first_error_status_published = threading.Event()
                release_first_error_operation = threading.Event()
                second_error_operation_entered = threading.Event()

                def execution_watcher_error_operation() -> str:
                    _publish_error_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                        message="Execution watcher observed process failure.",
                    )
                    first_error_status_published.set()
                    _wait_for_event(
                        release_first_error_operation,
                        "release_first_error_operation",
                    )
                    return "execution-error-published"

                def timeout_watcher_error_operation() -> str:
                    second_error_operation_entered.set()
                    _publish_error_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                        message="Timeout watcher terminated the job.",
                    )
                    return "timeout-error-published"

                first_thread, first_result_holder = _start_thread(
                    "execution-watcher-error-transition",
                    lambda: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.ERROR,
                        operation=execution_watcher_error_operation,
                    ),
                )

                _wait_for_event(
                    first_error_status_published,
                    "first_error_status_published",
                )

                second_thread, second_result_holder = _start_thread(
                    "timeout-watcher-error-transition",
                    lambda: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.ERROR,
                        operation=timeout_watcher_error_operation,
                    ),
                )

                _wait_until(lambda: second_thread.is_alive())
                assert not second_error_operation_entered.wait(SHORT_WAIT_SECONDS)

                release_first_error_operation.set()

                first_result = _join_thread(first_thread, first_result_holder)
                second_result = _join_thread(second_thread, second_result_holder)

                _wait_until(
                    lambda: len(_terminal_statuses(status_handler, job_id)) == 1
                )

                terminal_statuses = _terminal_statuses(status_handler, job_id)

                assert first_result.committed
                assert not first_result.failed
                assert not first_result.stale

                assert second_result.stale
                assert not second_result.committed
                assert not second_result.failed
                assert second_result.current_state is WorkerJobStatus.ERROR

                assert len(terminal_statuses) == 1
                assert terminal_statuses[0].status == WorkerJobStatus.ERROR.value
                assert coordinator.get_state(job_id) is WorkerJobStatus.ERROR
                assert submitted_ack.count == 1
                assert not second_error_operation_entered.is_set()

            finally:
                status_subscription.close()

    finally:
        _delete_queue_if_exists(rabbitmq_properties, status_queue_name)


def test_different_jobs_do_not_block_each_other_with_real_rabbitmq_and_s3(
    rabbitmq_properties: RabbitMqProperties,
    s3_client,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # Different jobs do not block each other while using real RabbitMQ/S3.
    #
    # job-1:
    #   DONE transition blocks inside real-side-effect wrapper
    #
    # job-2:
    #   DONE transition starts and completes while job-1 is blocked
    #
    # Expected:
    #   job-2 DONE transition completes without waiting for job-1
    #   job-2 DONE status appears in RabbitMQ while job-1 is still blocked
    #   job-2 output object exists in S3 while job-1 is still blocked
    #   job-1 remains blocked until the test explicitly releases it
    #   after release, job-1 DONE transition completes successfully
    #   job-1 DONE status appears in RabbitMQ
    #   job-1 output object exists in S3
    #   both jobs acknowledge their submitted messages exactly once
    #
    # This is an integration proof that there is no accidental global
    # coordinator lock around S3/RabbitMQ operations, while each job still
    # completes its own terminal transition correctly.
    test_id = str(uuid4())
    job_1_id = f"job-1-{test_id}"
    job_2_id = f"job-2-{test_id}"
    status_queue_name = f"test.status.transition_race.different_jobs.{test_id}"

    job_1_output_key = _output_key(job_1_id)
    job_2_output_key = _output_key(job_2_id)

    _declare_queue(rabbitmq_properties, status_queue_name)

    status_handler = StatusCollectingHandler()

    try:
        with RabbitMqQueueClient(
            rabbitmq_properties,
            prefetch_count=STATUS_PREFETCH_COUNT,
        ) as status_client:
            status_subscription = status_client.subscribe(
                status_queue_name,
                JobStatusUpdateDTO,
                status_handler,
            )

            try:
                coordinator = JobStateTransitionCoordinator()

                job_1_ack = CountingAcknowledger()
                job_2_ack = CountingAcknowledger()

                _create_in_progress_record(
                    coordinator=coordinator,
                    job_id=job_1_id,
                    submitted_ack=job_1_ack,
                )
                _create_in_progress_record(
                    coordinator=coordinator,
                    job_id=job_2_id,
                    submitted_ack=job_2_ack,
                )

                job_1_workspace = _workspace(
                    tmp_path=tmp_path,
                    job_id=job_1_id,
                )
                job_2_workspace = _workspace(
                    tmp_path=tmp_path,
                    job_id=job_2_id,
                )

                job_1_operation_entered = threading.Event()
                release_job_1_operation = threading.Event()
                job_2_operation_entered = threading.Event()

                def job_1_done_operation() -> str:
                    _put_bytes(
                        s3_client=s3_client,
                        bucket=S3_BUCKET,
                        key=job_1_output_key,
                        body=b"job-1-result\n",
                    )
                    job_1_operation_entered.set()
                    _wait_for_event(
                        release_job_1_operation,
                        "release_job_1_operation",
                    )
                    _publish_done_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=job_1_workspace,
                    )
                    return "job-1-done-published"

                def job_2_done_operation() -> str:
                    job_2_operation_entered.set()
                    _put_bytes(
                        s3_client=s3_client,
                        bucket=S3_BUCKET,
                        key=job_2_output_key,
                        body=b"job-2-result\n",
                    )
                    _publish_done_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=job_2_workspace,
                    )
                    return "job-2-done-published"

                job_1_thread, job_1_result_holder = _start_thread(
                    "job-1-done-transition",
                    lambda: coordinator.transition(
                        job_id=job_1_id,
                        target_state=WorkerJobStatus.DONE,
                        operation=job_1_done_operation,
                    ),
                )

                _wait_for_event(
                    job_1_operation_entered,
                    "job_1_operation_entered",
                )

                job_2_thread, job_2_result_holder = _start_thread(
                    "job-2-done-transition",
                    lambda: coordinator.transition(
                        job_id=job_2_id,
                        target_state=WorkerJobStatus.DONE,
                        operation=job_2_done_operation,
                    ),
                )

                job_2_result = _join_thread(job_2_thread, job_2_result_holder)

                _wait_until(
                    lambda: _has_status(
                        status_handler,
                        job_2_id,
                        WorkerJobStatus.DONE,
                    )
                )

                assert job_1_thread.is_alive()
                assert job_2_result.committed
                assert job_2_operation_entered.is_set()

                # Do not call coordinator.get_state(job_1_id) here.
                #
                # job-1 transition intentionally holds the per-job lock while its operation is
                # blocked by release_job_1_operation. get_state(job_1_id) is a consistent read:
                # it also waits for the same per-job lock, so calling it here would block this
                # test thread before it can release job-1. That would create a test-level
                # circular wait, not a coordinator bug.
                assert coordinator.get_state(job_2_id) is WorkerJobStatus.DONE

                assert job_1_ack.count == 0
                assert job_2_ack.count == 1

                assert _object_exists(
                    s3_client=s3_client,
                    bucket=S3_BUCKET,
                    key=job_2_output_key,
                )

                release_job_1_operation.set()

                job_1_result = _join_thread(job_1_thread, job_1_result_holder)

                assert job_1_result.committed
                assert not job_1_result.failed
                assert not job_1_result.stale

                assert coordinator.get_state(job_1_id) is WorkerJobStatus.DONE
                assert job_1_ack.count == 1

                assert _object_exists(
                    s3_client=s3_client,
                    bucket=S3_BUCKET,
                    key=job_1_output_key,
                )

                _wait_until(
                    lambda: _has_status(
                        status_handler,
                        job_1_id,
                        WorkerJobStatus.DONE,
                    )
                )

                _assert_single_terminal_status(
                    status_handler=status_handler,
                    job_id=job_1_id,
                    expected_status=WorkerJobStatus.DONE,
                )
                _assert_single_terminal_status(
                    status_handler=status_handler,
                    job_id=job_2_id,
                    expected_status=WorkerJobStatus.DONE,
                )

            finally:
                status_subscription.close()

    finally:
        _delete_queue_if_exists(rabbitmq_properties, status_queue_name)


def _create_in_progress_record(
    *,
    coordinator: JobStateTransitionCoordinator,
    job_id: str,
    submitted_ack: CountingAcknowledger,
) -> None:
    record = coordinator.create(
        job_id=job_id,
        submitted_ack=cast(Acknowledger, submitted_ack),
    )

    with record.lock:
        record.state = WorkerJobStatus.IN_PROGRESS


def _workspace(
    *,
    tmp_path: Path,
    job_id: str,
) -> JobWorkspace:
    manifest = JobManifest(
        manifest_version=1,
        user_id=USER_ID,
        job_id=job_id,
        job_type=JOB_TYPE,
        inputs={},
        params={},
        outputs={},
    )

    work_dir = tmp_path / "jobs" / str(USER_ID) / job_id

    return JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=work_dir / "in",
        output_dir=work_dir / "out",
        worker_id=WORKER_ID,
    )


def _publish_done_status(
    *,
    rabbitmq_properties: RabbitMqProperties,
    status_queue_name: str,
    workspace: JobWorkspace,
) -> None:
    with RabbitMqQueueClient(rabbitmq_properties) as queue_client:
        publisher = StatusPublisher(
            worker_status_queue_name=status_queue_name,
            queue_client=queue_client,
            clock=lambda: FIXED_TIME,
        )
        publisher.publish_done(
            workspace=workspace,
            message="Job completed successfully",
        )


def _publish_error_status(
    *,
    rabbitmq_properties: RabbitMqProperties,
    status_queue_name: str,
    workspace: JobWorkspace,
    message: str,
) -> None:
    with RabbitMqQueueClient(rabbitmq_properties) as queue_client:
        publisher = StatusPublisher(
            worker_status_queue_name=status_queue_name,
            queue_client=queue_client,
            clock=lambda: FIXED_TIME,
        )
        publisher.publish_error(
            workspace=workspace,
            message=message,
        )


def _start_thread(name: str, target) -> tuple[threading.Thread, _ThreadResult]:
    result = _ThreadResult()

    def run() -> None:
        try:
            result.value = target()
        except BaseException as error:  # pylint: disable=broad-exception-caught
            result.error = error

    thread = threading.Thread(target=run, name=name)
    thread.start()

    return thread, result


def _join_thread(thread: threading.Thread, result: _ThreadResult):
    thread.join(timeout=WAIT_TIMEOUT_SECONDS)

    assert not thread.is_alive(), f"Thread did not finish: {thread.name}"

    if result.error is not None:
        raise result.error

    return result.value


def _wait_for_event(event: threading.Event, name: str) -> None:
    assert event.wait(
        timeout=WAIT_TIMEOUT_SECONDS
    ), f"Event was not set before timeout: {name}"


def _wait_until(
    condition,
    *,
    timeout_seconds: float = WAIT_TIMEOUT_SECONDS,
    interval_seconds: float = 0.05,
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if condition():
            return

        time.sleep(interval_seconds)

    raise AssertionError("Condition was not satisfied before timeout.")


def _has_status(
    status_handler: StatusCollectingHandler,
    job_id: str,
    expected_status: WorkerJobStatus,
) -> bool:
    return any(
        status.jobId == job_id and status.status == expected_status.value
        for status in status_handler.snapshot()
    )


def _terminal_statuses(
    status_handler: StatusCollectingHandler,
    job_id: str,
) -> list[JobStatusUpdateDTO]:
    return [
        status
        for status in status_handler.snapshot()
        if status.jobId == job_id and status.status in TERMINAL_STATUS_VALUES
    ]


def _output_key(job_id: str) -> str:
    return f"jobs/{USER_ID}/{job_id}/out/sum.csv"


def _put_bytes(
    *,
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/csv",
    )


def _create_bucket_if_absent(s3_client, bucket: str) -> None:
    existing_buckets = s3_client.list_buckets()["Buckets"]
    existing_bucket_names = {bucket_info["Name"] for bucket_info in existing_buckets}

    if bucket not in existing_bucket_names:
        s3_client.create_bucket(Bucket=bucket)


def _object_exists(
    *,
    s3_client,
    bucket: str,
    key: str,
) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        http_status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if error_code in {"404", "NoSuchKey", "NotFound"} or http_status == 404:
            return False

        raise


def _declare_queue(properties: RabbitMqProperties, queue_name: str) -> None:
    connection = pika.BlockingConnection(_connection_parameters(properties))
    try:
        channel = connection.channel()
        channel.queue_declare(
            queue=queue_name,
            durable=False,
            exclusive=False,
            auto_delete=False,
        )
    finally:
        connection.close()


def _delete_queue_if_exists(
    properties: RabbitMqProperties,
    queue_name: str,
) -> None:
    connection = pika.BlockingConnection(_connection_parameters(properties))
    try:
        channel = connection.channel()
        channel.queue_delete(queue=queue_name)
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


def _create_s3_client(minio_container):
    config = minio_container.get_config()

    endpoint = (
        config.get("endpoint")
        if isinstance(config, dict)
        else getattr(config, "endpoint")
    )

    if not endpoint.startswith(("http://", "https://")):
        endpoint = "http://" + endpoint

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _run_started_transition(
    *,
    started: threading.Event,
    transition,
):
    started.set()
    return transition()


def _assert_single_terminal_status(
    *,
    status_handler: StatusCollectingHandler,
    job_id: str,
    expected_status: WorkerJobStatus,
) -> None:
    assert [status.status for status in _terminal_statuses(status_handler, job_id)] == [
        expected_status.value
    ]
