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
# than on the coordinator lock. These tests verify cancellation atomicity, so the
# status consumer must be able to receive more than one pending status message.
STATUS_PREFETCH_COUNT = 10

CANCELLATION_ATTEMPT_COUNT = 32

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
S3_BUCKET = "mdds-test"

WORKER_ID = "worker-cancellation-atomicity-it"
JOB_TYPE = "two_numbers_sum"
USER_ID = 123
FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

WAIT_TIMEOUT_SECONDS = 15.0

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


class ThreadSafeCounter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0

    def increment(self) -> None:
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


def test_repeated_cancellation_attempts_for_one_running_job_commit_one_cancelled(
    rabbitmq_properties: RabbitMqProperties,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # Many cancellation messages for one running job produce one CANCELLED.
    #
    # job state = IN_PROGRESS
    #
    # Thread A:
    #   starts the first CANCELLED transition
    #   publishes CANCELLED to RabbitMQ
    #   blocks before commit/ack inside cancellation finalization operation
    #
    # Other threads:
    #   repeatedly try CANCELLED for the same jobId as duplicate cancellation
    #   messages would do
    #
    # Expected:
    #   only one cancellation operation runs
    #   CANCELLED status is published once to RabbitMQ
    #   coordinator state is CANCELLED
    #   submitted ack is called once
    #   no DONE status is published
    #   no ERROR status is published
    test_id = str(uuid4())
    job_id = f"job-{test_id}"
    status_queue_name = f"test.status.cancellation_atomicity.one_job.{test_id}"

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

                cancellation_operation_count = ThreadSafeCounter()
                first_cancellation_entered = threading.Event()
                release_first_cancellation = threading.Event()

                def cancellation_operation() -> str:
                    cancellation_operation_count.increment()
                    _publish_cancelled_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                    )
                    first_cancellation_entered.set()
                    _wait_for_event(
                        release_first_cancellation,
                        "release_first_cancellation",
                    )
                    return "cancelled-published"

                first_thread, first_result_holder = _start_thread(
                    "first-cancellation-transition",
                    lambda: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.CANCELLED,
                        operation=cancellation_operation,
                    ),
                )

                _wait_for_event(
                    first_cancellation_entered,
                    "first_cancellation_entered",
                )

                duplicate_threads, duplicate_result_holders = _start_transition_threads(
                    name_prefix="duplicate-cancellation-transition",
                    count=CANCELLATION_ATTEMPT_COUNT - 1,
                    transition_factory=lambda _index: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.CANCELLED,
                        operation=cancellation_operation,
                    ),
                )

                release_first_cancellation.set()

                results = [
                    _join_thread(first_thread, first_result_holder),
                    *_join_threads(duplicate_threads, duplicate_result_holders),
                ]

                _wait_until(
                    lambda: len(_terminal_statuses(status_handler, job_id)) == 1
                )

                assert _committed_count(results) == 1
                assert _stale_count(results) == CANCELLATION_ATTEMPT_COUNT - 1
                assert _failed_count(results) == 0

                assert cancellation_operation_count.count == 1
                assert coordinator.get_state(job_id) is WorkerJobStatus.CANCELLED
                assert submitted_ack.count == 1

                _assert_single_terminal_status(
                    status_handler=status_handler,
                    job_id=job_id,
                    expected_status=WorkerJobStatus.CANCELLED,
                )
                _assert_no_status(
                    status_handler=status_handler,
                    job_id=job_id,
                    unexpected_status=WorkerJobStatus.DONE,
                )
                _assert_no_status(
                    status_handler=status_handler,
                    job_id=job_id,
                    unexpected_status=WorkerJobStatus.ERROR,
                )

            finally:
                status_subscription.close()

    finally:
        _delete_queue_if_exists(rabbitmq_properties, status_queue_name)


def test_repeated_cancellation_attempts_do_not_steal_in_progress_done_transition(
    rabbitmq_properties: RabbitMqProperties,
    s3_client,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # Repeated cancellation attempts while DONE is already in progress must not
    # steal DONE.
    #
    # job state = IN_PROGRESS
    #
    # Thread A:
    #   starts DONE transition
    #   uploads output artifact to MinIO
    #   blocks before publishing DONE or before commit/ack
    #
    # Other threads:
    #   repeatedly try CANCELLED for the same jobId as duplicate cancellation
    #   messages would do
    #
    # Expected:
    #   DONE wins because DONE transition already owns the per-job lock
    #   cancellation operations do not run
    #   CANCELLED status is not published
    #   DONE status is published once to RabbitMQ
    #   output object exists in S3
    #   coordinator state is DONE
    #   submitted ack is called once
    test_id = str(uuid4())
    job_id = f"job-{test_id}"
    status_queue_name = f"test.status.cancellation_atomicity.done_wins.{test_id}"
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
                cancellation_operation_count = ThreadSafeCounter()

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

                def cancellation_operation() -> str:
                    cancellation_operation_count.increment()
                    _publish_cancelled_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                    )
                    return "cancelled-published"

                done_thread, done_result_holder = _start_thread(
                    "done-transition",
                    lambda: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.DONE,
                        operation=done_operation,
                    ),
                )

                _wait_for_event(done_operation_entered, "done_operation_entered")

                cancellation_threads, cancellation_result_holders = (
                    _start_transition_threads(
                        name_prefix="repeated-cancellation-transition",
                        count=CANCELLATION_ATTEMPT_COUNT,
                        transition_factory=lambda _index: coordinator.transition(
                            job_id=job_id,
                            target_state=WorkerJobStatus.CANCELLED,
                            operation=cancellation_operation,
                        ),
                    )
                )

                release_done_operation.set()

                done_result = _join_thread(done_thread, done_result_holder)
                cancellation_results = _join_threads(
                    cancellation_threads,
                    cancellation_result_holders,
                )

                _wait_until(
                    lambda: len(_terminal_statuses(status_handler, job_id)) == 1
                )

                assert done_result.committed
                assert not done_result.failed
                assert not done_result.stale

                assert _stale_count(cancellation_results) == CANCELLATION_ATTEMPT_COUNT
                assert _committed_count(cancellation_results) == 0
                assert _failed_count(cancellation_results) == 0

                assert cancellation_operation_count.count == 0
                assert coordinator.get_state(job_id) is WorkerJobStatus.DONE
                assert submitted_ack.count == 1

                assert _object_exists(
                    s3_client=s3_client,
                    bucket=S3_BUCKET,
                    key=output_key,
                )

                _assert_single_terminal_status(
                    status_handler=status_handler,
                    job_id=job_id,
                    expected_status=WorkerJobStatus.DONE,
                )
                _assert_no_status(
                    status_handler=status_handler,
                    job_id=job_id,
                    unexpected_status=WorkerJobStatus.CANCELLED,
                )

            finally:
                status_subscription.close()

    finally:
        _delete_queue_if_exists(rabbitmq_properties, status_queue_name)


def test_repeated_cancellation_attempts_after_terminal_state_are_stale(
    rabbitmq_properties: RabbitMqProperties,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # Cancellation messages after terminal state are stale and harmless.
    #
    # job state = CANCELLED
    #
    # Setup:
    #   first cancellation transition has already committed terminal CANCELLED
    #
    # Other threads:
    #   repeatedly try CANCELLED for the same jobId after terminal state is
    #   already committed
    #
    # Expected:
    #   no additional cancellation operation runs
    #   no additional terminal status is published
    #   coordinator state remains CANCELLED
    #   submitted ack count does not change
    #   all duplicate cancellation attempts are stale
    test_id = str(uuid4())
    job_id = f"job-{test_id}"
    status_queue_name = f"test.status.cancellation_atomicity.stale.{test_id}"

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

                initial_cancel_result = coordinator.transition(
                    job_id=job_id,
                    target_state=WorkerJobStatus.CANCELLED,
                    operation=lambda: _publish_cancelled_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                    ),
                )

                _wait_until(
                    lambda: len(_terminal_statuses(status_handler, job_id)) == 1
                )

                cancellation_operation_count = ThreadSafeCounter()

                def duplicate_cancellation_operation() -> str:
                    cancellation_operation_count.increment()
                    _publish_cancelled_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=workspace,
                    )
                    return "duplicate-cancelled-published"

                duplicate_threads, duplicate_result_holders = _start_transition_threads(
                    name_prefix="stale-cancellation-transition",
                    count=CANCELLATION_ATTEMPT_COUNT,
                    transition_factory=lambda _index: coordinator.transition(
                        job_id=job_id,
                        target_state=WorkerJobStatus.CANCELLED,
                        operation=duplicate_cancellation_operation,
                    ),
                )

                duplicate_results = _join_threads(
                    duplicate_threads,
                    duplicate_result_holders,
                )

                assert initial_cancel_result.committed
                assert not initial_cancel_result.failed
                assert not initial_cancel_result.stale

                assert _stale_count(duplicate_results) == CANCELLATION_ATTEMPT_COUNT
                assert _committed_count(duplicate_results) == 0
                assert _failed_count(duplicate_results) == 0

                assert cancellation_operation_count.count == 0
                assert coordinator.get_state(job_id) is WorkerJobStatus.CANCELLED
                assert submitted_ack.count == 1

                _assert_single_terminal_status(
                    status_handler=status_handler,
                    job_id=job_id,
                    expected_status=WorkerJobStatus.CANCELLED,
                )

            finally:
                status_subscription.close()

    finally:
        _delete_queue_if_exists(rabbitmq_properties, status_queue_name)


def test_repeated_cancellation_attempts_for_one_job_do_not_block_another_job(
    rabbitmq_properties: RabbitMqProperties,
    s3_client,
    tmp_path: Path,
) -> None:
    # Scenario:
    #
    # Repeated cancellation attempts for one job do not block another job.
    #
    # job-1 state = IN_PROGRESS
    # job-2 state = IN_PROGRESS
    #
    # job-1:
    #   first CANCELLED transition starts
    #   publishes CANCELLED to RabbitMQ
    #   blocks before commit/ack inside cancellation finalization operation
    #   duplicate cancellation transitions queue behind the same per-job lock
    #
    # job-2:
    #   DONE transition starts and completes while job-1 cancellation is blocked
    #
    # Expected:
    #   job-2 DONE transition completes without waiting for job-1 cancellation
    #   job-2 DONE status appears in RabbitMQ while job-1 is still blocked
    #   job-2 output object exists in S3 while job-1 is still blocked
    #   after release, job-1 CANCELLED transition completes successfully
    #   job-1 CANCELLED status appears in RabbitMQ exactly once
    #   both jobs acknowledge their submitted messages exactly once
    test_id = str(uuid4())
    job_1_id = f"job-1-{test_id}"
    job_2_id = f"job-2-{test_id}"
    status_queue_name = f"test.status.cancellation_atomicity.two_jobs.{test_id}"

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

                job_1_workspace = _workspace(tmp_path=tmp_path, job_id=job_1_id)
                job_2_workspace = _workspace(tmp_path=tmp_path, job_id=job_2_id)

                job_1_cancellation_entered = threading.Event()
                release_job_1_cancellation = threading.Event()
                job_1_cancellation_operation_count = ThreadSafeCounter()
                job_2_done_operation_entered = threading.Event()

                def job_1_cancellation_operation() -> str:
                    job_1_cancellation_operation_count.increment()
                    _publish_cancelled_status(
                        rabbitmq_properties=rabbitmq_properties,
                        status_queue_name=status_queue_name,
                        workspace=job_1_workspace,
                    )
                    job_1_cancellation_entered.set()
                    _wait_for_event(
                        release_job_1_cancellation,
                        "release_job_1_cancellation",
                    )
                    return "job-1-cancelled-published"

                def job_2_done_operation() -> str:
                    job_2_done_operation_entered.set()
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

                first_job_1_thread, first_job_1_result_holder = _start_thread(
                    "job-1-first-cancellation-transition",
                    lambda: coordinator.transition(
                        job_id=job_1_id,
                        target_state=WorkerJobStatus.CANCELLED,
                        operation=job_1_cancellation_operation,
                    ),
                )

                _wait_for_event(
                    job_1_cancellation_entered,
                    "job_1_cancellation_entered",
                )

                duplicate_job_1_threads, duplicate_job_1_result_holders = (
                    _start_transition_threads(
                        name_prefix="job-1-duplicate-cancellation-transition",
                        count=CANCELLATION_ATTEMPT_COUNT - 1,
                        transition_factory=lambda _index: coordinator.transition(
                            job_id=job_1_id,
                            target_state=WorkerJobStatus.CANCELLED,
                            operation=job_1_cancellation_operation,
                        ),
                    )
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

                assert first_job_1_thread.is_alive()
                assert job_2_result.committed
                assert job_2_done_operation_entered.is_set()

                # Do not call coordinator.get_state(job_1_id) here. job-1
                # cancellation intentionally holds the per-job lock while its
                # operation is blocked by release_job_1_cancellation. Reading
                # job-1 state here would block this test thread before it can
                # release job-1 and would create a test-level circular wait.
                assert coordinator.get_state(job_2_id) is WorkerJobStatus.DONE

                assert job_1_ack.count == 0
                assert job_2_ack.count == 1

                assert _object_exists(
                    s3_client=s3_client,
                    bucket=S3_BUCKET,
                    key=job_2_output_key,
                )

                release_job_1_cancellation.set()

                job_1_results = [
                    _join_thread(first_job_1_thread, first_job_1_result_holder),
                    *_join_threads(
                        duplicate_job_1_threads,
                        duplicate_job_1_result_holders,
                    ),
                ]

                _wait_until(
                    lambda: _has_status(
                        status_handler,
                        job_1_id,
                        WorkerJobStatus.CANCELLED,
                    )
                )

                assert _committed_count(job_1_results) == 1
                assert _stale_count(job_1_results) == CANCELLATION_ATTEMPT_COUNT - 1
                assert _failed_count(job_1_results) == 0

                assert job_1_cancellation_operation_count.count == 1
                assert coordinator.get_state(job_1_id) is WorkerJobStatus.CANCELLED
                assert job_1_ack.count == 1

                _assert_single_terminal_status(
                    status_handler=status_handler,
                    job_id=job_1_id,
                    expected_status=WorkerJobStatus.CANCELLED,
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
        submitted_ack=cast(Acknowledger, cast(object, submitted_ack)),
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


def _publish_cancelled_status(
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
        publisher.publish_cancelled(
            workspace=workspace,
            message="Job cancellation requested and applied.",
        )


def _start_transition_threads(
    *,
    name_prefix: str,
    count: int,
    transition_factory,
) -> tuple[list[threading.Thread], list[_ThreadResult]]:
    threads: list[threading.Thread] = []
    result_holders: list[_ThreadResult] = []
    started_events: list[threading.Event] = []

    for index in range(count):
        started = threading.Event()
        thread, result_holder = _start_thread(
            f"{name_prefix}-{index}",
            lambda index=index, started=started: _run_started_transition(
                started=started,
                transition=lambda: transition_factory(index),
            ),
        )
        threads.append(thread)
        result_holders.append(result_holder)
        started_events.append(started)

    for index, started in enumerate(started_events):
        _wait_for_event(started, f"{name_prefix}-{index}-started")

    return threads, result_holders


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


def _join_threads(
    threads: list[threading.Thread],
    result_holders: list[_ThreadResult],
) -> list[Any]:
    return [
        _join_thread(thread, result_holder)
        for thread, result_holder in zip(threads, result_holders, strict=True)
    ]


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


def _run_started_transition(
    *,
    started: threading.Event,
    transition,
):
    started.set()
    return transition()


def _committed_count(results: list[Any]) -> int:
    return sum(1 for result in results if result.committed)


def _stale_count(results: list[Any]) -> int:
    return sum(1 for result in results if result.stale)


def _failed_count(results: list[Any]) -> int:
    return sum(1 for result in results if result.failed)


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


def _assert_single_terminal_status(
    *,
    status_handler: StatusCollectingHandler,
    job_id: str,
    expected_status: WorkerJobStatus,
) -> None:
    assert [status.status for status in _terminal_statuses(status_handler, job_id)] == [
        expected_status.value
    ]


def _assert_no_status(
    *,
    status_handler: StatusCollectingHandler,
    job_id: str,
    unexpected_status: WorkerJobStatus,
) -> None:
    assert not _has_status(
        status_handler=status_handler,
        job_id=job_id,
        expected_status=unexpected_status,
    )


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
