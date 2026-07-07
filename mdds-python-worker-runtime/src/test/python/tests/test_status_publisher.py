# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.dto.messages import JobStatusUpdateDTO
from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.execution.status_publisher import (
    StatusPublisher,
    _format_event_time,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace, JobWorkspaceFactory
from mdds_worker_runtime.queue.queue_client import QueueMessage
from tests.test_execution_models import FIXED_TIME


def test_status_publisher_publishes_in_progress_status_message(tmp_path) -> None:
    queue_client = MagicMock()

    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=queue_client,
        clock=lambda: FIXED_TIME,
    )
    workspace = _workspace(tmp_path)
    publisher.publish_in_progress(
        workspace=workspace,
        progress=0,
        message="Start job execution",
    )

    queue_client.publish.assert_called_once()
    queue_name, published_message = queue_client.publish.call_args.args

    assert queue_name == "mdds_status_queue"
    assert isinstance(published_message, QueueMessage)

    payload = published_message.payload

    assert isinstance(payload, JobStatusUpdateDTO)
    assert payload.jobId == "job-1"
    assert payload.job_id == "job-1"
    assert payload.workerId == "worker-1"
    assert payload.worker_id == "worker-1"
    assert payload.status == WorkerJobStatus.IN_PROGRESS.value
    assert payload.progress == 0
    assert payload.message == "Start job execution"
    assert payload.eventTime == "2026-01-01T00:00:00Z"
    assert payload.event_time == payload.eventTime


def test_status_publisher_trims_worker_status_queue_name(tmp_path) -> None:
    queue_client = MagicMock()

    publisher = StatusPublisher(
        worker_status_queue_name="  mdds_status_queue  ",
        queue_client=queue_client,
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)
    publisher.publish_in_progress(
        workspace=workspace,
        progress=0,
        message="Start job execution",
    )

    queue_name, _published_message = queue_client.publish.call_args.args

    assert queue_name == "mdds_status_queue"


def test_status_publisher_rejects_null_queue_client() -> None:
    with pytest.raises(ValueError, match="queue_client cannot be null."):
        StatusPublisher(
            worker_status_queue_name="mdds_status_queue",
            queue_client=None,
        )


@pytest.mark.parametrize("worker_status_queue_name", [None, "", " "])
def test_status_publisher_rejects_null_or_blank_worker_status_queue_name(
    worker_status_queue_name,
) -> None:
    with pytest.raises(
        ValueError,
        match="worker_status_queue_name cannot be null or blank.",
    ):
        StatusPublisher(
            worker_status_queue_name=worker_status_queue_name,
            queue_client=MagicMock(),
        )


def test_format_event_time_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="event_time must be timezone-aware."):
        _format_event_time(datetime(2026, 1, 1, 0, 0, 0))


def test_format_event_time_formats_utc_datetime_as_rfc3339_zulu() -> None:
    assert _format_event_time(FIXED_TIME) == "2026-01-01T00:00:00Z"


@pytest.mark.parametrize("job_id", [None, "", " "])
def test_status_publisher_rejects_null_or_blank_job_id(job_id, tmp_path) -> None:
    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=MagicMock(),
        clock=lambda: FIXED_TIME,
    )

    with pytest.raises(ValueError, match="job_id cannot be null or blank."):
        workspace = _workspace(tmp_path, job_id=job_id)
        publisher.publish_in_progress(
            workspace=workspace,
            progress=0,
            message="Start job execution",
        )


@pytest.mark.parametrize("job_type", [None, "", " "])
def test_status_publisher_rejects_null_or_blank_job_type(job_type, tmp_path) -> None:
    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=MagicMock(),
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path, job_type=job_type)
    with pytest.raises(ValueError, match="job_type cannot be null or blank."):
        publisher.publish_in_progress(
            workspace=workspace,
            progress=0,
            message="Start job execution",
        )


@pytest.mark.parametrize("worker_id", [None, "", " "])
def test_status_publisher_rejects_null_or_blank_worker_id(worker_id, tmp_path) -> None:
    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=MagicMock(),
        clock=lambda: FIXED_TIME,
    )

    with pytest.raises(ValueError, match="worker_id cannot be null or blank."):
        workspace = _workspace(tmp_path, worker_id=worker_id)
        publisher.publish_in_progress(
            workspace=workspace,
            progress=0,
            message="Start job execution",
        )


@pytest.mark.parametrize("progress", [-1, 101])
def test_status_publisher_rejects_progress_out_of_range(progress, tmp_path) -> None:
    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=MagicMock(),
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)
    with pytest.raises(ValueError, match="progress must be between 0 and 100."):
        publisher.publish_in_progress(
            workspace=workspace,
            progress=progress,
            message="Start job execution",
        )


@pytest.mark.parametrize("message", [None, "", " "])
def test_status_publisher_rejects_null_or_blank_message(message, tmp_path) -> None:
    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=MagicMock(),
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)
    with pytest.raises(ValueError, match="message cannot be null or blank."):
        publisher.publish_in_progress(
            workspace=workspace,
            progress=0,
            message=message,
        )


def test_status_publisher_publishes_done_status_message(tmp_path) -> None:
    queue_client = MagicMock()

    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=queue_client,
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)
    publisher.publish_done(
        workspace=workspace,
        message="Job completed successfully",
    )

    queue_client.publish.assert_called_once()
    queue_name, published_message = queue_client.publish.call_args.args

    assert queue_name == "mdds_status_queue"
    assert isinstance(published_message, QueueMessage)

    payload = published_message.payload

    assert isinstance(payload, JobStatusUpdateDTO)
    assert payload.jobId == "job-1"
    assert payload.job_id == "job-1"
    assert payload.workerId == "worker-1"
    assert payload.worker_id == "worker-1"
    assert payload.status == WorkerJobStatus.DONE.value
    assert payload.progress == 100
    assert payload.message == "Job completed successfully"
    assert payload.eventTime == "2026-01-01T00:00:00Z"
    assert payload.event_time == payload.eventTime


def test_status_publisher_publishes_done_status_message_with_default_message(
    tmp_path,
) -> None:
    queue_client = MagicMock()

    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=queue_client,
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)
    publisher.publish_done(workspace=workspace)

    queue_client.publish.assert_called_once()
    queue_name, published_message = queue_client.publish.call_args.args

    assert queue_name == "mdds_status_queue"
    assert isinstance(published_message, QueueMessage)

    payload = published_message.payload

    assert isinstance(payload, JobStatusUpdateDTO)
    assert payload.jobId == "job-1"
    assert payload.workerId == "worker-1"
    assert payload.status == WorkerJobStatus.DONE.value
    assert payload.progress == 100
    assert payload.message == "Job completed successfully."
    assert payload.eventTime == "2026-01-01T00:00:00Z"


def test_status_publisher_publishes_error_status_message(tmp_path) -> None:
    queue_client = MagicMock()

    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=queue_client,
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)

    publisher.publish_error(
        workspace=workspace,
        message="Supervised execution failed",
    )

    queue_client.publish.assert_called_once()
    queue_name, published_message = queue_client.publish.call_args.args

    assert queue_name == "mdds_status_queue"
    assert isinstance(published_message, QueueMessage)

    payload = published_message.payload

    assert isinstance(payload, JobStatusUpdateDTO)
    assert payload.jobId == "job-1"
    assert payload.job_id == "job-1"
    assert payload.workerId == "worker-1"
    assert payload.worker_id == "worker-1"
    assert payload.status == WorkerJobStatus.ERROR.value
    assert payload.progress == 100
    assert payload.message == "Supervised execution failed"
    assert payload.eventTime == "2026-01-01T00:00:00Z"
    assert payload.event_time == payload.eventTime


@pytest.mark.parametrize("message", [None, "", " "])
def test_status_publisher_publish_error_rejects_null_or_blank_message(
    message, tmp_path
) -> None:
    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=MagicMock(),
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)

    with pytest.raises(ValueError, match="message cannot be null or blank."):
        publisher.publish_error(
            workspace=workspace,
            message=message,
        )


def test_status_publisher_publishes_cancelled_status_message(tmp_path) -> None:
    queue_client = MagicMock()

    publisher = StatusPublisher(
        worker_status_queue_name="mdds_status_queue",
        queue_client=queue_client,
        clock=lambda: FIXED_TIME,
    )

    workspace = _workspace(tmp_path)

    publisher.publish_cancelled(
        workspace=workspace,
        message="Job cancellation requested and applied",
    )

    queue_client.publish.assert_called_once()
    queue_name, published_message = queue_client.publish.call_args.args

    assert queue_name == "mdds_status_queue"
    assert isinstance(published_message, QueueMessage)

    payload = published_message.payload

    assert isinstance(payload, JobStatusUpdateDTO)
    assert payload.jobId == "job-1"
    assert payload.job_id == "job-1"
    assert payload.workerId == "worker-1"
    assert payload.worker_id == "worker-1"
    assert payload.status == WorkerJobStatus.CANCELLED.value
    assert payload.progress == 100
    assert payload.message == "Job cancellation requested and applied"
    assert payload.eventTime == "2026-01-01T00:00:00Z"
    assert payload.event_time == payload.eventTime


def _workspace(
    tmp_path: Path,
    *,
    job_type: str = "SOLVING_SLAE",
    user_id: int = 42,
    job_id: str = "job-1",
    worker_id: str = "worker-1",
) -> JobWorkspace:
    manifest = JobManifest(
        manifest_version=1,
        user_id=user_id,
        job_id=job_id,
        job_type=job_type,
        inputs={},
        params={},
        outputs={},
    )

    factory = JobWorkspaceFactory(jobs_root=tmp_path, worker_id=worker_id)
    return factory.create(manifest=manifest)
