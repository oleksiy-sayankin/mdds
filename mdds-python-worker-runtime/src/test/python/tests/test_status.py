# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from mdds_worker_runtime.execution.status import WorkerStatus


def test_worker_status_values_match_wire_contract() -> None:
    assert WorkerStatus.VALIDATION_FAILED.value == "VALIDATION_FAILED"
    assert WorkerStatus.IN_PROGRESS.value == "IN_PROGRESS"
    assert WorkerStatus.DONE.value == "DONE"
    assert WorkerStatus.ERROR.value == "ERROR"
    assert WorkerStatus.CANCELLED.value == "CANCELLED"


def test_worker_status_does_not_include_web_server_owned_statuses() -> None:
    status_values = {status.value for status in WorkerStatus}

    assert "DRAFT" not in status_values
    assert "SUBMITTED" not in status_values
    assert "CANCEL_REQUESTED" not in status_values
