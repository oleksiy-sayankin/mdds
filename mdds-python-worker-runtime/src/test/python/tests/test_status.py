# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from mdds_worker_runtime.execution.models import WorkerJobStatus


def test_worker_status_values_match_wire_contract() -> None:
    assert WorkerJobStatus.IN_PROGRESS.value == "IN_PROGRESS"
    assert WorkerJobStatus.DONE.value == "DONE"
    assert WorkerJobStatus.ERROR.value == "ERROR"
    assert WorkerJobStatus.CANCELLED.value == "CANCELLED"


def test_worker_status_does_not_include_web_server_owned_statuses() -> None:
    status_values = {status.value for status in WorkerJobStatus}

    assert "DRAFT" not in status_values
