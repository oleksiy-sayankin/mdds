# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from typing import cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.job_state import JobStateTransitionCoordinator
from mdds_worker_runtime.job_state.worker_job_state_record import WorkerJobStateRecord
from mdds_worker_runtime.queue.queue_client import Acknowledger

JOB_ID = "job-1234"


def test_create_creates_submitted_record() -> None:
    # Scenario:
    # A submitted job message has been consumed and job identity is known.
    # The coordinator must create exactly one worker-local state record in
    # SUBMITTED state and store the original submitted-message acknowledger.
    coordinator = JobStateTransitionCoordinator()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    record = coordinator.create(
        job_id=JOB_ID,
        submitted_ack=ack,
    )

    assert record.state is WorkerJobStatus.SUBMITTED
    assert record.submitted_ack is ack
    assert coordinator.get(JOB_ID) is record
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.SUBMITTED
    ack_mock.ack.assert_not_called()


def test_create_rejects_duplicate_job_state_record() -> None:
    # Scenario:
    # A second submitted message for the same jobId appears while the coordinator
    # already owns worker-local state for that job. This must not overwrite the
    # existing state record, lock, or submitted-message acknowledger.
    coordinator = JobStateTransitionCoordinator()

    first_ack_mock = MagicMock(spec=Acknowledger)
    first_ack = cast(Acknowledger, cast(object, first_ack_mock))

    second_ack_mock = MagicMock(spec=Acknowledger)
    second_ack = cast(Acknowledger, cast(object, second_ack_mock))

    first_record = coordinator.create(
        job_id=JOB_ID,
        submitted_ack=first_ack,
    )

    with pytest.raises(ValueError, match="Job state record already exists"):
        coordinator.create(
            job_id=JOB_ID,
            submitted_ack=second_ack,
        )

    assert coordinator.get(JOB_ID) is first_record
    assert first_record.submitted_ack is first_ack
    assert first_record.submitted_ack is not second_ack
    first_ack_mock.ack.assert_not_called()
    second_ack_mock.ack.assert_not_called()


@pytest.mark.parametrize("bad_job_id", [None, "", " "])
def test_create_rejects_blank_job_id(bad_job_id: str | None) -> None:
    # Scenario:
    # The coordinator must not create worker-local state without a valid jobId.
    coordinator = JobStateTransitionCoordinator()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    with pytest.raises(ValueError, match="job_id must not be blank"):
        coordinator.create(
            job_id=cast(str, bad_job_id),
            submitted_ack=ack,
        )

    ack_mock.ack.assert_not_called()


def test_create_rejects_null_submitted_ack() -> None:
    # Scenario:
    # Terminal transitions may happen before an ExecutionRecord exists, so the
    # submitted-message acknowledger is mandatory when creating coordinator state.
    coordinator = JobStateTransitionCoordinator()

    with pytest.raises(ValueError, match="submitted_ack must not be null"):
        coordinator.create(
            job_id=JOB_ID,
            submitted_ack=cast(Acknowledger, None),
        )


def test_get_returns_none_for_unknown_job() -> None:
    # Scenario:
    # Watchers may ask about jobs unknown to this worker-local coordinator.
    # Unknown state must be represented as None.
    coordinator = JobStateTransitionCoordinator()

    assert coordinator.get(JOB_ID) is None
    assert coordinator.get_state(JOB_ID) is None


@pytest.mark.parametrize("bad_job_id", [None, "", " "])
def test_get_rejects_blank_job_id(bad_job_id: str | None) -> None:
    # Scenario:
    # Read operations should fail fast on invalid job identity.
    coordinator = JobStateTransitionCoordinator()

    with pytest.raises(ValueError, match="job_id must not be blank"):
        coordinator.get(cast(str, bad_job_id))


@pytest.mark.parametrize("bad_job_id", [None, "", " "])
def test_get_state_rejects_blank_job_id(bad_job_id: str | None) -> None:
    # Scenario:
    # State reads must use the same jobId validation as state creation and
    # transition operations.
    coordinator = JobStateTransitionCoordinator()

    with pytest.raises(ValueError, match="job_id must not be blank"):
        coordinator.get_state(cast(str, bad_job_id))


def test_transition_returns_stale_when_record_does_not_exist() -> None:
    # Scenario:
    # A watcher or consumer asks for a transition for an unknown job. The
    # coordinator must not run operation() and must report the attempt as stale.
    coordinator = JobStateTransitionCoordinator()
    operation = MagicMock(return_value="prepared")

    result = coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.INPUTS_PREPARED,
        operation=operation,
    )

    assert result.stale
    assert not result.committed
    assert not result.failed
    assert result.current_state is None
    assert result.target_state is None
    assert result.value is None
    assert result.error is None
    operation.assert_not_called()


def test_transition_returns_stale_when_state_cannot_switch_to_target() -> None:
    # Scenario:
    # The job is still SUBMITTED, but a component tries to jump directly to DONE.
    # The coordinator must reject the invalid transition before operation() runs.
    fixture = _state_fixture(WorkerJobStatus.SUBMITTED)
    operation = MagicMock(return_value="done")

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.DONE,
        operation=operation,
    )

    assert result.stale
    assert not result.committed
    assert not result.failed
    assert result.current_state is WorkerJobStatus.SUBMITTED
    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.SUBMITTED
    operation.assert_not_called()
    fixture.ack_mock.ack.assert_not_called()


def test_transition_commits_non_terminal_state_when_operation_succeeds() -> None:
    # Scenario:
    # Input preparation succeeds. The coordinator must run operation() under the
    # per-job transition contract, move SUBMITTED -> INPUTS_PREPARED, and must
    # not acknowledge the submitted job message because the state is non-terminal.
    fixture = _state_fixture(WorkerJobStatus.SUBMITTED)
    operation = MagicMock(return_value="prepared-inputs")

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.INPUTS_PREPARED,
        operation=operation,
    )

    assert result.committed
    assert not result.failed
    assert not result.stale
    assert result.value == "prepared-inputs"
    assert result.current_state is WorkerJobStatus.INPUTS_PREPARED
    assert result.target_state is WorkerJobStatus.INPUTS_PREPARED
    assert result.error is None

    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.INPUTS_PREPARED
    operation.assert_called_once_with()
    fixture.ack_mock.ack.assert_not_called()


def test_transition_keeps_source_state_when_operation_fails() -> None:
    # Scenario:
    # A protected transition action fails before state commit. The coordinator
    # must keep the source state unchanged and must not acknowledge the submitted
    # job message.
    fixture = _state_fixture(WorkerJobStatus.SUBMITTED)
    operation_error = RuntimeError("input preparation failed")
    operation = MagicMock(side_effect=operation_error)

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.INPUTS_PREPARED,
        operation=operation,
    )

    assert result.failed
    assert not result.committed
    assert not result.stale
    assert result.current_state is WorkerJobStatus.SUBMITTED
    assert result.target_state is None
    assert result.value is None
    assert result.error is operation_error

    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.SUBMITTED
    operation.assert_called_once_with()
    fixture.ack_mock.ack.assert_not_called()


def test_in_progress_can_publish_progress_without_terminal_ack() -> None:
    # Scenario:
    # Periodic progress publication is a coordinated IN_PROGRESS -> IN_PROGRESS
    # operation. It may run under the job lock, but it must not acknowledge the
    # submitted job message because the job is still non-terminal.
    fixture = _state_fixture(WorkerJobStatus.IN_PROGRESS)
    operation = MagicMock(return_value="progress-published")

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.IN_PROGRESS,
        operation=operation,
    )

    assert result.committed
    assert result.value == "progress-published"
    assert result.current_state is WorkerJobStatus.IN_PROGRESS
    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS
    operation.assert_called_once_with()
    fixture.ack_mock.ack.assert_not_called()


def test_terminal_transition_commits_state_and_acknowledges_submitted_message() -> None:
    # Scenario:
    # Supervised execution completed successfully. The coordinator must commit
    # IN_PROGRESS -> DONE and acknowledge the original submitted job message
    # only after the terminal transition operation succeeds.
    fixture = _state_fixture(WorkerJobStatus.IN_PROGRESS)
    operation = MagicMock(return_value="done-published")

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.DONE,
        operation=operation,
    )

    assert result.committed
    assert not result.failed
    assert not result.stale
    assert result.value == "done-published"
    assert result.current_state is WorkerJobStatus.DONE
    assert result.target_state is WorkerJobStatus.DONE

    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE
    operation.assert_called_once_with()
    fixture.ack_mock.ack.assert_called_once_with()


def test_terminal_transition_does_not_ack_when_operation_fails_before_state_commit() -> (
    None
):
    # Scenario:
    # Terminal status publication fails before terminal state commit. The
    # coordinator must not move to ERROR and must not acknowledge the submitted
    # job message.
    fixture = _state_fixture(WorkerJobStatus.IN_PROGRESS)
    operation_error = RuntimeError("publish error failed")
    operation = MagicMock(side_effect=operation_error)

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.ERROR,
        operation=operation,
    )

    assert result.failed
    assert not result.committed
    assert not result.stale
    assert result.current_state is WorkerJobStatus.IN_PROGRESS
    assert result.error is operation_error

    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS
    operation.assert_called_once_with()
    fixture.ack_mock.ack.assert_not_called()


def test_terminal_transition_returns_failed_result_when_ack_fails_after_terminal_commit() -> (
    None
):
    # Scenario:
    # Terminal status publication succeeds and the coordinator commits terminal
    # ERROR, but submitted-message ack fails afterwards. The coordinator must not
    # roll back the terminal state and must return a failed transition result
    # whose current_state is the already committed terminal target state.
    fixture = _state_fixture(WorkerJobStatus.IN_PROGRESS)

    ack_error = RuntimeError("ack failed")
    fixture.ack_mock.ack.side_effect = ack_error

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.ERROR,
        operation=lambda: "error-published",
    )

    assert result.failed
    assert not result.committed
    assert not result.stale
    assert result.current_state is WorkerJobStatus.ERROR
    assert result.error is ack_error
    assert result.value is None

    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR
    fixture.ack_mock.ack.assert_called_once_with()


def test_late_terminal_transition_is_stale_after_terminal_state_is_committed() -> None:
    # Scenario:
    # The job has already reached ERROR. A later component tries to commit DONE.
    # Terminal states are absorbing: the late terminal attempt must be stale and
    # operation() must not run.
    fixture = _state_fixture(WorkerJobStatus.ERROR)
    operation = MagicMock(return_value="done-published")

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.DONE,
        operation=operation,
    )

    assert result.stale
    assert not result.committed
    assert not result.failed
    assert result.current_state is WorkerJobStatus.ERROR
    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR
    operation.assert_not_called()
    fixture.ack_mock.ack.assert_not_called()


def test_repeated_same_terminal_transition_is_stale_after_terminal_state_is_committed() -> (
    None
):
    # Scenario:
    # The job has already reached ERROR. Another component tries to commit ERROR
    # again. This must not publish a second terminal status and must not ack the
    # submitted message again.
    fixture = _state_fixture(WorkerJobStatus.ERROR)
    operation = MagicMock(return_value="error-published-again")

    result = fixture.coordinator.transition(
        job_id=JOB_ID,
        target_state=WorkerJobStatus.ERROR,
        operation=operation,
    )

    assert result.stale
    assert not result.committed
    assert not result.failed
    assert result.current_state is WorkerJobStatus.ERROR
    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR
    operation.assert_not_called()
    fixture.ack_mock.ack.assert_not_called()


def test_remove_if_state_removes_record_when_state_matches() -> None:
    # Scenario:
    # Cleanup sees a terminal worker-local state and asks the coordinator to
    # remove the state record. Removal is allowed only if the current state still
    # matches the expected cleanup-eligible state.
    fixture = _state_fixture(WorkerJobStatus.ERROR)

    removed = fixture.coordinator.remove_if_state(
        job_id=JOB_ID,
        expected_states={
            WorkerJobStatus.DONE,
            WorkerJobStatus.ERROR,
            WorkerJobStatus.CANCELLED,
        },
    )

    assert removed is True
    assert fixture.coordinator.get(JOB_ID) is None
    assert fixture.coordinator.get_state(JOB_ID) is None
    fixture.ack_mock.ack.assert_not_called()


def test_remove_if_state_does_not_remove_record_when_state_does_not_match() -> None:
    # Scenario:
    # Cleanup attempts to remove a non-terminal job. The coordinator must keep
    # the state record because the current state is not cleanup-eligible.
    fixture = _state_fixture(WorkerJobStatus.IN_PROGRESS)

    removed = fixture.coordinator.remove_if_state(
        job_id=JOB_ID,
        expected_states={
            WorkerJobStatus.DONE,
            WorkerJobStatus.ERROR,
            WorkerJobStatus.CANCELLED,
        },
    )

    assert removed is False
    assert fixture.coordinator.get(JOB_ID) is fixture.record
    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS
    fixture.ack_mock.ack.assert_not_called()


def test_remove_if_state_returns_false_for_unknown_job() -> None:
    # Scenario:
    # Cleanup may race with another cleanup attempt. If the state record is
    # already gone, removal must be idempotent and return False.
    coordinator = JobStateTransitionCoordinator()

    removed = coordinator.remove_if_state(
        job_id=JOB_ID,
        expected_states={WorkerJobStatus.ERROR},
    )

    assert removed is False


@pytest.mark.parametrize("bad_job_id", [None, "", " "])
def test_remove_if_state_rejects_blank_job_id(bad_job_id: str | None) -> None:
    # Scenario:
    # Cleanup removal must validate job identity before touching coordinator
    # state.
    coordinator = JobStateTransitionCoordinator()

    with pytest.raises(ValueError, match="job_id must not be blank"):
        coordinator.remove_if_state(
            job_id=cast(str, bad_job_id),
            expected_states={WorkerJobStatus.ERROR},
        )


@pytest.mark.parametrize("bad_job_id", [None, "", " "])
def test_transition_rejects_blank_job_id(bad_job_id: str | None) -> None:
    # Scenario:
    # A transition without valid job identity must fail fast before operation()
    # can run.
    coordinator = JobStateTransitionCoordinator()
    operation = MagicMock(return_value=None)

    with pytest.raises(ValueError, match="job_id must not be blank"):
        coordinator.transition(
            job_id=cast(str, bad_job_id),
            target_state=WorkerJobStatus.ERROR,
            operation=operation,
        )

    operation.assert_not_called()


def test_transition_rejects_null_operation() -> None:
    # Scenario:
    # The coordinator transition primitive is meaningless without an operation.
    # It must reject null operation before reading or mutating state.
    fixture = _state_fixture(WorkerJobStatus.SUBMITTED)

    with pytest.raises(ValueError, match="operation must not be null"):
        fixture.coordinator.transition(
            job_id=JOB_ID,
            target_state=WorkerJobStatus.INPUTS_PREPARED,
            operation=cast(object, None),
        )

    assert fixture.coordinator.get_state(JOB_ID) is WorkerJobStatus.SUBMITTED
    fixture.ack_mock.ack.assert_not_called()


@dataclass(frozen=True)
class _StateFixture:
    coordinator: JobStateTransitionCoordinator
    record: WorkerJobStateRecord
    ack_mock: MagicMock


def _state_fixture(
    state: WorkerJobStatus,
    *,
    job_id: str = JOB_ID,
) -> _StateFixture:
    coordinator = JobStateTransitionCoordinator()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    record = coordinator.create(
        job_id=job_id,
        submitted_ack=ack,
    )

    with record.lock:
        record.state = state

    return _StateFixture(
        coordinator=coordinator,
        record=record,
        ack_mock=ack_mock,
    )
