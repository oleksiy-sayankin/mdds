# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock
from typing import cast
from unittest.mock import MagicMock

from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.job_state import JobStateTransitionCoordinator
from mdds_worker_runtime.job_state.transition_result import TransitionResult
from mdds_worker_runtime.queue.queue_client import Acknowledger

JOB_ID = "job-1"
OTHER_JOB_ID = "job-2"

THREAD_COUNT = 32
WAIT_TIMEOUT_SECONDS = 5.0


def test_same_job_allows_only_one_concurrent_transition_operation() -> None:
    # Scenario:
    # Many threads try to perform the same SUBMITTED -> INPUTS_PREPARED transition
    # for the same job at the same time. The coordinator must allow exactly one
    # thread to run operation(); all other threads must wait for the per-job lock
    # and then observe stale state.
    coordinator = JobStateTransitionCoordinator()
    ack_mock = _create_state_record(
        coordinator,
        job_id=JOB_ID,
        state=WorkerJobStatus.SUBMITTED,
    )

    start_barrier = Barrier(THREAD_COUNT)
    operation_entered = Event()
    release_operation = Event()

    operation_call_count = 0
    operation_count_lock = Lock()

    def operation() -> str:
        nonlocal operation_call_count

        with operation_count_lock:
            operation_call_count += 1

        operation_entered.set()

        if not release_operation.wait(timeout=WAIT_TIMEOUT_SECONDS):
            raise AssertionError("Timed out waiting to release transition operation.")

        return "prepared-inputs"

    def contender() -> TransitionResult[str]:
        start_barrier.wait(timeout=WAIT_TIMEOUT_SECONDS)

        return coordinator.transition(
            job_id=JOB_ID,
            target_state=WorkerJobStatus.INPUTS_PREPARED,
            operation=operation,
        )

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(contender) for _ in range(THREAD_COUNT)]

        try:
            assert operation_entered.wait(timeout=WAIT_TIMEOUT_SECONDS)
        finally:
            release_operation.set()

        results = [future.result(timeout=WAIT_TIMEOUT_SECONDS) for future in futures]

    committed_results = [result for result in results if result.committed]
    stale_results = [result for result in results if result.stale]
    failed_results = [result for result in results if result.failed]

    assert operation_call_count == 1
    assert len(committed_results) == 1
    assert committed_results[0].value == "prepared-inputs"
    assert committed_results[0].current_state is WorkerJobStatus.INPUTS_PREPARED

    assert len(stale_results) == THREAD_COUNT - 1
    assert all(
        result.current_state is WorkerJobStatus.INPUTS_PREPARED
        for result in stale_results
    )

    assert failed_results == []
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.INPUTS_PREPARED
    ack_mock.ack.assert_not_called()


def test_failed_transition_preserves_source_state_and_next_terminal_transition_can_commit() -> (
    None
):
    # Scenario:
    # One thread starts a protected happy-path transition and its operation()
    # fails. Another thread is waiting to perform the fallback terminal transition
    # from the same source state. The failed operation must not mutate state, so
    # the fallback transition must still be able to commit SUBMITTED -> ERROR.
    coordinator = JobStateTransitionCoordinator()
    ack_mock = _create_state_record(
        coordinator,
        job_id=JOB_ID,
        state=WorkerJobStatus.SUBMITTED,
    )

    failing_operation_entered = Event()
    release_failing_operation = Event()
    terminal_operation_entered = Event()

    prepare_error = RuntimeError("input preparation failed")

    def failing_operation() -> None:
        failing_operation_entered.set()

        if not release_failing_operation.wait(timeout=WAIT_TIMEOUT_SECONDS):
            raise AssertionError("Timed out waiting to release failing operation.")

        raise prepare_error

    def terminal_operation() -> str:
        terminal_operation_entered.set()
        return "error-published"

    def run_failing_transition() -> TransitionResult[None]:
        return coordinator.transition(
            job_id=JOB_ID,
            target_state=WorkerJobStatus.INPUTS_PREPARED,
            operation=failing_operation,
        )

    def run_terminal_transition() -> TransitionResult[str]:
        return coordinator.transition(
            job_id=JOB_ID,
            target_state=WorkerJobStatus.ERROR,
            operation=terminal_operation,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        failing_future = executor.submit(run_failing_transition)

        assert failing_operation_entered.wait(timeout=WAIT_TIMEOUT_SECONDS)

        terminal_future = executor.submit(run_terminal_transition)

        release_failing_operation.set()

        failing_result = failing_future.result(timeout=WAIT_TIMEOUT_SECONDS)
        terminal_result = terminal_future.result(timeout=WAIT_TIMEOUT_SECONDS)

    assert failing_result.failed
    assert failing_result.error is prepare_error
    assert failing_result.current_state is WorkerJobStatus.SUBMITTED

    assert terminal_operation_entered.is_set()
    assert terminal_result.committed
    assert terminal_result.value == "error-published"
    assert terminal_result.current_state is WorkerJobStatus.ERROR

    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR
    ack_mock.ack.assert_called_once_with()


def test_terminal_transition_race_commits_once_and_acknowledges_once() -> None:
    # Scenario:
    # Many runtime components race to finalize the same IN_PROGRESS job as ERROR.
    # Only one terminal transition may run operation() and commit. The submitted
    # job message must be acknowledged exactly once.
    coordinator = JobStateTransitionCoordinator()
    ack_mock = _create_state_record(
        coordinator,
        job_id=JOB_ID,
        state=WorkerJobStatus.IN_PROGRESS,
    )

    start_barrier = Barrier(THREAD_COUNT)

    operation_call_count = 0
    operation_count_lock = Lock()

    def operation() -> str:
        nonlocal operation_call_count

        with operation_count_lock:
            operation_call_count += 1

        return "error-published"

    def contender() -> TransitionResult[str]:
        start_barrier.wait(timeout=WAIT_TIMEOUT_SECONDS)

        return coordinator.transition(
            job_id=JOB_ID,
            target_state=WorkerJobStatus.ERROR,
            operation=operation,
        )

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        results = [
            future.result(timeout=WAIT_TIMEOUT_SECONDS)
            for future in [executor.submit(contender) for _ in range(THREAD_COUNT)]
        ]

    committed_results = [result for result in results if result.committed]
    stale_results = [result for result in results if result.stale]
    failed_results = [result for result in results if result.failed]

    assert operation_call_count == 1
    assert len(committed_results) == 1
    assert committed_results[0].current_state is WorkerJobStatus.ERROR
    assert committed_results[0].value == "error-published"

    assert len(stale_results) == THREAD_COUNT - 1
    assert all(
        result.current_state is WorkerJobStatus.ERROR for result in stale_results
    )

    assert failed_results == []
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR
    ack_mock.ack.assert_called_once_with()


def test_remove_if_state_cannot_remove_record_during_in_flight_transition() -> None:
    # Scenario:
    # A transition thread holds the per-job lock while executing operation().
    # Cleanup tries to remove the same job while the operation is still running.
    # remove_if_state() must not observe the old source state during the protected
    # operation. It must wait, then see the committed target state.
    coordinator = JobStateTransitionCoordinator()
    _create_state_record(
        coordinator,
        job_id=JOB_ID,
        state=WorkerJobStatus.IN_PROGRESS,
    )

    operation_entered = Event()
    release_operation = Event()
    remover_started = Event()
    remover_completed = Event()

    def operation() -> str:
        operation_entered.set()

        if not release_operation.wait(timeout=WAIT_TIMEOUT_SECONDS):
            raise AssertionError("Timed out waiting to release transition operation.")

        return "done-published"

    def run_transition() -> TransitionResult[str]:
        return coordinator.transition(
            job_id=JOB_ID,
            target_state=WorkerJobStatus.DONE,
            operation=operation,
        )

    def run_remove() -> bool:
        remover_started.set()
        try:
            return coordinator.remove_if_state(
                job_id=JOB_ID,
                expected_states={WorkerJobStatus.IN_PROGRESS},
            )
        finally:
            remover_completed.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        transition_future = executor.submit(run_transition)

        assert operation_entered.wait(timeout=WAIT_TIMEOUT_SECONDS)

        remove_future = executor.submit(run_remove)

        assert remover_started.wait(timeout=WAIT_TIMEOUT_SECONDS)
        assert not remover_completed.wait(timeout=0.2)

        release_operation.set()

        transition_result = transition_future.result(timeout=WAIT_TIMEOUT_SECONDS)
        remove_result = remove_future.result(timeout=WAIT_TIMEOUT_SECONDS)

    assert transition_result.committed
    assert transition_result.value == "done-published"

    assert remove_result is False
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE


def test_different_jobs_do_not_block_each_other_while_one_operation_is_running() -> (
    None
):
    # Scenario:
    # One job has a long-running operation() under its per-job lock. A second job
    # must still be able to transition independently. This protects the contract:
    # per-job lock serializes only one job, not the whole coordinator.
    coordinator = JobStateTransitionCoordinator()

    first_ack_mock = _create_state_record(
        coordinator,
        job_id=JOB_ID,
        state=WorkerJobStatus.SUBMITTED,
    )
    second_ack_mock = _create_state_record(
        coordinator,
        job_id=OTHER_JOB_ID,
        state=WorkerJobStatus.SUBMITTED,
    )

    first_operation_entered = Event()
    release_first_operation = Event()
    second_operation_entered = Event()

    def first_operation() -> str:
        first_operation_entered.set()

        if not release_first_operation.wait(timeout=WAIT_TIMEOUT_SECONDS):
            raise AssertionError("Timed out waiting to release first operation.")

        return "first-prepared"

    def second_operation() -> str:
        second_operation_entered.set()
        return "second-prepared"

    def run_first_transition() -> TransitionResult[str]:
        return coordinator.transition(
            job_id=JOB_ID,
            target_state=WorkerJobStatus.INPUTS_PREPARED,
            operation=first_operation,
        )

    def run_second_transition() -> TransitionResult[str]:
        return coordinator.transition(
            job_id=OTHER_JOB_ID,
            target_state=WorkerJobStatus.INPUTS_PREPARED,
            operation=second_operation,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(run_first_transition)

        assert first_operation_entered.wait(timeout=WAIT_TIMEOUT_SECONDS)

        second_future = executor.submit(run_second_transition)
        second_result = second_future.result(timeout=WAIT_TIMEOUT_SECONDS)

        assert second_operation_entered.is_set()
        assert second_result.committed
        assert second_result.value == "second-prepared"
        assert coordinator.get_state(OTHER_JOB_ID) is WorkerJobStatus.INPUTS_PREPARED

        release_first_operation.set()
        first_result = first_future.result(timeout=WAIT_TIMEOUT_SECONDS)

    assert first_result.committed
    assert first_result.value == "first-prepared"

    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.INPUTS_PREPARED
    assert coordinator.get_state(OTHER_JOB_ID) is WorkerJobStatus.INPUTS_PREPARED

    first_ack_mock.ack.assert_not_called()
    second_ack_mock.ack.assert_not_called()


def test_concurrent_cleanup_removes_terminal_record_once() -> None:
    # Scenario:
    # Many cleanup-like threads race to remove the same terminal record.
    # The coordinator must remove the record at most once; all other removers
    # must observe that the record is already gone.
    coordinator = JobStateTransitionCoordinator()
    _create_state_record(
        coordinator,
        job_id=JOB_ID,
        state=WorkerJobStatus.ERROR,
    )

    start_barrier = Barrier(THREAD_COUNT)

    def remover() -> bool:
        start_barrier.wait(timeout=WAIT_TIMEOUT_SECONDS)

        return coordinator.remove_if_state(
            job_id=JOB_ID,
            expected_states={
                WorkerJobStatus.DONE,
                WorkerJobStatus.ERROR,
                WorkerJobStatus.CANCELLED,
            },
        )

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        results = [
            future.result(timeout=WAIT_TIMEOUT_SECONDS)
            for future in [executor.submit(remover) for _ in range(THREAD_COUNT)]
        ]

    assert results.count(True) == 1
    assert results.count(False) == THREAD_COUNT - 1
    assert coordinator.get(JOB_ID) is None
    assert coordinator.get_state(JOB_ID) is None


def test_many_independent_jobs_can_transition_concurrently_without_cross_job_state_corruption() -> (
    None
):
    # Scenario:
    # Many different job ids transition concurrently through the same coordinator.
    # This verifies that records are isolated by job id and that per-job state is
    # not corrupted by concurrent access to the shared coordinator.
    coordinator = JobStateTransitionCoordinator()

    job_ids = [f"job-{index}" for index in range(THREAD_COUNT)]

    for job_id in job_ids:
        _create_state_record(
            coordinator,
            job_id=job_id,
            state=WorkerJobStatus.SUBMITTED,
        )

    start_barrier = Barrier(THREAD_COUNT)

    def transition_job(job_id: str) -> TransitionResult[str]:
        start_barrier.wait(timeout=WAIT_TIMEOUT_SECONDS)

        return coordinator.transition(
            job_id=job_id,
            target_state=WorkerJobStatus.INPUTS_PREPARED,
            operation=lambda: f"{job_id}-prepared",
        )

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        results = [
            future.result(timeout=WAIT_TIMEOUT_SECONDS)
            for future in [
                executor.submit(transition_job, job_id) for job_id in job_ids
            ]
        ]

    assert all(result.committed for result in results)
    assert sorted(result.value for result in results) == sorted(
        f"{job_id}-prepared" for job_id in job_ids
    )

    for job_id in job_ids:
        assert coordinator.get_state(job_id) is WorkerJobStatus.INPUTS_PREPARED


def _create_state_record(
    coordinator: JobStateTransitionCoordinator,
    *,
    job_id: str,
    state: WorkerJobStatus,
) -> MagicMock:
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))

    record = coordinator.create(
        job_id=job_id,
        submitted_ack=ack,
    )

    with record.lock:
        record.state = state

    return ack_mock
