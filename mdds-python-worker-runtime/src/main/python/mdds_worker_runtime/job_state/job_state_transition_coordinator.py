# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
from collections.abc import Callable, Collection
from threading import Lock
from typing import TypeVar

from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.job_state.transition_result import TransitionResult
from mdds_worker_runtime.job_state.worker_job_state_record import WorkerJobStateRecord
from mdds_worker_runtime.queue.queue_client import Acknowledger

T = TypeVar("T")

logger = logging.getLogger(__name__)


class JobStateTransitionCoordinator:
    """Coordinates worker-local lifecycle transitions for submitted jobs.

    The coordinator owns the Worker Runtime state machine after a job identity is
    known. It serializes all transitions for the same job id with a per-job lock,
    while allowing transitions for different jobs to proceed independently.

    This class does not provide distributed transactions across RabbitMQ, S3,
    the local filesystem, or the Metadata Store. It provides worker-local state
    ownership and enforces the ordering required by the Worker Runtime contract.
    """

    def __init__(
        self,
    ) -> None:
        """Create an empty coordinator with no known job state records."""
        self._records: dict[str, WorkerJobStateRecord] = {}
        self._records_lock = Lock()

    def create(
        self,
        job_id: str,
        submitted_ack: Acknowledger,
    ) -> WorkerJobStateRecord:
        self._validate_job_id(job_id)
        self._validate_ack(submitted_ack)

        with self._records_lock:
            if job_id in self._records:
                raise ValueError(f"Job state record already exists: job_id='{job_id}'.")
            record = WorkerJobStateRecord(
                lock=Lock(),
                state=WorkerJobStatus.SUBMITTED,
                submitted_ack=submitted_ack,
            )
            self._records[job_id] = record
            return record

    def get(
        self,
        job_id: str,
    ) -> WorkerJobStateRecord | None:
        """Return the worker-local state record for a job.

        The ``initial_state`` is used only when the record does not exist yet.
        Existing records are returned unchanged, because their state is already
        owned by previous coordinator-managed transitions.
        """
        self._validate_job_id(job_id)

        with self._records_lock:
            record = self._records.get(job_id)
            return record

    def get_state(self, job_id: str) -> WorkerJobStatus | None:
        """Return the current worker-local state for a job, if it is known."""
        self._validate_job_id(job_id)

        with self._records_lock:
            record = self._records.get(job_id)

        if record is None:
            return None

        with record.lock:
            return record.state

    def transition(
        self,
        *,
        job_id: str,
        target_state: WorkerJobStatus,
        operation: Callable[[], T],
    ) -> TransitionResult[T]:
        """Execute one worker-local lifecycle transition.

        The operation represents the attempted transition side effects. It must not
        call another transition for the same job id.

        If the current state cannot switch to target_state according to
        WorkerJobStatus.can_switch_to(...), the result is stale and operation is not
        executed. If operation raises an exception, worker-local state remains
        unchanged and the returned result contains that exception.

        For terminal target states, the coordinator commits the terminal state first
        and then acknowledges the original submitted job message. If acknowledgement
        fails after terminal state commit, the terminal state is not rolled back and
        the returned result is failed with current_state equal to target_state.
        """
        self._validate_job_id(job_id)
        self._validate_operation(operation)

        with self._records_lock:
            record = self._records.get(job_id)

        if record is None:
            return TransitionResult.stale_result(current_state=None)

        with record.lock:
            current_state = record.state

            if not current_state.can_switch_to(target_state):
                return TransitionResult.stale_result(current_state=current_state)

            try:
                value = operation()
            except Exception as error:
                return TransitionResult.failed_result(
                    current_state=current_state,
                    error=error,
                )

            record.state = target_state
            # The submitted job message is acknowledged only after terminal status
            # publication succeeds and the worker-local terminal state is committed.
            if record.state.terminal:
                try:
                    record.submitted_ack.ack()
                except Exception as error:
                    return TransitionResult.failed_result(
                        current_state=target_state,
                        error=error,
                    )
            logger.info(
                f"Transition completed: {current_state} -> {target_state}",
                extra={
                    "component": "job_state_transition_coordinator",
                    "event": "transition_completed",
                    "jobId": job_id,
                },
            )
            return TransitionResult.committed_result(
                value=value,
                target_state=target_state,
            )

    def remove_if_state(
        self,
        *,
        job_id: str,
        expected_states: Collection[WorkerJobStatus],
    ) -> bool:
        """Remove a job state record only if it is in an expected state.

        This method is intended for coordinator-owned cleanup. It avoids removing
        state for jobs that have moved to another state while cleanup was being
        considered.
        """
        self._validate_job_id(job_id)

        with self._records_lock:
            record = self._records.get(job_id)

        if record is None:
            return False

        with record.lock:
            if record.state not in expected_states:
                return False

            with self._records_lock:
                if self._records.get(job_id) is record:
                    del self._records[job_id]
                    return True

                return False

    @staticmethod
    def _validate_job_id(job_id: str) -> None:
        if not job_id or not job_id.strip():
            raise ValueError("job_id must not be blank")

    @staticmethod
    def _validate_ack(submitted_ack: Acknowledger) -> None:
        if submitted_ack is None:
            raise ValueError("submitted_ack must not be null")

    @staticmethod
    def _validate_operation(operation: Callable[[], object] | None) -> None:
        if operation is None:
            raise ValueError("operation must not be null")
