# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import logging
from pathlib import Path
import shutil
import threading

from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)

logger = logging.getLogger(__name__)

DEFAULT_CLEANUP_INTERVAL_SECONDS = 1.0
CLEANUP_ELIGIBLE_STATES = {
    WorkerJobStatus.DONE,
    WorkerJobStatus.ERROR,
    WorkerJobStatus.CANCELLED,
}


class CleanupWatcher:
    """Removes terminal execution records and local job workspaces.

    CleanupWatcher is a local Worker Runtime housekeeping component.

    It does not decide terminal job outcomes, does not publish statuses, does
    not acknowledge queue messages, does not upload artifacts, and does not
    terminate supervised processes.

    A record is cleanup-eligible after worker-local state is terminal. Terminal
    states are committed by JobStateTransitionCoordinator only after the
    required terminal side effects have completed.
    """

    def __init__(
        self,
        execution_registry: ExecutionRegistry,
        job_state_transition_coordinator: JobStateTransitionCoordinator,
        worker_id: str,
        cleanup_interval_seconds: float = DEFAULT_CLEANUP_INTERVAL_SECONDS,
    ) -> None:
        if execution_registry is None:
            raise ValueError("execution_registry cannot be null.")
        if job_state_transition_coordinator is None:
            raise ValueError("job_state_transition_coordinator cannot be null.")
        if worker_id is None or worker_id.strip() == "":
            raise ValueError("worker_id cannot be null or blank.")
        if cleanup_interval_seconds <= 0:
            raise ValueError("cleanup_interval_seconds must be greater than zero.")

        self._execution_registry = execution_registry
        self._job_state_transition_coordinator = job_state_transition_coordinator
        self._worker_id = worker_id.strip()
        self._cleanup_interval_seconds = cleanup_interval_seconds

        self._stop_requested = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start background cleanup watcher thread."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop_requested.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="mdds-cleanup-watcher",
                daemon=True,
            )
            self._thread.start()

        logger.info(
            "Cleanup watcher started.",
            extra={
                "component": "cleanup_watcher",
                "event": "cleanup_watcher_started",
                "workerId": self._worker_id,
            },
        )

    def stop(self, timeout_seconds: float = 10.0) -> None:
        """Request watcher shutdown and wait for its thread to stop."""
        self._stop_requested.set()

        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout_seconds)

        logger.info(
            "Cleanup watcher stopped.",
            extra={
                "component": "cleanup_watcher",
                "event": "cleanup_watcher_stopped",
                "workerId": self._worker_id,
            },
        )

    def poll_once(self) -> None:
        """Process one snapshot of cleanup-eligible execution records.

        This method is intentionally public to make watcher behavior deterministic
        in unit tests.
        """
        for record in self._execution_registry.snapshot():
            self._process_record(record)

    def _process_record(self, record: ExecutionRecord | None) -> None:
        if record is None:
            return

        if not self._is_cleanup_candidate(record):
            return

        try:
            self._delete_local_workspace(record)
            self._remove_execution_record_and_state(record)
        except Exception:
            logger.exception(
                "Execution record cleanup failed; terminal record will remain eligible for retry.",
                extra={
                    "component": "cleanup_watcher",
                    "event": "execution_record_cleanup_failed",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                    "localPath": str(record.workspace.work_dir),
                },
            )

    def _is_cleanup_candidate(self, record: ExecutionRecord) -> bool:
        state = self._job_state_transition_coordinator.get_state(record.job_id)
        return state in CLEANUP_ELIGIBLE_STATES

    def _delete_local_workspace(self, record: ExecutionRecord) -> None:
        work_dir = Path(record.workspace.work_dir)

        if not work_dir.exists():
            logger.info(
                "Local job workspace is already absent.",
                extra={
                    "component": "cleanup_watcher",
                    "event": "local_job_workspace_already_absent",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                    "localPath": str(work_dir),
                },
            )
            return

        if not work_dir.is_dir():
            raise ValueError(
                "Local job workspace path is not a directory: "
                f"jobId='{record.job_id}', workDir='{work_dir}'."
            )

        shutil.rmtree(work_dir)

        logger.info(
            "Local job workspace deleted.",
            extra={
                "component": "cleanup_watcher",
                "event": "local_job_workspace_deleted",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "localPath": str(work_dir),
            },
        )

    def _remove_execution_record_and_state(self, record: ExecutionRecord) -> None:
        removed = self._execution_registry.remove_if_same(record)

        if not removed:
            logger.warning(
                "Cleanup-eligible execution record was not removed because registry entry changed or disappeared.",
                extra={
                    "component": "cleanup_watcher",
                    "event": "cleanup_eligible_record_remove_skipped",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                    "localPath": str(record.workspace.work_dir),
                },
            )
            return

        state_removed = self._job_state_transition_coordinator.remove_if_state(
            job_id=record.job_id,
            expected_states=CLEANUP_ELIGIBLE_STATES,
        )

        if not state_removed:
            logger.warning(
                "Cleanup-eligible worker-local state was not removed because state changed or disappeared.",
                extra={
                    "component": "cleanup_watcher",
                    "event": "cleanup_eligible_state_remove_skipped",
                    "jobId": record.job_id,
                    "userId": record.user_id,
                    "jobType": record.job_type,
                    "workerId": record.worker_id,
                    "localPath": str(record.workspace.work_dir),
                },
            )

        logger.info(
            "Cleanup-eligible execution record removed.",
            extra={
                "component": "cleanup_watcher",
                "event": "cleanup_eligible_record_removed",
                "jobId": record.job_id,
                "userId": record.user_id,
                "jobType": record.job_type,
                "workerId": record.worker_id,
                "localPath": str(record.workspace.work_dir),
                "stateRemoved": state_removed,
            },
        )

    def _run_loop(self) -> None:
        while not self._stop_requested.is_set():
            try:
                self.poll_once()
            except Exception:
                logger.exception(
                    "Cleanup watcher polling iteration failed.",
                    extra={
                        "component": "cleanup_watcher",
                        "event": "cleanup_watcher_poll_failed",
                        "workerId": self._worker_id,
                    },
                )

            self._stop_requested.wait(self._cleanup_interval_seconds)
