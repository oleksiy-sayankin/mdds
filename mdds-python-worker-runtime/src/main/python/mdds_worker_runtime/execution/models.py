# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import multiprocessing as mp
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from multiprocessing.connection import Connection
from typing import TYPE_CHECKING

from mdds_worker_runtime.execution.workspace import JobWorkspace

if TYPE_CHECKING:
    from mdds_worker_runtime.execution.context import JobExecutionContext


class WorkerJobStatus(Enum):
    SUBMITTED = "SUBMITTED"
    INPUTS_PREPARED = "INPUTS_PREPARED"
    VALIDATED = "VALIDATED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in {
            WorkerJobStatus.DONE,
            WorkerJobStatus.ERROR,
            WorkerJobStatus.CANCELLED,
        }

    def can_switch_to(self, new_status: "WorkerJobStatus") -> bool:
        if new_status is None:
            return False

        match self:
            case WorkerJobStatus.SUBMITTED:
                return new_status in {
                    WorkerJobStatus.INPUTS_PREPARED,
                    WorkerJobStatus.ERROR,
                }
            case WorkerJobStatus.INPUTS_PREPARED:
                return new_status in {
                    WorkerJobStatus.VALIDATED,
                    WorkerJobStatus.ERROR,
                }
            case WorkerJobStatus.VALIDATED:
                return new_status in {
                    WorkerJobStatus.IN_PROGRESS,
                    WorkerJobStatus.ERROR,
                }
            case WorkerJobStatus.IN_PROGRESS:
                return new_status in {
                    WorkerJobStatus.IN_PROGRESS,
                    WorkerJobStatus.DONE,
                    WorkerJobStatus.ERROR,
                    WorkerJobStatus.CANCELLED,
                }
            case (
                WorkerJobStatus.DONE | WorkerJobStatus.ERROR | WorkerJobStatus.CANCELLED
            ):
                return False


@dataclass
class ProcessRecord:
    process: mp.Process
    parent_connection: Connection
    started_at: datetime
    finished_at: datetime | None = None


@dataclass
class ExecutionRecord:
    workspace: JobWorkspace
    context: JobExecutionContext | None = None
    process_record: ProcessRecord | None = None

    @property
    def user_id(self) -> int:
        return self.workspace.user_id

    @property
    def job_id(self) -> str:
        return self.workspace.job_id

    @property
    def job_type(self) -> str:
        return self.workspace.job_type

    @property
    def worker_id(self) -> str:
        return self.workspace.worker_id

    @property
    def process(self) -> mp.Process:
        return self.process_record.process

    @property
    def parent_connection(self) -> Connection:
        return self.process_record.parent_connection

    @property
    def started_at(self) -> datetime:
        return self.process_record.started_at

    @property
    def finished_at(self) -> datetime | None:
        return self.process_record.finished_at

    @property
    def has_context(self) -> bool:
        return self.context is not None

    @property
    def has_started_process(self) -> bool:
        return self.process_record is not None

    @property
    def is_ready_for_execution_watchers(self) -> bool:
        return self.context is not None and self.process_record is not None
