# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import multiprocessing as mp
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from multiprocessing.connection import Connection

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.queue.queue_client import Acknowledger


class WorkerJobStatus(Enum):
    VALIDATION_FAILED = "VALIDATION_FAILED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


@dataclass
class ExecutionRecord:
    job_id: str
    user_id: int
    job_type: str
    worker_id: str

    manifest_object_key: str
    manifest: JobManifest

    process: mp.Process
    parent_connection: Connection

    submitted_ack: Acknowledger

    started_at: datetime
    finished_at: datetime | None = None

    terminal_status: WorkerJobStatus | None = None
    terminal_message: str | None = None

    terminal_status_claimed: bool = False
    terminal_status_published: bool = False
    acknowledgement_done: bool = False
    cleanup_ready: bool = False

    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
