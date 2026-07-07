# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from dataclasses import dataclass
from threading import Lock

from mdds_worker_runtime.execution.models import WorkerJobStatus
from mdds_worker_runtime.queue.queue_client import Acknowledger


@dataclass
class WorkerJobStateRecord:
    lock: Lock
    state: WorkerJobStatus
    submitted_ack: Acknowledger
