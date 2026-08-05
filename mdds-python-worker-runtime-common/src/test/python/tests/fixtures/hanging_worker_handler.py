# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import os
import time
from pathlib import Path

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.handler import WorkerHandler


class HangingWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> None:
        started_path = Path(context.params.required("startedPath"))
        temporary_path = started_path.with_suffix(".tmp")
        temporary_path.write_text(f"{os.getpid()}\n", encoding="utf-8")
        temporary_path.replace(started_path)

        while True:
            time.sleep(0.1)
