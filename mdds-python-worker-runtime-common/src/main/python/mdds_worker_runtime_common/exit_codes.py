# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import signal
from enum import IntEnum


class WorkerExitCode(IntEnum):
    SUCCESS = 0
    EXECUTION_FAILED = 1
    WORKER_CONTRACT_VIOLATION = 2
    SIGTERM_EXIT_CODE = 128 + signal.SIGTERM  # 143
