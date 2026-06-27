# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from enum import Enum


class WorkerStatus(str, Enum):
    """Lifecycle statuses that may be published by Python Worker Runtime."""

    VALIDATION_FAILED = "VALIDATION_FAILED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
