# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""List of possible results for a task"""

from enum import Enum


class TaskStatus(Enum):
    IN_PROGRESS = 1
    DONE = 2
    ERROR = 3
