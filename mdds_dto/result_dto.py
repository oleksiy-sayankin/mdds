# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Data Transfer Object for Result"""
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

from mdds_dto.task_status import TaskStatus


class ResultDTO(BaseModel):
    task_id: str
    date_time_task_created: datetime
    date_time_task_finished: datetime = None
    status: TaskStatus
    solution: Optional[List[float]] = None
    error_message: Optional[str] = None
