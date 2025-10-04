# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Data Transfer Object for Task"""

from pydantic import BaseModel
from typing import List
from datetime import datetime


class TaskDTO(BaseModel):
    task_id: str
    date_time_created: datetime
    matrix: List[List[float]]
    rhs: List[float]
    slae_solving_method: str
