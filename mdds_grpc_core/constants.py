# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from generated import solver_pb2


TaskStatus = solver_pb2.TaskStatus
DONE = int(TaskStatus.DONE)
ERROR = int(TaskStatus.ERROR)
CANCELLED = int(TaskStatus.CANCELLED)
IN_PROGRESS = int(TaskStatus.IN_PROGRESS)

TERMINAL = {DONE, ERROR, CANCELLED}

RequestStatus = solver_pb2.RequestStatus
DECLINED = RequestStatus.DECLINED
COMPLETED = RequestStatus.COMPLETED

JOB_TIMEOUT = 600  # in seconds
