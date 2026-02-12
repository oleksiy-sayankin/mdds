# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from generated import solver_pb2


JobStatus = solver_pb2.JobStatus
DONE = int(JobStatus.DONE)
ERROR = int(JobStatus.ERROR)
CANCELLED = int(JobStatus.CANCELLED)
IN_PROGRESS = int(JobStatus.IN_PROGRESS)

TERMINAL = {DONE, ERROR, CANCELLED}

RequestStatus = solver_pb2.RequestStatus
DECLINED = RequestStatus.DECLINED
COMPLETED = RequestStatus.COMPLETED

JOB_TIMEOUT = 600  # in seconds
RESULT_TIME_TO_LIVE = 300  # in seconds
MAX_MESSAGE_LENGTH = 1000 * 1024 * 1024
