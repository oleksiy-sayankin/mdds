# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import threading
import multiprocessing as mp

from dataclasses import dataclass, field
from multiprocessing.connection import Connection


@dataclass
class Job:
    """
    Job with multithreading process and its status.
    """

    process: mp.Process
    jobStatus: int
    solution: list[float]
    jobMessage: str
    startTime: float  # unix timestamp
    endTime: float | None
    connection: Connection
    delivered: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
