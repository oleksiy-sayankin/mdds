# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from mdds_worker_runtime.execution.models import WorkerJobStatus

T = TypeVar("T")


class TransitionResultStatus(Enum):
    COMMITTED = "COMMITTED"
    STALE = "STALE"
    FAILED = "FAILED"


@dataclass(frozen=True)
class TransitionResult(Generic[T]):
    status: TransitionResultStatus
    value: T | None = None
    current_state: WorkerJobStatus | None = None
    target_state: WorkerJobStatus | None = None
    error: Exception | None = None

    @property
    def committed(self) -> bool:
        return self.status == TransitionResultStatus.COMMITTED

    @property
    def stale(self) -> bool:
        return self.status == TransitionResultStatus.STALE

    @property
    def failed(self) -> bool:
        return self.status == TransitionResultStatus.FAILED

    @classmethod
    def committed_result(
        cls,
        value: T | None,
        target_state: WorkerJobStatus,
    ) -> "TransitionResult[T]":
        return cls(
            status=TransitionResultStatus.COMMITTED,
            value=value,
            target_state=target_state,
            current_state=target_state,
        )

    @classmethod
    def stale_result(
        cls,
        current_state: WorkerJobStatus | None,
    ) -> "TransitionResult[T]":
        return cls(
            status=TransitionResultStatus.STALE,
            current_state=current_state,
        )

    @classmethod
    def failed_result(
        cls,
        current_state: WorkerJobStatus,
        error: Exception,
    ) -> "TransitionResult[T]":
        return cls(
            status=TransitionResultStatus.FAILED,
            current_state=current_state,
            error=error,
        )
