# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JobMessageDTO:
    """Submitted job message published by the Java orchestrator.

    This DTO mirrors the Java JobMessageDTO wire contract.
    The JSON field name is intentionally camelCase: manifestObjectKey.
    """

    manifestObjectKey: str

    def __post_init__(self) -> None:
        if self.manifestObjectKey is None or self.manifestObjectKey.strip() == "":
            raise ValueError("manifestObjectKey cannot be null or blank.")

    @property
    def manifest_object_key(self) -> str:
        """Return manifest object key using Python naming style."""
        return self.manifestObjectKey


@dataclass(frozen=True)
class JobStatusUpdateDTO:
    """
    Job status update message published by the Python Worker Runtime.

    This DTO mirrors the Java JobStatusUpdateDTO wire contract consumed by
    the Web Server status update pipeline.
    """

    jobId: str
    workerId: str
    status: str
    progress: int
    message: str
    eventTime: str

    @property
    def job_id(self) -> str:
        return self.jobId

    @property
    def worker_id(self) -> str:
        return self.workerId

    @property
    def event_time(self) -> str:
        return self.eventTime


@dataclass(frozen=True)
class CancelJobDTO:
    """Cancellation request message consumed by Worker Runtime.

    The Web Server sends this message to a worker-specific cancellation queue.
    The message identifies the job requested for cancellation.

    The DTO only represents the transport payload. Actual cancellation execution
    is handled by Worker Runtime lifecycle services.
    """

    jobId: str

    @property
    def job_id(self) -> str:
        return self.jobId
