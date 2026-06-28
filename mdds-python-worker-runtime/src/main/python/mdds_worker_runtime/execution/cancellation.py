# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations


class CancellationRequestHandler:
    """Accepts cancellation requests for local Worker Runtime processing.

    This class is the runtime boundary between cancellation message consumption
    and cancellation execution.

    The message consumer is responsible for receiving, validating, and
    acknowledging cancellation messages. This handler is responsible for
    accepting a validated jobId for local runtime processing.
    """

    def request_cancellation(self, job_id: str) -> None:
        """Accept cancellation request for a job.

        Concrete runtime behavior includes finding the local execution record,
        coordinating cancellation with the supervised process lifecycle, and
        finalizing the job as CANCELLED when cancellation succeeds.
        """
        raise NotImplementedError("Cancellation execution flow is not implemented yet.")
