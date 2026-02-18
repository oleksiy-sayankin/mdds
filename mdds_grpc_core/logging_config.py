# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler


class JsonLogFormatter(logging.Formatter):
    """Extends standard log formatter to add log event with JSON fields."""

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    @staticmethod
    def _ts_utc_z(record: logging.LogRecord) -> str:
        """Utility method to format timestamp field with UTC timezone.
        The timestamp pattern is yyyy-MM-dd'T'HH:mm:ss.SSSXXX, e.g. 2026-02-18T16:59:14.044Z
        """
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def format(self, record: logging.LogRecord) -> str:
        """Format logging record with JSON fields."""
        payload = {
            "ts": self._ts_utc_z(record),
            "level": record.levelname,
            "service": self.service_name,
            "class": record.name,
            "thread": record.threadName,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        job_id = getattr(record, "job_id", None)
        if job_id:
            payload["job_id"] = job_id

        return json.dumps(payload, ensure_ascii=False)


def setup_logging() -> None:
    """Setup logging and use single service name for all modules and classes."""
    formatter = JsonLogFormatter(service_name="gRPC Server")

    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)

    fh = RotatingFileHandler(
        "mdds.log", maxBytes=20 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    fh.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO, handlers=[sh, fh], force=True)
