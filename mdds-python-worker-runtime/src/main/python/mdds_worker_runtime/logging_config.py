# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

SERVICE_NAME = "mdds-python-worker-runtime"


class JsonLogFormatter(logging.Formatter):
    """Formats log records as JSON."""

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    @staticmethod
    def _ts_utc_z(record: logging.LogRecord) -> str:
        """Formats timestamp as UTC RFC3339-like value with milliseconds."""
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def format(self, record: logging.LogRecord) -> str:
        """Format logging record with JSON fields."""
        payload = {
            "ts": self._ts_utc_z(record),
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "thread": record.threadName,
            "process": record.processName,
            "pid": record.process,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        for field in (
            "userId",
            "jobId",
            "workerId",
            "event",
            "component",
            "status",
            "progress",
        ):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        return json.dumps(payload, ensure_ascii=False)


def setup_logging(
    service_name: str = SERVICE_NAME,
    log_file: str | None = None,
    level: int = logging.INFO,
) -> None:
    """Configure JSON logging.

    In production Docker/Kubernetes deployments, logs should normally be written
    to stdout only. File logging is intended for local debugging and tests.
    """
    formatter = JsonLogFormatter(service_name=service_name)

    handlers: list[logging.Handler] = []

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    if log_file is not None:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=20 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers, force=True)
