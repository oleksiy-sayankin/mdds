# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import logging
from logging.handlers import RotatingFileHandler


def setup_logging():
    log_format = "%(asctime)s %(levelname)s [%(processName)s/%(threadName)s] %(name)s: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = []

    # log to file
    fh = RotatingFileHandler("mdds.log", maxBytes=20 * 1024 * 1024, backupCount=10)
    fh.setFormatter(logging.Formatter(fmt=log_format, datefmt=date_format))
    handlers.append(fh)

    # log to console
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter(fmt=log_format, datefmt=date_format))
    handlers.append(sh)

    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)
