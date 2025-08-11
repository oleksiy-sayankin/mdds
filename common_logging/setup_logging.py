# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import os
import yaml
import logging.config
from pathlib import Path


def setup_logging(
    default_path="./common_logging/logging_config.yaml", default_level=logging.INFO
):
    """
    Load logging configuration from YAML file and apply it globally.
    Creates logs directory if it doesn't exist.
    """
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    if os.path.exists(default_path):
        with open(default_path, "rt", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
        logging.warning(
            f"Logging configuration file {default_path} not found. Using basic config."
        )
