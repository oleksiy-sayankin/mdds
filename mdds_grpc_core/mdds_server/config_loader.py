# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import os
from typing import Dict, Any

import yaml
from threading import Lock


class Config:
    _instance = None
    _data: Dict[str, Any] = {}
    _lock = Lock()

    def __new__(cls, config_path=None):
        with cls._lock:
            if cls._instance is None:
                if config_path is None:
                    # Default path inside server package
                    config_path = os.path.join(
                        os.path.dirname(__file__), "conf", "server_config.yaml"
                    )
                with open(config_path, "r", encoding="utf-8") as f:
                    cls._instance = super(Config, cls).__new__(cls)
                    cls._instance._data = yaml.safe_load(f)
            return cls._instance

    def get(self, *keys, default=None):
        """Get nested config values with fallback"""
        data = self._data
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return default
        return data
