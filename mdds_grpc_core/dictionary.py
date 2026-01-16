# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import threading


class ThreadSafeDictionary:
    """
    Thread safe dictionary
    """

    def __init__(self):
        self._dict = {}
        # The lock for synchronization
        self._lock = threading.Lock()

    def get(self, key, default=None):
        with self._lock:
            return self._dict.get(key, default)

    def put(self, key, value):
        with self._lock:
            self._dict[key] = value

    def pop(self, key, default=None):
        with self._lock:
            return self._dict.pop(key, default)

    def size(self):
        with self._lock:
            return len(self._dict)
