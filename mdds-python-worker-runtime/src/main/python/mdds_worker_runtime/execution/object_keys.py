# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from pathlib import PurePosixPath


def file_name_from_object_key(object_key: str) -> str:
    if object_key is None or object_key.strip() == "":
        raise ValueError("object_key cannot be null or blank.")

    if object_key.endswith("/"):
        raise ValueError(f"Object key has no file name: {object_key}")

    file_name = PurePosixPath(object_key).name

    if file_name in {"", ".", ".."}:
        raise ValueError(f"Object key has no valid file name: {object_key}")

    return file_name
