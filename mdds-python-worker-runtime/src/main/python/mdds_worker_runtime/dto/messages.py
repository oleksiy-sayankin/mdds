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
