# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from enum import Enum


class UnknownArtifactFormatError(ValueError):
    pass


class ArtifactFormat(Enum):
    """Artifact format. This class is similar to the Java class."""

    CSV = "csv"
    JSON = "json"

    @classmethod
    def from_raw(cls, raw: str | None) -> "ArtifactFormat":
        if raw is None or raw.strip() == "":
            raise UnknownArtifactFormatError(
                "Artifact format must not be null or blank."
            )

        normalized = raw.strip().lower()
        for item in cls:
            if item.value == normalized:
                return item

        raise UnknownArtifactFormatError(
            f"Unknown or unsupported artifact format: '{raw}'."
        )
