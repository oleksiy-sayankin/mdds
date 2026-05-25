# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.


class ManifestError(Exception):
    """Base class for manifest loading, validation and mapping errors."""

    def __init__(self, key: str, message: str):
        super().__init__(message)
        self.key = key
        self.message = message


class ManifestLoadingError(ManifestError):
    """Raised when manifest cannot be loaded or parsed from object storage."""

    def __init__(self, key: str, message: str, error_code: str | None = None):
        super().__init__(key, message)
        self.error_code = error_code


class ManifestSchemaValidationError(ManifestError):
    """Raised when raw manifest JSON does not match Manifest DTO schema."""


class ManifestDomainMappingError(ManifestError):
    """Raised when DTO cannot be converted into domain manifest."""
