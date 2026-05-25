# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from mdds_worker_runtime.manifest.errors import (
    ManifestLoadingError,
    ManifestError,
    ManifestSchemaValidationError,
    ManifestDomainMappingError,
)


def test_manifest_errors_are_manifest_errors():
    # Just not to forget the superclass.
    assert issubclass(ManifestLoadingError, ManifestError)
    assert issubclass(ManifestSchemaValidationError, ManifestError)
    assert issubclass(ManifestDomainMappingError, ManifestError)
