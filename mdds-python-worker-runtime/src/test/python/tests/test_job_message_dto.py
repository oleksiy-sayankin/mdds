# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from mdds_worker_runtime.dto.messages import JobMessageDTO


def test_job_message_dto_accepts_java_wire_field_name() -> None:
    message = JobMessageDTO(
        manifestObjectKey="jobs/42/job-1/manifest.json",
    )

    assert message.manifestObjectKey == "jobs/42/job-1/manifest.json"
    assert message.manifest_object_key == "jobs/42/job-1/manifest.json"


def test_job_message_dto_can_be_created_from_java_style_json_dict() -> None:
    raw = {
        "manifestObjectKey": "jobs/42/job-1/manifest.json",
    }

    message = JobMessageDTO(**raw)

    assert message.manifestObjectKey == "jobs/42/job-1/manifest.json"
    assert message.manifest_object_key == "jobs/42/job-1/manifest.json"


@pytest.mark.parametrize("manifest_object_key", [None, "", " ", "\t"])
def test_job_message_dto_rejects_null_or_blank_manifest_object_key(
    manifest_object_key: str | None,
) -> None:
    with pytest.raises(ValueError, match="manifestObjectKey cannot be null or blank"):
        JobMessageDTO(manifestObjectKey=manifest_object_key)


def test_job_message_dto_is_immutable() -> None:
    message = JobMessageDTO(
        manifestObjectKey="jobs/42/job-1/manifest.json",
    )

    with pytest.raises(FrozenInstanceError):
        setattr(message, "manifestObjectKey", "jobs/42/job-2/manifest.json")
