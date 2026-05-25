# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import pytest

from mdds_worker_runtime.domain.artifact_format import (
    ArtifactFormat,
    UnknownArtifactFormatError,
)


def test_from_raw_csv():
    actual = ArtifactFormat.from_raw("csv")

    assert actual == ArtifactFormat.CSV


def test_from_raw_json():
    actual = ArtifactFormat.from_raw("json")

    assert actual == ArtifactFormat.JSON


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("csv", ArtifactFormat.CSV),
        ("CSV", ArtifactFormat.CSV),
        (" Csv ", ArtifactFormat.CSV),
        ("json", ArtifactFormat.JSON),
        ("JSON", ArtifactFormat.JSON),
        (" Json ", ArtifactFormat.JSON),
    ],
)
def test_from_raw_is_case_insensitive_and_trims(raw, expected):
    actual = ArtifactFormat.from_raw(raw)

    assert actual == expected


@pytest.mark.parametrize("raw", [None, "", " ", "\t", "\n"])
def test_from_raw_null_or_blank(raw):
    with pytest.raises(UnknownArtifactFormatError) as error:
        ArtifactFormat.from_raw(raw)

    assert str(error.value) == "Artifact format must not be null or blank."


@pytest.mark.parametrize("raw", ["xml", "txt", "parquet", "unsupported"])
def test_from_raw_unknown_format(raw):
    with pytest.raises(UnknownArtifactFormatError) as error:
        ArtifactFormat.from_raw(raw)

    assert str(error.value) == f"Unknown or unsupported artifact format: '{raw}'."
