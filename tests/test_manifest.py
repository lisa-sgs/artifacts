from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from lisa.artifacts.manifest import (
    Artifact,
    LocalFilesPolicy,
    Manifest,
    ManifestConfiguration,
)


@pytest.fixture
def sample_manifest(tmp_path: Path) -> Manifest:
    config = ManifestConfiguration(
        bucket="test_bucket",
        local_prefix=str(tmp_path),
        local_policy=LocalFilesPolicy.OVERWRITE,
    )
    artifact = Artifact(name="test", path="test_file")
    manifest = Manifest(artifacts=[artifact], config=config)
    return manifest


@pytest.fixture
def sample_manifest_multiple(tmp_path: Path) -> Manifest:
    config = ManifestConfiguration(
        bucket="test_bucket",
        local_prefix=str(tmp_path),
        local_policy=LocalFilesPolicy.OVERWRITE,
    )
    artifacts = [
        Artifact(name="test1", path="test_file1"),
        Artifact(name="test2", path="test_file2"),
    ]
    manifest = Manifest(artifacts=artifacts, config=config)
    return manifest


@pytest.fixture
def mock_boto3_client(monkeypatch: MonkeyPatch) -> Mock:
    mock_client = Mock()
    monkeypatch.setattr(
        "lisa.artifacts.manifest.boto3.client", Mock(return_value=mock_client)
    )
    return mock_client


def test_overwrite_policy_no_existing_file(
    mock_boto3_client: Mock, sample_manifest: Manifest, tmp_path: Path
):
    def mock_download(Bucket, Key, Filename):  # noqa: N803
        Path(Filename).write_text("downloaded")

    mock_boto3_client.download_file.side_effect = mock_download
    local_path = Path(tmp_path / "test_file")
    assert not local_path.exists()
    result = sample_manifest.get()
    assert local_path.exists()
    assert local_path.read_text() == "downloaded"
    mock_boto3_client.download_file.assert_called_once()
    assert result.file_locations == {"test": local_path}


def test_overwrite_policy_existing_file(
    mock_boto3_client: Mock, sample_manifest: Manifest, tmp_path: Path
):
    def mock_download(Bucket, Key, Filename):  # noqa: N803
        Path(Filename).write_text("downloaded")

    mock_boto3_client.download_file.side_effect = mock_download
    local_path = Path(tmp_path / "test_file")
    local_path.write_text("existing content")
    assert local_path.exists()
    result = sample_manifest.get()
    assert local_path.exists()
    assert local_path.read_text() == "downloaded"
    mock_boto3_client.download_file.assert_called_once()
    assert result.file_locations == {"test": local_path}


def test_skip_policy_no_existing_file(
    mock_boto3_client: Mock, sample_manifest: Manifest, tmp_path: Path
):
    def mock_download(Bucket, Key, Filename):  # noqa: N803
        Path(Filename).write_text("downloaded")

    mock_boto3_client.download_file.side_effect = mock_download
    sample_manifest.config.local_policy = LocalFilesPolicy.SKIP
    local_path = Path(tmp_path / "test_file")
    assert not local_path.exists()
    result = sample_manifest.get()
    assert local_path.exists()
    assert local_path.read_text() == "downloaded"
    mock_boto3_client.download_file.assert_called_once()
    assert result.file_locations == {"test": local_path}


def test_skip_policy_existing_file(
    mock_boto3_client: Mock, sample_manifest: Manifest, tmp_path: Path
):
    sample_manifest.config.local_policy = LocalFilesPolicy.SKIP
    local_path = Path(tmp_path / "test_file")
    local_path.write_text("existing content")
    assert local_path.exists()
    result = sample_manifest.get()
    assert local_path.exists()
    assert local_path.read_text() == "existing content"
    mock_boto3_client.download_file.assert_not_called()
    assert result.file_locations == {"test": local_path}


def test_invalid_policy(tmp_path: Path):
    with pytest.raises(
        ValidationError, match="Input should be an instance of LocalFilesPolicy"
    ):
        ManifestConfiguration(
            bucket="test_bucket",
            local_prefix=str(tmp_path),
            local_policy="invalid",  # type: ignore
        )


def test_empty_artifacts(mock_boto3_client: Mock):
    config = ManifestConfiguration(
        bucket="test_bucket", local_policy=LocalFilesPolicy.OVERWRITE
    )
    manifest = Manifest(artifacts=[], config=config)
    result = manifest.get()
    mock_boto3_client.download_file.assert_not_called()
    assert result.file_locations == {}


def test_download_error(mock_boto3_client: Mock, sample_manifest):
    mock_boto3_client.download_file.side_effect = Exception("Download failed")
    with pytest.raises(Exception, match="Download failed"):
        sample_manifest.get()


def test_get_file_locations_multiple_artifacts(
    mock_boto3_client: Mock, sample_manifest_multiple: Manifest, tmp_path: Path
):
    def mock_download(Bucket, Key, Filename):  # noqa: N803
        Path(Filename).write_text(f"downloaded {Path(Filename).name}")

    mock_boto3_client.download_file.side_effect = mock_download
    local_path1 = Path(tmp_path / "test_file1")
    local_path2 = Path(tmp_path / "test_file2")
    assert not local_path1.exists()
    assert not local_path2.exists()
    result = sample_manifest_multiple.get()
    assert local_path1.exists()
    assert local_path2.exists()
    assert local_path1.read_text() == "downloaded test_file1"
    assert local_path2.read_text() == "downloaded test_file2"
    assert mock_boto3_client.download_file.call_count == 2
    expected_locations = {
        "test1": local_path1,
        "test2": local_path2,
    }
    assert result.file_locations == expected_locations
