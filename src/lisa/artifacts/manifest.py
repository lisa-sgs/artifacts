import logging
import os
from enum import Enum
from pathlib import Path
from typing import Annotated, Self

import boto3
from pydantic import BaseModel, Field
from types_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


class LocalFilesPolicy(str, Enum):
    OVERWRITE = "overwrite"
    SKIP = "skip"


class Artifact(BaseModel):
    """Represents an artifact with a name and local path."""

    name: Annotated[str, Field(min_length=1, strict=True)]
    """The name of the artifact, used as the S3 key."""

    path: Annotated[str, Field(min_length=1, strict=True)]
    """The local file path relative to the local prefix."""


class ManifestConfiguration(BaseModel):
    """Configuration for manifest operations."""

    bucket: Annotated[str, Field(min_length=1, strict=True)]
    """The S3 bucket name."""

    remote_prefix: Annotated[str, Field(strict=True)] = ""
    """Prefix for S3 keys."""

    local_prefix: Annotated[str, Field(strict=True)] = ""
    """Prefix for local paths."""

    local_policy: Annotated[LocalFilesPolicy, Field(strict=True)] = (
        LocalFilesPolicy.OVERWRITE
    )
    """Behaviour when downloading files already present in the local filesystem."""


class GetManifestResult(BaseModel):
    """Result of a get operation."""

    file_locations: Annotated[dict[str, Path], Field(strict=True)]
    """Mapping of object keys to filesystem paths."""


class StoreManifestResult(BaseModel):
    """Result of a store operation. Currently empty."""


class Manifest(BaseModel):
    """Manages a collection of artifacts for S3 operations."""

    config: Annotated[ManifestConfiguration, Field()]
    """Configuration for the manifest."""

    artifacts: Annotated[list[Artifact], Field(strict=True)]
    """List of artifacts to manage."""

    @classmethod
    def from_env(cls, artifacts: list[Artifact]) -> Self:
        """Create a Manifest from environment variables.

        Args:
            artifacts: List of artifacts to include.

        Returns:
            A new Manifest instance.

        Raises:
            KeyError: If ARTIFACTS_BUCKET is not set.

        """
        return cls(
            config=ManifestConfiguration(
                bucket=os.environ["ARTIFACTS_BUCKET"],
                remote_prefix=os.environ.get("ARTIFACTS_REMOTE_PREFIX", ""),
                local_prefix=os.environ.get("ARTIFACTS_LOCAL_PREFIX", ""),
                local_policy=LocalFilesPolicy(
                    os.environ.get("ARTIFACTS_LOCAL_POLICY", LocalFilesPolicy.OVERWRITE)
                ),
            ),
            artifacts=artifacts,
        )

    def get(self) -> GetManifestResult:
        """Download all artifacts from S3.

        Returns:
            Result of the operation.

        """
        file_locations = {}
        client = boto3.client("s3")
        for i, a in enumerate(self.artifacts, start=1):
            logger.info(
                "Downloading artifact %d/%d: %s", i, len(self.artifacts), a.name
            )
            path = self.get_artifact(client, a)
            file_locations[a.name] = path
        return GetManifestResult(file_locations=file_locations)

    def store(self) -> StoreManifestResult:
        """Upload all artifacts to S3.

        Returns:
            Result of the operation.

        """
        client = boto3.client("s3")
        for i, a in enumerate(self.artifacts, start=1):
            logger.info("Storing artifact %d/%d: %s", i, len(self.artifacts), a.name)
            self.store_artifact(client, a)

        return StoreManifestResult()

    def get_artifact(self, client: S3Client, artifact: Artifact) -> Path:
        """Download a single artifact from S3.

        Args:
            client: S3 client.
            artifact: The artifact to download.

        """
        artifact_key = f"{self.config.remote_prefix}{artifact.name}"
        local_path = Path(self.config.local_prefix) / artifact.path
        if local_path.is_file():
            if self.config.local_policy == LocalFilesPolicy.SKIP:
                logger.info(
                    "Skipping %s, local file %s exists", artifact_key, local_path
                )
                return local_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(
            Bucket=self.config.bucket,
            Key=artifact_key,
            Filename=str(local_path),
        )
        logger.debug("Downloaded %s to %s", artifact_key, local_path)
        return local_path

    def store_artifact(self, client: S3Client, artifact: Artifact):
        """Upload a single artifact to S3.

        Args:
            client: S3 client.
            artifact: The artifact to upload.

        """
        artifact_key = f"{self.config.remote_prefix}{artifact.name}"
        local_path = Path(self.config.local_prefix) / artifact.path
        client.upload_file(
            Filename=str(local_path),
            Bucket=self.config.bucket,
            Key=artifact_key,
        )
        logger.debug("Uploaded %s to %s", local_path, artifact_key)
