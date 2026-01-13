import logging
import os
from pathlib import Path
from typing import Annotated, Self

import boto3
from pydantic import BaseModel, Field
from types_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


class Artifact(BaseModel):
    """Represents an artifact with a name and local path.

    Attributes:
        name: The name of the artifact, used as the S3 key.
        path: The local file path relative to the local prefix.

    """

    name: Annotated[str, Field(min_length=1, strict=True)]
    path: Annotated[str, Field(min_length=1, strict=True)]


class ManifestConfiguration(BaseModel):
    """Configuration for manifest operations.

    Attributes:
        bucket: The S3 bucket name.
        remote_prefix: Prefix for S3 keys.
        local_prefix: Prefix for local paths.
        max_concurrent: Maximum concurrent operations (currently unused).

    """

    bucket: Annotated[str, Field(min_length=1, strict=True)]
    remote_prefix: Annotated[str, Field(strict=True)] = ""
    local_prefix: Annotated[str, Field(strict=True)] = ""
    max_concurrent: Annotated[int, Field(strict=True)] = 50


class GetManifestResult(BaseModel):
    """Result of a get operation. Currently empty."""


class StoreManifestResult(BaseModel):
    """Result of a store operation. Currently empty."""


class Manifest(BaseModel):
    """Manages a collection of artifacts for S3 operations.

    Attributes:
        config: Configuration for the manifest.
        artifacts: List of artifacts to manage.

    """

    config: Annotated[ManifestConfiguration, Field()]
    artifacts: Annotated[list[Artifact], Field(strict=True)]

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
            ),
            artifacts=artifacts,
        )

    def get(self) -> GetManifestResult:
        """Download all artifacts from S3.

        Returns:
            Result of the operation.

        """
        client = boto3.client("s3")
        for i, a in enumerate(self.artifacts, start=1):
            logger.info(
                "Downloading artifact %d/%d: %s", i, len(self.artifacts), a.name
            )
            self.get_artifact(client, a)
        return GetManifestResult()

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

    def get_artifact(self, client: S3Client, artifact: Artifact):
        """Download a single artifact from S3.

        Args:
            client: S3 client.
            artifact: The artifact to download.

        """
        artifact_key = f"{self.config.remote_prefix}{artifact.name}"
        local_path = Path(self.config.local_prefix) / artifact.path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(
            Bucket=self.config.bucket,
            Key=artifact_key,
            Filename=str(local_path),
        )
        logger.debug("Downloaded %s to %s", artifact_key, local_path)

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
