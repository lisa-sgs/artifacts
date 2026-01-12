import logging
import os
from pathlib import Path
from typing import Annotated, Self

import boto3
from pydantic import BaseModel, Field
from types_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


class Artifact(BaseModel):
    name: Annotated[str, Field(min_length=1, strict=True)]
    path: Annotated[str, Field(min_length=1, strict=True)]


class ManifestConfiguration(BaseModel):
    bucket: Annotated[str, Field(min_length=1, strict=True)]
    remote_prefix: Annotated[str, Field(strict=True)] = ""
    local_prefix: Annotated[str, Field(strict=True)] = ""
    max_concurrent: Annotated[int, Field(strict=True)] = 50


class GetManifestResult(BaseModel): ...


class StoreManifestResult(BaseModel): ...


class Manifest(BaseModel):
    config: Annotated[ManifestConfiguration, Field()]
    artifacts: Annotated[list[Artifact], Field(strict=True)]

    @classmethod
    def from_env(cls, artifacts: list[Artifact]) -> Self:
        return cls(
            config=ManifestConfiguration(
                bucket=os.environ["ARTIFACTS_BUCKET"],
                remote_prefix=os.environ.get("ARTIFACTS_REMOTE_PREFIX", ""),
                local_prefix=os.environ.get("ARTIFACTS_LOCAL_PREFIX", ""),
            ),
            artifacts=artifacts,
        )

    def get(self) -> GetManifestResult:
        client = boto3.client("s3")
        for i, a in enumerate(self.artifacts, start=1):
            logger.info(
                "Downloading artifact %d/%d: %s", i, len(self.artifacts), a.name
            )
            self.get_artifact(client, a)
        return GetManifestResult()

    def store(self) -> StoreManifestResult:
        client = boto3.client("s3")
        for i, a in enumerate(self.artifacts, start=1):
            logger.info("Storing artifact %d/%d: %s", i, len(self.artifacts), a.name)
            self.store_artifact(client, a)

        return StoreManifestResult()

    def get_artifact(self, client: S3Client, artifact: Artifact):
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
        artifact_key = f"{self.config.remote_prefix}{artifact.name}"
        local_path = Path(self.config.local_prefix) / artifact.path
        client.upload_file(
            Filename=str(local_path),
            Bucket=self.config.bucket,
            Key=artifact_key,
        )
        logger.debug("Uploaded %s to %s", local_path, artifact_key)
