import asyncio
import logging
import os
from pathlib import Path
from typing import Annotated, Self

import aiofiles
from aiobotocore.session import get_session
from pydantic import BaseModel, Field
from types_aiobotocore_s3 import S3Client

logger = logging.getLogger(__name__)


class Artifact(BaseModel):
    name: Annotated[str, Field(min_length=1, strict=True)]
    path: Annotated[str, Field(min_length=1, strict=True)]


class ManifestConfiguration(BaseModel):
    bucket: Annotated[str, Field(min_length=1, strict=True)]
    artifact_prefix: Annotated[str, Field(strict=True)] = ""
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
                bucket=os.environ["S3_BUCKET"],
                artifact_prefix=os.environ.get("S3_ARTIFACT_PREFIX", ""),
            ),
            artifacts=artifacts,
        )

    def get_sync(self) -> GetManifestResult:
        return asyncio.run(self.get())

    def store_sync(self) -> StoreManifestResult:
        return asyncio.run(self.store())

    async def get(self) -> GetManifestResult:
        session = get_session()

        # Limit the number of concurrent S3 downloads to avoid overwhelming the
        # network, exhausting file descriptors, or hitting connection limits. Without
        # this semaphore, starting all artifact downloads at once could degrade
        # performance or cause transient "Too many open connections" errors on large
        # manifests.
        #
        # This is still not the most efficient way to download a very large number of
        # artifacts, as all coroutines are created before starting the download, but
        # it should be sufficient for now.
        sem = asyncio.Semaphore(self.config.max_concurrent)

        async with session.create_client("s3") as client:

            async def get_artifact(artifact):
                async with sem:
                    await self._get_artifact(client, artifact)
                logger.debug("Downloaded %s", artifact.name)

            await asyncio.gather(*(get_artifact(a) for a in self.artifacts))
        return GetManifestResult()

    async def store(self) -> StoreManifestResult:
        session = get_session()
        sem = asyncio.Semaphore(self.config.max_concurrent)

        async with session.create_client("s3") as client:

            async def put_artifact(artifact):
                async with sem:
                    await self._put_artifact(client, artifact)
                logger.debug("Uploaded %s", artifact.name)

            await asyncio.gather(*(put_artifact(a) for a in self.artifacts))

        return StoreManifestResult()

    async def _get_artifact(self, client: S3Client, artifact: Artifact):
        artifact_key = f"{self.config.artifact_prefix}{artifact.name}"
        response = await client.get_object(Bucket=self.config.bucket, Key=artifact_key)

        Path(artifact.path).parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(artifact.path, "wb") as f:
            async for chunk in response["Body"].iter_chunks():
                await f.write(chunk)

    async def _put_artifact(self, client: S3Client, artifact: Artifact):
        artifact_key = f"{self.config.artifact_prefix}{artifact.name}"
        # aiobotocore does not support aiofiles handles
        # https://github.com/aio-libs/aiobotocore/issues/746
        with open(artifact.path, "rb") as f:  # noqa: ASYNC230
            await client.put_object(
                Bucket=self.config.bucket,
                Key=artifact_key,
                Body=f,
            )
