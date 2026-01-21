# Getting Started

## Installation

Install the library using uv:

```bash
uv add lisa-artifacts
```

## Prerequisites

You need AWS credentials configured to access S3. This can be done via:

- AWS CLI: `aws configure`
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- IAM roles (for EC2/ECS)

## Configuration

Set the following environment variables:

- `ARTIFACTS_BUCKET`: The S3 bucket name.
- `ARTIFACTS_REMOTE_PREFIX` (optional): Prefix for S3 keys (e.g., `artifacts/`).
- `ARTIFACTS_LOCAL_PREFIX` (optional): Prefix for local paths (e.g., `./local/`).
- `ARTIFACTS_LOCAL_POLICY` (optional): Policy for handling local files during download. Acceptable values are defined in {py:class}`lisa.artifacts.manifest.LocalFilesPolicy`.

## Basic Usage

See [Examples](examples.md).
