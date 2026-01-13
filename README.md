# LISA Artifacts

[![PyPI](https://img.shields.io/pypi/v/lisa-artifacts)](https://pypi.org/project/lisa-artifacts/)
[![CI](https://github.com/lisa-sgs/artifacts/actions/workflows/ci.yml/badge.svg)](https://github.com/lisa-sgs/artifacts/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/lisa-sgs/artifacts)](https://github.com/lisa-sgs/artifacts/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-latest-blue)](https://lisa-sgs.github.io/artifacts/)

A Python library for uploading and downloading artifacts to/from object storages using manifests.

## Installation

Install using uv:

```bash
uv add lisa-artifacts
```

## Quick Start

Set environment variables:

```bash
export ARTIFACTS_BUCKET=my-bucket
export ARTIFACTS_REMOTE_PREFIX=artifacts/ # optional
export ARTIFACTS_LOCAL_PREFIX=./local/ # optional
```

Create and use a manifest:

```python
from lisa.artifacts import Artifact, Manifest

artifacts = [
    Artifact(name="model.pkl", path="models/model.pkl"),
    Artifact(name="data.csv", path="data/data.csv"),
]

manifest = Manifest.from_env(artifacts)
manifest.get()  # Download artifacts
manifest.store()  # Upload artifacts
```
