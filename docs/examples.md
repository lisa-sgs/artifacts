# Examples

## Basic Upload and Download

```python
from lisa.artifacts import Artifact, Manifest

# Define artifacts
artifacts = [
    Artifact(name="some-signal.h5", path="data/signal.h5"),
    Artifact(name="config.json", path="config.json"),
]

# Create manifest from env
manifest = Manifest.from_env(artifacts)

# Upload
manifest.store()

# Download
manifest.get()
```

## Custom Configuration

```python
from lisa.artifacts import Artifact, Manifest, ManifestConfiguration

config = ManifestConfiguration(
    bucket="my-bucket",
    remote_prefix="v1/",
    local_prefix="./data/",
)

artifacts = [Artifact(name="data.csv", path="data.csv")]
manifest = Manifest(config=config, artifacts=artifacts)

manifest.get()
```
