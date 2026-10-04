"""OpenSidewalks NYC: OpenSidewalks v0.3-conformant NYC pedestrian graph pipeline."""

from importlib.metadata import PackageNotFoundError, version

try:
    # The one place the version is written is pyproject.toml. After changing
    # it, reinstall (uv pip install -e .) so the metadata follows.
    __version__ = version("opensidewalks-nyc")
except PackageNotFoundError:
    __version__ = "unknown"
