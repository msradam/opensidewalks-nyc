"""Provenance tracking utilities.

Every feature written to staged/output carries:
  ext:source            . Source_id from sources.yaml
  ext:source_timestamp  . ISO-8601 datetime when that source was retrieved
  ext:pipeline_version  . Git SHA of the pipeline commit that produced this run

The manifest at data/raw/manifest.json records retrieval timestamps per source.
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def get_git_sha() -> str:
    """Return the current HEAD git SHA, or 'unknown' if not in a git repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def load_manifest(raw_dir: Path) -> dict:
    """Load the raw data manifest, or return empty dict if not yet created."""
    manifest_path = raw_dir / "manifest.json"
    if manifest_path.exists():
        return json.loads(manifest_path.read_text())
    return {}


def save_manifest(raw_dir: Path, manifest: dict) -> None:
    manifest_path = raw_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))


def record_source(manifest: dict, source_id: str, file_path: str,
                  content_hash: str, row_count: int | None = None) -> None:
    """Record a successful source retrieval in the manifest."""
    manifest[source_id] = {
        "file_path": file_path,
        "content_hash": content_hash,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
    }


def provenance_fields(source_id: str, manifest: dict, pipeline_version: str) -> dict:
    """Return the three ext: provenance fields for a feature."""
    source_ts = manifest.get(source_id, {}).get("retrieved_at", "unknown")
    return {
        "ext:source": source_id,
        "ext:source_timestamp": source_ts,
        "ext:pipeline_version": pipeline_version,
    }
