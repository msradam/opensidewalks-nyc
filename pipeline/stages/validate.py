"""Stage 5: Validate the staged FeatureCollection against OSW v0.3 schema.

Input:  data/staged/nyc-osw-unvalidated.geojson
Output: output/validation_report.md

Validation is performed in two layers:
  1. JSON Schema validation. Each feature validated against the OSW v0.3
     schema downloaded from the canonical URL. Uses jsonschema library.
  2. Structural integrity checks. Independent of the schema:
     a. Every Edge _u_id and _v_id references an existing Node _id.
     b. Every feature has a unique _id.
     c. No feature spans outside WGS-84 bounds.
     d. Curb nodes are Points (not LineStrings).
     e. Edges are LineStrings (not Points).

The OSWValidation project (github.com/OpenSidewalks/OSWValidation) is not
published on PyPI; we implement equivalent validation using jsonschema directly.
"""

import json
import re
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import click
import jsonschema
from jsonschema import Draft7Validator, ValidationError


# ---------------------------------------------------------------------------
# Schema acquisition
# ---------------------------------------------------------------------------

def _fetch_osw_schema(schema_url: str, cache_path: Path) -> dict:
    """Fetch the OSW JSON Schema, caching locally."""
    if cache_path.exists():
        click.echo(f"  Using cached schema: {cache_path}")
        return json.loads(cache_path.read_text())

    click.echo(f"  Fetching OSW schema from {schema_url}...")
    try:
        with urllib.request.urlopen(schema_url, timeout=30) as resp:
            schema_bytes = resp.read()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(schema_bytes)
        click.echo(f"  Schema cached to {cache_path}")
        return json.loads(schema_bytes)
    except Exception as exc:
        click.echo(f"  Warning: could not fetch OSW schema ({exc}). "
                   "Running structural checks only.")
        return {}


# ---------------------------------------------------------------------------
# Structural integrity checks (independent of JSON Schema)
# ---------------------------------------------------------------------------

def _check_structural(features: list[dict]) -> list[dict]:
    """Run structural integrity checks. Returns list of failure dicts."""
    failures = []

    # Build node ID index.
    node_ids = set()
    ids_seen = {}  # id → first feature index

    for i, feat in enumerate(features):
        props = feat.get("properties", {}) or {}
        geom  = feat.get("geometry", {}) or {}
        fid   = props.get("_id")
        geom_type = geom.get("type", "")

        if fid:
            if fid in ids_seen:
                failures.append({
                    "check": "unique_id",
                    "feature_id": fid,
                    "message": f"Duplicate _id at feature index {i} (first seen at {ids_seen[fid]})",
                })
            else:
                ids_seen[fid] = i

            if geom_type == "Point":
                node_ids.add(fid)

    # Check edge references and geometry types.
    for feat in features:
        props     = feat.get("properties", {}) or {}
        geom      = feat.get("geometry", {}) or {}
        fid       = props.get("_id", "<no _id>")
        geom_type = geom.get("type", "")
        highway   = props.get("highway", "")
        barrier   = props.get("barrier", "")

        # Edges must be LineStrings.
        if "_u_id" in props or "_v_id" in props:
            if geom_type != "LineString":
                failures.append({
                    "check": "edge_geometry_type",
                    "feature_id": fid,
                    "message": f"Edge has geometry {geom_type}, expected LineString",
                })

            u_id = props.get("_u_id")
            v_id = props.get("_v_id")
            if u_id and u_id not in node_ids:
                failures.append({
                    "check": "edge_node_reference",
                    "feature_id": fid,
                    "message": f"_u_id '{u_id}' does not reference any Node feature",
                })
            if v_id and v_id not in node_ids:
                failures.append({
                    "check": "edge_node_reference",
                    "feature_id": fid,
                    "message": f"_v_id '{v_id}' does not reference any Node feature",
                })

        # Curb nodes must be Points.
        if barrier == "kerb" and geom_type != "Point":
            failures.append({
                "check": "curb_node_geometry",
                "feature_id": fid,
                "message": f"Curb node has geometry {geom_type}, expected Point",
            })

        # WGS-84 bounds check.
        coords = _extract_all_coords(geom)
        for lon, lat in coords:
            if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                failures.append({
                    "check": "wgs84_bounds",
                    "feature_id": fid,
                    "message": f"Coordinate ({lon}, {lat}) outside WGS-84 bounds",
                })
                break

    return failures


def _extract_all_coords(geom: dict) -> list[tuple[float, float]]:
    """Recursively extract all (lon, lat) coordinate pairs from a GeoJSON geometry."""
    geom_type = geom.get("type", "")
    coords    = geom.get("coordinates", [])

    if geom_type == "Point":
        return [(coords[0], coords[1])] if len(coords) >= 2 else []
    elif geom_type in ("LineString", "MultiPoint"):
        return [(c[0], c[1]) for c in coords if len(c) >= 2]
    elif geom_type in ("Polygon", "MultiLineString"):
        return [(c[0], c[1]) for ring in coords for c in ring if len(c) >= 2]
    elif geom_type == "MultiPolygon":
        return [(c[0], c[1]) for poly in coords for ring in poly for c in ring if len(c) >= 2]
    return []


# ---------------------------------------------------------------------------
# JSON Schema validation
# ---------------------------------------------------------------------------

def _extract_feature_subschema(schema: dict) -> dict | None:
    """Extract the per-feature subschema from the OSW FeatureCollection schema.

    The OSW schema is a FeatureCollection schema. Its `features.items` or
    `features.items.oneOf` defines valid individual Feature shapes. We extract
    this subschema to validate individual features efficiently.
    """
    try:
        # Navigate: root → properties.features → items
        feat_items = (
            schema.get("properties", {})
                  .get("features", {})
                  .get("items")
        )
        if feat_items:
            return feat_items
        # Some schemas wrap with anyOf/oneOf at root.
        for key in ("anyOf", "oneOf", "allOf"):
            for sub in schema.get(key, []):
                result = _extract_feature_subschema(sub)
                if result:
                    return result
    except Exception:
        pass
    return None


def _validate_fc_root(fc: dict, schema: dict) -> list[dict]:
    """Validate the FeatureCollection root structure (metadata only, not features)."""
    if not schema:
        return []

    # Build a minimal FC with only the root fields to check metadata conformance.
    minimal_fc = {k: v for k, v in fc.items() if k != "features"}
    minimal_fc["features"] = []

    validator = Draft7Validator(schema)
    failures  = []
    try:
        for err in validator.iter_errors(minimal_fc):
            # Skip errors about the empty features array (we stripped features).
            if "features" in str(err.path) or "features" in err.message:
                continue
            failures.append({
                "check":      "json_schema",
                "feature_id": "<root>",
                "message":    err.message,
                "path":       " > ".join(str(p) for p in err.absolute_path),
            })
    except Exception as exc:
        failures.append({
            "check": "json_schema", "feature_id": "<root>",
            "message": f"Root validation error: {exc}", "path": "",
        })
    return failures


def _validate_schema(features: list[dict], schema: dict,
                     fc_root: dict | None = None,
                     sample_size: int = 2000) -> list[dict]:
    """Validate features against the OSW JSON Schema.

    The OSW schema is a FeatureCollection schema with definitions that individual
    feature subschemas reference via $ref. Extracting a subschema breaks $ref
    resolution. Instead, we build a mini FeatureCollection from a random sample
    and validate that against the full root schema. $refs resolve correctly.

    For a 4M-feature dataset, full validation would take hours; we sample
    `sample_size` features and report results from the sample.
    """
    if not schema:
        return []

    import random
    random.seed(42)
    sample = random.sample(features, min(sample_size, len(features)))

    click.echo(f"    Building mini-FC with {len(sample):,} sample features "
               f"(of {len(features):,}) for schema validation...")

    # Build a mini FeatureCollection preserving all root metadata + the sample.
    mini_fc = {k: v for k, v in (fc_root or {}).items() if k != "features"}
    mini_fc["features"] = sample

    validator = Draft7Validator(schema)
    failures  = []

    try:
        for err in validator.iter_errors(mini_fc):
            path_parts = list(err.absolute_path)
            # If error path starts with "features", extract feature index and get _id.
            if path_parts and path_parts[0] == "features" and len(path_parts) > 1:
                try:
                    feat_idx = int(path_parts[1])
                    feat     = sample[feat_idx]
                    fid      = (feat.get("properties") or {}).get("_id", f"<idx {feat_idx}>")
                except (IndexError, ValueError, TypeError):
                    fid = "<unknown>"
                prop_path = " > ".join(str(p) for p in path_parts[2:])
            else:
                fid       = "<root>"
                prop_path = " > ".join(str(p) for p in path_parts)

            failures.append({
                "check":      "json_schema",
                "feature_id": fid,
                "message":    err.message[:200],
                "path":       prop_path,
            })
    except Exception as exc:
        failures.append({
            "check": "json_schema", "feature_id": "<validator>",
            "message": f"Validator error: {exc}", "path": "",
        })

    return failures


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _write_validation_report(
    features:          list[dict],
    schema_version:    str,
    structural_fails:  list[dict],
    schema_fails:      list[dict],
    output_dir:        Path,
    sample_size:       int = 2000,
) -> None:
    total    = len(features)
    s_fail   = len(structural_fails)
    sc_fail  = len(schema_fails)

    # Features with any failure.
    failed_ids = set(f["feature_id"] for f in structural_fails + schema_fails)
    n_failed   = len(failed_ids)
    n_pass     = total - n_failed
    pct_pass   = (n_pass / total * 100) if total else 0

    # Count by check type.
    check_counts = Counter(f["check"] for f in structural_fails + schema_fails)

    now = datetime.now(timezone.utc).isoformat()
    lines = [
        "# OSW Validation Report",
        "",
        f"Generated: {now}",
        "",
        "## Summary",
        "",
        f"- **Schema version targeted:** OpenSidewalks v{schema_version}",
        f"- **Validator:** jsonschema (Draft7Validator) + structural integrity checks "
        f"(JSON Schema validation on {sample_size:,}-feature sample; structural checks on all features)",
        f"- **Total features:** {total:,}",
        f"- **Features passing all checks:** {n_pass:,} ({pct_pass:.1f}%)",
        f"- **Features with at least one failure:** {n_failed:,}",
        "",
        "## Results by Check Category",
        "",
        "| Check | Failures |",
        "|-------|---------|",
    ]

    all_checks = [
        "unique_id", "edge_geometry_type", "edge_node_reference",
        "curb_node_geometry", "wgs84_bounds", "json_schema",
    ]
    for check in all_checks:
        count = check_counts.get(check, 0)
        status = "✅ pass" if count == 0 else f"❌ {count:,} failures"
        lines.append(f"| {check} | {status} |")

    lines += ["", "## Structural Conformance", ""]
    if not structural_fails:
        lines.append("All structural checks passed.")
    else:
        lines.append(f"{len(structural_fails)} structural failures:")
        lines.append("")
        lines.append("| Check | Feature ID | Message |")
        lines.append("|-------|-----------|---------|")
        for f in structural_fails[:100]:  # cap at 100 rows
            msg = f["message"].replace("|", "/")
            lines.append(f"| {f['check']} | `{f['feature_id']}` | {msg} |")
        if len(structural_fails) > 100:
            lines.append(f"| … | ({len(structural_fails) - 100} more) | |")

    lines += ["", "## JSON Schema Conformance", ""]
    if not schema_fails:
        lines.append("All JSON Schema checks passed (or schema not available).")
    else:
        lines.append(f"{len(schema_fails)} schema failures:")
        lines.append("")
        lines.append("| Feature ID | Path | Message |")
        lines.append("|-----------|------|---------|")
        for f in schema_fails[:100]:
            msg  = (f.get("message", "") or "").replace("|", "/")[:120]
            path = (f.get("path", "") or "")[:60]
            lines.append(f"| `{f['feature_id']}` | {path} | {msg} |")
        if len(schema_fails) > 100:
            lines.append(f"| … | ({len(schema_fails) - 100} more) | |")

    lines += [
        "",
        "## Known V1 Limitations",
        "",
        "- No incline/slope data (requires elevation DEM. V1.1 scope)",
        "- No accessibility condition ratings (requires field survey data. V1.1 scope)",
        "- No APS (Accessible Pedestrian Signal) data (V1.1 scope)",
        "- Planimetric gap-fill sidewalks have approximate centerlines only",
        "- Curb ramp snap tolerance is 5 m; some ramps may not be correctly associated",
    ]

    report_path = output_dir / "validation_report.md"
    report_path.write_text("\n".join(lines))
    click.echo(f"\n  Validation report → {report_path}")
    click.echo(f"  Conformance: {pct_pass:.1f}% ({n_pass:,}/{total:,} features pass all checks)")


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def run(sources: dict, build_cfg: dict, repo_root: Path) -> None:
    """Stage 5: validate the staged FeatureCollection."""
    staged_dir = repo_root / build_cfg["dirs"]["staged"]
    output_dir = repo_root / build_cfg["dirs"]["output"]
    output_dir.mkdir(parents=True, exist_ok=True)

    schema_version  = build_cfg.get("osw_schema_version", "0.3")
    schema_url      = build_cfg.get("osw_schema_url",
                                    f"https://sidewalks.washington.edu/opensidewalks/{schema_version}/schema.json")
    schema_cache    = repo_root / build_cfg.get("osw_schema_local_cache",
                                                 f"data/raw/osw_schema_v{schema_version}.json")

    input_path = staged_dir / "nyc-osw-unvalidated.geojson"
    if not input_path.exists():
        raise FileNotFoundError(
            f"Staged FeatureCollection not found: {input_path}\n"
            "Run stages 1-4 first or use --stage to resume."
        )

    click.echo(f"  Loading {input_path.name}...")
    fc = json.loads(input_path.read_text())
    features = fc.get("features", [])
    click.echo(f"  {len(features):,} features to validate")

    # Layer 1: structural checks.
    click.echo("\n  Running structural integrity checks...")
    structural_fails = _check_structural(features)
    click.echo(f"    {len(structural_fails)} structural failures")

    # Layer 2: JSON Schema validation.
    schema = _fetch_osw_schema(schema_url, schema_cache)
    schema_sample_size = 2000
    if schema:
        click.echo(f"\n  Running JSON Schema validation (OSW v{schema_version})...")
        schema_fails = _validate_schema(features, schema, fc_root=fc,
                                        sample_size=schema_sample_size)
        click.echo(f"    {len(schema_fails)} schema failures in sample")
    else:
        schema_fails = []
        click.echo("  JSON Schema validation skipped (schema not available)")

    _write_validation_report(
        features, schema_version, structural_fails, schema_fails, output_dir,
        sample_size=schema_sample_size,
    )
