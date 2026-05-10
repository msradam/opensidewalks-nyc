"""Stage 2: Clean and normalize all raw source data.

Input:  data/raw/{source_id}/
Output: data/clean/{source_id}.geojson
        data/clean/cleaning_report.md

Per-source operations:
  - Validate geometry (drop or repair invalid geometries)
  - Normalize CRS to EPSG:4326 (WGS-84)
  - Normalize attribute names to lowercase/underscore
  - Drop empty/null geometry rows
  - Report counts of dropped and repaired features
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import click
import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid

from pipeline.utils.provenance import load_manifest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Lowercase + strip column names; replace spaces/dashes with underscores."""
    gdf.columns = [
        c.lower().strip().replace(" ", "_").replace("-", "_")
        for c in gdf.columns
    ]
    return gdf


def _repair_geometries(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, int, int]:
    """Attempt to repair invalid geometries.

    Returns (cleaned_gdf, n_repaired, n_dropped) where n_dropped counts rows
    that had null geometry or could not be repaired to a valid non-empty geometry.
    """
    n_repaired = 0
    n_dropped  = 0
    keep_mask  = []

    for geom in gdf.geometry:
        if geom is None or geom.is_empty:
            keep_mask.append(False)
            n_dropped += 1
            continue
        if not geom.is_valid:
            fixed = make_valid(geom)
            if fixed is None or fixed.is_empty:
                keep_mask.append(False)
                n_dropped += 1
            else:
                keep_mask.append(True)
                n_repaired += 1
        else:
            keep_mask.append(True)

    # Apply the valid geometries back.
    gdf = gdf[keep_mask].copy()
    gdf.geometry = [
        make_valid(g) if not g.is_valid else g
        for g in gdf.geometry
    ]
    return gdf, n_repaired, n_dropped


def _ensure_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject to EPSG:4326 if necessary."""
    if gdf.crs is None:
        # Assume WGS-84 for data from NYC Open Data and OSMnx.
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _clip_to_bbox(gdf: gpd.GeoDataFrame, bbox: dict) -> gpd.GeoDataFrame:
    """Clip a GeoDataFrame to the study area bbox (belt-and-suspenders after acquire)."""
    from shapely.geometry import box as shapely_box
    rect = shapely_box(bbox["west"], bbox["south"], bbox["east"], bbox["north"])
    return gdf[gdf.geometry.intersects(rect)].copy()


def _clean_source(source_id: str, raw_file: Path,
                  clean_dir: Path, bbox: dict | None = None) -> dict:
    """Load, clean, and save one source. Returns a report dict."""
    report = {
        "source_id": source_id,
        "raw_file": str(raw_file),
        "n_raw": 0,
        "n_dropped_geom": 0,
        "n_repaired_geom": 0,
        "n_clean": 0,
        "notes": [],
    }

    click.echo(f"  Cleaning {source_id} from {raw_file.name}...")

    gdf = gpd.read_file(raw_file)

    # gpd.read_file() returns a plain DataFrame (not GeoDataFrame) when the file
    # has 0 features and no geometry column. Normalize to an empty GeoDataFrame.
    if not isinstance(gdf, gpd.GeoDataFrame) or "geometry" not in gdf.columns:
        gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries([], crs="EPSG:4326"))

    report["n_raw"] = len(gdf)
    click.echo(f"    Loaded {len(gdf)} features")

    # Normalize column names.
    gdf = _normalize_columns(gdf)

    # Drop rows with null geometry before reprojection.
    null_geom_mask = gdf.geometry.isna()
    n_null = null_geom_mask.sum()
    if n_null > 0:
        gdf = gdf[~null_geom_mask].copy()
        report["notes"].append(f"Dropped {n_null} rows with null geometry")

    # Ensure WGS-84.
    gdf = _ensure_wgs84(gdf)

    # Repair/drop invalid geometries.
    gdf, n_repaired, n_dropped = _repair_geometries(gdf)
    report["n_dropped_geom"] = n_dropped + n_null
    report["n_repaired_geom"] = n_repaired
    report["n_clean"] = len(gdf)

    if n_repaired:
        report["notes"].append(f"Repaired {n_repaired} invalid geometries with make_valid()")
    if n_dropped:
        report["notes"].append(f"Dropped {n_dropped} geometries that could not be repaired")

    # Source-specific attribute normalization.
    gdf = _source_specific_normalize(source_id, gdf, report)

    # Belt-and-suspenders bbox clip for study area mode.
    if bbox and len(gdf) > 0:
        before = len(gdf)
        gdf = _clip_to_bbox(gdf, bbox)
        clipped = before - len(gdf)
        if clipped:
            report["notes"].append(f"Clipped {clipped} features outside study area bbox")
        report["n_clean"] = len(gdf)

    out_file = clean_dir / f"{source_id}.geojson"
    gdf.to_file(out_file, driver="GeoJSON")
    click.echo(f"    {report['n_clean']} clean features → {out_file.name}")
    return report


def _source_specific_normalize(source_id: str, gdf: gpd.GeoDataFrame,
                                report: dict) -> gpd.GeoDataFrame:
    """Apply any source-specific normalization steps."""

    if source_id == "nyc_dot_ramps":
        # Replace sentinel "999.0" values (used for unmeasurable/missing data) with NaN.
        numeric_cols = gdf.select_dtypes(include="number").columns
        for col in numeric_cols:
            mask = gdf[col] == 999.0
            if mask.any():
                gdf.loc[mask, col] = None
        n_sentinels = sum((gdf[c] == 999.0).sum() for c in numeric_cols)
        if n_sentinels:
            report["notes"].append(
                f"Replaced {n_sentinels} sentinel 999.0 values with NaN in ramp measurements"
            )

        # Normalize borough column to string name.
        boro_map = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn",
                    "4": "Queens", "5": "Staten Island",
                    1: "Manhattan", 2: "Bronx", 3: "Brooklyn",
                    4: "Queens", 5: "Staten Island"}
        if "borough" in gdf.columns:
            gdf["borough"] = gdf["borough"].map(
                lambda x: boro_map.get(x, boro_map.get(str(int(float(x))) if x else x, str(x)))
            )

    elif source_id == "nyc_planimetric_sidewalks":
        if len(gdf) == 0:
            report["notes"].append("No features loaded. Check dataset ID in sources.yaml")
            return gdf

        # Ensure only Polygon/MultiPolygon geometry. Drop any Point/Line noise.
        before = len(gdf)
        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
        dropped = before - len(gdf)
        if dropped:
            report["notes"].append(
                f"Dropped {dropped} non-polygon features from planimetric layer"
            )

        if len(gdf) == 0:
            return gdf

        # Drop tiny slivers (< 1 m²) that are likely digitizing artifacts.
        gdf_proj = gdf.to_crs("EPSG:32618")  # UTM Zone 18N for NYC
        area_mask = gdf_proj.geometry.area >= 1.0
        dropped_slivers = (~area_mask).sum()
        if dropped_slivers:
            gdf = gdf[area_mask].copy()
            report["notes"].append(
                f"Dropped {dropped_slivers} slivers with area < 1 m²"
            )

    elif source_id == "nyc_boroughs":
        # Ensure we have a boro_name column.
        name_candidates = ["boro_name", "boroname", "name"]
        for col in name_candidates:
            if col in gdf.columns and "boro_name" not in gdf.columns:
                gdf = gdf.rename(columns={col: "boro_name"})
                break

    elif source_id == "osm_walk":
        # OSMnx edges may have list-valued columns (e.g. Osmid, highway).
        # Flatten lists to pipe-separated strings for GeoJSON compatibility.
        for col in gdf.columns:
            if col == "geometry":
                continue
            gdf[col] = gdf[col].apply(
                lambda x: "|".join(str(v) for v in x) if isinstance(x, list) else x
            )

    return gdf


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _write_cleaning_report(reports: list[dict], clean_dir: Path) -> None:
    lines = [
        "# Cleaning Report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Summary",
        "",
        "| Source | Raw features | Dropped (geom) | Repaired (geom) | Clean features |",
        "|--------|-------------|----------------|-----------------|----------------|",
    ]
    for r in reports:
        lines.append(
            f"| {r['source_id']} | {r['n_raw']} | {r['n_dropped_geom']} | "
            f"{r['n_repaired_geom']} | {r['n_clean']} |"
        )

    lines += ["", "## Notes by Source", ""]
    for r in reports:
        lines.append(f"### {r['source_id']}")
        for note in r["notes"]:
            lines.append(f"- {note}")
        if not r["notes"]:
            lines.append("- No issues")
        lines.append("")

    report_path = clean_dir / "cleaning_report.md"
    report_path.write_text("\n".join(lines))
    click.echo(f"  Cleaning report → {report_path}")


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def run(sources: dict, build_cfg: dict, repo_root: Path) -> None:
    """Stage 2: clean and normalize all raw source data."""
    raw_dir   = repo_root / build_cfg["dirs"]["raw"]
    clean_dir = repo_root / build_cfg["dirs"]["clean"]
    clean_dir.mkdir(parents=True, exist_ok=True)

    study_area = build_cfg.get("study_area")
    bbox = study_area["bbox"] if study_area else None

    manifest = load_manifest(raw_dir)
    reports  = []

    source_files = {
        "nyc_boroughs":               raw_dir / "nyc_boroughs"               / "boroughs.geojson",
        "osm_walk":                   raw_dir / "osm_walk"                   / "osm_edges.geojson",
        "nyc_dot_ramps":              raw_dir / "nyc_dot_ramps"              / "nyc_dot_ramps.geojson",
        "nyc_planimetric_sidewalks":  raw_dir / "nyc_planimetric_sidewalks"  / "nyc_planimetric_sidewalks.geojson",
    }

    mta_raw = raw_dir / "mta_ada_stations" / "mta_ada_stations.geojson"
    if mta_raw.exists():
        source_files["mta_ada_stations"] = mta_raw

    for source_id, raw_file in source_files.items():
        if not raw_file.exists():
            click.echo(f"  Warning: {raw_file} not found. Skipping {source_id}")
            continue
        report = _clean_source(source_id, raw_file, clean_dir, bbox=bbox)
        reports.append(report)

    # Also copy OSM nodes (used in assemble for graph topology).
    osm_nodes_raw = raw_dir / "osm_walk" / "osm_nodes.geojson"
    if osm_nodes_raw.exists():
        osm_nodes_clean = clean_dir / "osm_nodes.geojson"
        nodes_gdf = gpd.read_file(osm_nodes_raw)
        nodes_gdf = _normalize_columns(nodes_gdf)
        nodes_gdf = _ensure_wgs84(nodes_gdf)
        if bbox and len(nodes_gdf) > 0:
            nodes_gdf = _clip_to_bbox(nodes_gdf, bbox)
        nodes_gdf.to_file(osm_nodes_clean, driver="GeoJSON")
        click.echo(f"  OSM nodes: {len(nodes_gdf)} → {osm_nodes_clean.name}")

    _write_cleaning_report(reports, clean_dir)
