"""Restore the v0.2-targeted artifact into a v0.3-conformant, routable graph.

This is a one-shot post-processor that fixes the structural issues identified
by the validators/quality_audit.py audit:

  - Root metadata: $schema -> 0.3 canonical id, dataSource/dataTimestamp/
    pipelineVersion updated, region populated (best-effort from feature bbox).
  - Drop self-loops (u == v).
  - Deduplicate edges by (sorted(u,v), highway, footway).
  - Aggressive node-merge: cluster endpoints within R metres into one
    canonical node id, rewrite all _u_id/_v_id refs, drop redundant nodes.
  - Canonicalize enums: surface, crossing:markings.
  - Stamp ext:source / ext:source_timestamp / ext:pipeline_version on every
    feature using best-effort heuristics.
  - Re-run the dedup/self-loop pass after node-merge (merging can create new ones).

Output:
  output/nyc-osw.geojson           Canonical OSW v0.3 FeatureCollection
  output/osw-split/                Split-by-type ZIP for python-osw-validation
  output/restore-report.json       What changed, with counts.

Inputs are parameterised so this script is reproducible.

Usage:
    uv run --python ~/ariadne-nyc/.venv/bin/python \
        scripts/restore_artifact.py \
        --input  /Users/amsrahman/macadam-nyc/opensidewalks_nyc.geojson \
        --output ./output/nyc-osw.geojson \
        --merge-tolerance-m 5.0
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.spatial import cKDTree
from shapely.geometry import LineString, MultiPolygon, Point, Polygon, shape
from shapely.ops import unary_union

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_ID  = "https://sidewalks.washington.edu/opensidewalks/0.3/schema.json"
SCHEMA_VER = "0.3"

PIPELINE_NAME = "opensidewalks-nyc restore_artifact"
PIPELINE_VER  = "0.3.0-nyc.1"
PIPELINE_URL  = "https://github.com/msradam/opensidewalks-nyc"

# OSW canonical enums (from upstream JSON Schema, v0.3)
SURFACE_CANONICAL = {"paved", "asphalt", "concrete", "paving_stones",
                     "gravel", "dirt", "grass", "wood", "metal"}
SURFACE_REMAP = {
    "unpaved":     "dirt",
    "grass_paver": "grass",
    "compacted":   "gravel",
    "fine_gravel": "gravel",
    "ground":      "dirt",
    "earth":       "dirt",
    "sand":        "dirt",
    "tarmac":      "asphalt",
    "cobblestone": "paving_stones",
    "sett":        "paving_stones",
    "bricks":      "paving_stones",
    "brick":       "paving_stones",
    "pebblestone": "gravel",
}
CROSSING_MARKINGS_CANONICAL = {"zebra", "lines", "dashes", "surface", "no"}
CROSSING_MARKINGS_REMAP = {
    "yes": "surface",   # OSM `crossing=marked` carries no specific style. Surface is the OSW conservative default for "yes there are markings".
    "marked": "surface",
    "unmarked": "no",
    "uncontrolled": "no",
}

NYC_BOROUGH_BBOX = {
    # Approximate boundaries; used to derive `region` MultiPolygon if no
    # source polygons are available. Coordinates are (W, S, E, N).
    "MN": (-74.020, 40.700, -73.907, 40.880),
    "BK": (-74.042, 40.566, -73.833, 40.739),
    "QN": (-73.962, 40.541, -73.700, 40.812),
    "BX": (-73.933, 40.785, -73.748, 40.917),
    "SI": (-74.259, 40.477, -74.052, 40.651),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _decimal_to_float(x):
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, dict):
        return {k: _decimal_to_float(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_decimal_to_float(v) for v in x]
    return x


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _heuristic_source(feat: dict) -> str:
    """Best-effort source attribution from feature properties."""
    props = feat.get("properties") or {}
    geom_type = (feat.get("geometry") or {}).get("type")
    if geom_type == "LineString":
        if props.get("ext:osm_id"):
            return "osm_walk"
        return "nyc_planimetric_sidewalks"
    elif geom_type == "Point":
        if props.get("barrier") == "kerb" or props.get("kerb") in {"lowered", "raised", "flush"}:
            return "nyc_dot_ramps"
        return "osm_walk"
    return "unknown"


def _canon_surface(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in SURFACE_CANONICAL:
        return s
    return SURFACE_REMAP.get(s, s)  # if unknown, return as-is so we can audit


def _canon_crossing_markings(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in CROSSING_MARKINGS_CANONICAL:
        return s
    return CROSSING_MARKINGS_REMAP.get(s, s)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load(input_path: Path) -> tuple[list, list, dict]:
    """Read the FeatureCollection, return (points, lines, root_meta)."""
    print(f"[load] reading {input_path.name}...", flush=True)
    t0 = time.time()
    with input_path.open() as f:
        fc = json.load(f, parse_float=float)
    points = []
    lines  = []
    for feat in fc["features"]:
        gt = (feat.get("geometry") or {}).get("type")
        if gt == "Point":
            points.append(feat)
        elif gt == "LineString":
            lines.append(feat)
    print(f"[load]   points={len(points):,} lines={len(lines):,} "
          f"({time.time()-t0:.1f}s)")
    root_meta = {k: v for k, v in fc.items() if k not in ("features", "type")}
    return points, lines, root_meta


# ---------------------------------------------------------------------------
# Drop pass 1: self-loops, geometry-degenerate edges
# ---------------------------------------------------------------------------

def drop_self_loops(lines: list, report: dict) -> list:
    kept, dropped = [], 0
    for f in lines:
        p = f.get("properties") or {}
        u, v = p.get("_u_id"), p.get("_v_id")
        coords = (f.get("geometry") or {}).get("coordinates", [])
        if u and v and u == v:
            dropped += 1; continue
        if len(coords) < 2:
            dropped += 1; continue
        if coords[0] == coords[-1] and len(coords) == 2:
            dropped += 1; continue
        kept.append(f)
    report["self_loops_dropped"] = dropped
    print(f"[drop] self-loops removed: {dropped:,}")
    return kept


# ---------------------------------------------------------------------------
# Dedup pass: same (sorted u,v, highway, footway) → keep first
# ---------------------------------------------------------------------------

def dedup_edges(lines: list, report: dict) -> list:
    seen = set()
    kept, removed = [], 0
    for f in lines:
        p = f.get("properties") or {}
        u, v = p.get("_u_id"), p.get("_v_id")
        if not u or not v:
            kept.append(f); continue
        key = (tuple(sorted([u, v])), p.get("highway"), p.get("footway"))
        if key in seen:
            removed += 1; continue
        seen.add(key)
        kept.append(f)
    report["edges_deduplicated"] = removed
    print(f"[dedup] duplicate edges removed: {removed:,}")
    return kept


# ---------------------------------------------------------------------------
# Aggressive node merge — cluster endpoints within R metres
# ---------------------------------------------------------------------------

def merge_nearby_nodes(points: list, lines: list, tol_m: float, report: dict):
    """Build a kdtree of node positions in metric CRS, find pairs within tol_m,
    union-find them into equivalence classes, rewrite all edges, drop redundant
    points. Picks the lexicographically-smallest _id as canonical per class."""
    print(f"[merge] aggressive node-merge with tol={tol_m} m...", flush=True)
    t0 = time.time()
    fwd = Transformer.from_crs("EPSG:4326", "EPSG:32618", always_xy=True)

    # Index nodes by _id
    node_idx = {}        # _id -> index
    coords_4326 = []
    for f in points:
        p = f.get("properties") or {}
        nid = p.get("_id")
        if not nid or nid in node_idx:
            continue
        c = (f.get("geometry") or {}).get("coordinates")
        if not c or len(c) < 2:
            continue
        node_idx[nid] = len(coords_4326)
        coords_4326.append((float(c[0]), float(c[1])))

    if not coords_4326:
        report["nodes_merged_into"] = 0
        return points, lines

    # Project to UTM 18N for metric distance
    xs, ys = fwd.transform([c[0] for c in coords_4326],
                           [c[1] for c in coords_4326])
    coords_utm = np.column_stack([xs, ys])

    # kdtree-based pairs within tol_m
    tree = cKDTree(coords_utm)
    pairs = tree.query_pairs(r=tol_m, output_type="ndarray")
    print(f"[merge]   found {len(pairs):,} candidate pairs within {tol_m} m")

    # Union-find
    parent = list(range(len(coords_utm)))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    for a, b in pairs:
        union(int(a), int(b))

    # Compose: for each cluster pick canonical _id (lowest _id by string)
    inv = {v: k for k, v in node_idx.items()}  # idx -> _id
    cluster_members = defaultdict(list)
    for idx in range(len(coords_utm)):
        cluster_members[find(idx)].append(idx)

    canonical_id = {}    # cluster_root_idx -> chosen _id
    id_remap = {}        # old _id -> canonical _id
    canonical_centroid = {}  # cluster_root_idx -> (lon, lat) avg
    for root, members in cluster_members.items():
        ids = sorted(inv[i] for i in members)
        cid = ids[0]
        canonical_id[root] = cid
        # Average position (still in 4326 lat/lon — safe for small clusters)
        lons = [coords_4326[i][0] for i in members]
        lats = [coords_4326[i][1] for i in members]
        canonical_centroid[root] = (sum(lons)/len(lons), sum(lats)/len(lats))
        for old_id in ids:
            id_remap[old_id] = cid

    n_merged = sum(1 for old, new in id_remap.items() if old != new)
    n_clusters = len(cluster_members)
    n_input = len(coords_4326)
    print(f"[merge]   {n_input:,} -> {n_clusters:,} unique nodes "
          f"({n_merged:,} ids remapped)")

    # Rewrite point features. Keep only canonical _id features; merge props from
    # all members (curb-ramp props win where present).
    grouped = defaultdict(list)
    for f in points:
        p = f.get("properties") or {}
        nid = p.get("_id")
        if not nid:
            continue
        canon = id_remap.get(nid, nid)
        grouped[canon].append(f)

    merged_points = []
    for canon, members in grouped.items():
        # Start from first; layer in non-null props from siblings.
        base = json.loads(json.dumps(members[0]))  # deep copy
        for sib in members[1:]:
            for k, v in (sib.get("properties") or {}).items():
                if v in (None, "", "null"):
                    continue
                if k not in (base.get("properties") or {}) or base["properties"].get(k) in (None, "", "null"):
                    base.setdefault("properties", {})[k] = v
        # Use cluster centroid for coordinates
        # Find the cluster root for this canon
        members_idx = [node_idx[m["properties"]["_id"]] for m in members
                       if m["properties"]["_id"] in node_idx]
        if members_idx:
            root = find(members_idx[0])
            cl, ca = canonical_centroid[root]
            base["geometry"] = {"type": "Point", "coordinates": [cl, ca]}
        base["properties"]["_id"] = canon
        merged_points.append(base)

    # Rewrite edge endpoints
    for f in lines:
        p = f.get("properties") or {}
        u, v = p.get("_u_id"), p.get("_v_id")
        if u in id_remap:
            p["_u_id"] = id_remap[u]
        if v in id_remap:
            p["_v_id"] = id_remap[v]

    report["nodes_in"]    = n_input
    report["nodes_out"]   = n_clusters
    report["nodes_merged"] = n_merged
    report["merge_tolerance_m"] = tol_m
    print(f"[merge]   done in {time.time()-t0:.1f}s")
    return merged_points, lines


# ---------------------------------------------------------------------------
# Enum canonicalization
# ---------------------------------------------------------------------------

def canonicalize_enums(points: list, lines: list, report: dict):
    surf_changes = Counter()
    cm_changes   = Counter()
    for f in lines:
        p = f.get("properties") or {}
        if "surface" in p and p["surface"] is not None:
            new = _canon_surface(p["surface"])
            if new != p["surface"]:
                surf_changes[(str(p["surface"]), new)] += 1
                p["surface"] = new
        if "crossing:markings" in p and p["crossing:markings"] is not None:
            new = _canon_crossing_markings(p["crossing:markings"])
            if new != p["crossing:markings"]:
                cm_changes[(str(p["crossing:markings"]), new)] += 1
                p["crossing:markings"] = new
    report["surface_remaps"]           = {f"{a}->{b}": c for (a, b), c in surf_changes.items()}
    report["crossing_markings_remaps"] = {f"{a}->{b}": c for (a, b), c in cm_changes.items()}
    print(f"[enum] surface remaps: {sum(surf_changes.values()):,}")
    print(f"[enum] crossing:markings remaps: {sum(cm_changes.values()):,}")
    return points, lines


# ---------------------------------------------------------------------------
# Provenance stamp
# ---------------------------------------------------------------------------

def stamp_provenance(points: list, lines: list, report: dict, src_ts: str):
    sc = Counter()
    for feat in points + lines:
        p = feat.setdefault("properties", {})
        if not p.get("ext:source"):
            p["ext:source"] = _heuristic_source(feat)
        if not p.get("ext:source_timestamp"):
            p["ext:source_timestamp"] = src_ts
        if not p.get("ext:pipeline_version"):
            p["ext:pipeline_version"] = PIPELINE_VER
        sc[p["ext:source"]] += 1
    report["provenance_stamped_by_source"] = dict(sc)
    print(f"[prov] stamped: {dict(sc)}")
    return points, lines


# ---------------------------------------------------------------------------
# Build region MultiPolygon from per-borough bboxes
# ---------------------------------------------------------------------------

def build_region() -> dict:
    polys = [Polygon([(w,s),(e,s),(e,n),(w,n),(w,s)])
             for (w,s,e,n) in NYC_BOROUGH_BBOX.values()]
    region = unary_union(polys)
    return json.loads(gpd.GeoSeries([region]).to_json())["features"][0]["geometry"]


# ---------------------------------------------------------------------------
# Topology metrics quick-check
# ---------------------------------------------------------------------------

def topology_quickcheck(points, lines, report: dict, label: str):
    import networkx as nx
    G = nx.Graph()
    for f in lines:
        p = f.get("properties") or {}
        u, v = p.get("_u_id"), p.get("_v_id")
        if u and v and u != v:
            G.add_edge(u, v)
    n_components = nx.number_connected_components(G)
    sizes = sorted((len(c) for c in nx.connected_components(G)), reverse=True)
    n_nodes = G.number_of_nodes()
    n_edges = G.number_of_edges()
    giant   = sizes[0] if sizes else 0
    self_loops = sum(1 for f in lines
                     if (f["properties"].get("_u_id") ==
                         f["properties"].get("_v_id")))
    edge_keys = Counter(
        (tuple(sorted([f["properties"].get("_u_id"), f["properties"].get("_v_id")])),
         f["properties"].get("highway"), f["properties"].get("footway"))
        for f in lines
        if f["properties"].get("_u_id") and f["properties"].get("_v_id")
    )
    dups = sum(1 for v in edge_keys.values() if v > 1)
    report[label] = {
        "graph_nodes":  n_nodes,
        "graph_edges":  n_edges,
        "components":   n_components,
        "giant_size":   giant,
        "giant_pct":    round(giant / n_nodes, 4) if n_nodes else 0.0,
        "self_loops":   self_loops,
        "duplicate_pairs": dups,
    }
    print(f"[topo:{label}] nodes={n_nodes:,} edges={n_edges:,} "
          f"components={n_components:,} "
          f"giant={giant:,} ({100*giant/max(1,n_nodes):.1f}%) "
          f"self_loops={self_loops} dups={dups}")


# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------

def write_outputs(points: list, lines: list, output: Path, report: dict):
    out = output
    out.parent.mkdir(parents=True, exist_ok=True)
    fc = {
        "$schema":      SCHEMA_ID,
        "type":         "FeatureCollection",
        "dataSource": {
            "name":      "OpenStreetMap + NYC DOT Pedestrian Ramps + USGS 3DEP",
            "copyright": "© OpenStreetMap contributors; NYC DOT (public domain); USGS (public domain)",
            "license":   "ODbL-1.0 (OSM-derived; share-alike applies)",
        },
        "dataTimestamp": _now_iso(),
        "pipelineVersion": {
            "name":    PIPELINE_NAME,
            "version": PIPELINE_VER,
            "url":     PIPELINE_URL,
        },
        "region": build_region(),
        "features": [_decimal_to_float(f) for f in (points + lines)],
    }

    with out.open("w") as f:
        json.dump(fc, f, separators=(",", ":"))  # compact for size
    size_mb = out.stat().st_size / 1024 / 1024
    print(f"[write] {out.name}: {len(fc['features']):,} features ({size_mb:.1f} MB)")

    # Split-by-type ZIP for python-osw-validation
    split_dir = out.parent / "osw-split"
    split_dir.mkdir(exist_ok=True)
    base = {
        "$schema": SCHEMA_ID,
        "type":    "FeatureCollection",
        "dataSource": fc["dataSource"],
        "dataTimestamp": fc["dataTimestamp"],
        "pipelineVersion": fc["pipelineVersion"],
    }
    nodes_fc = {**base, "features": [_decimal_to_float(f) for f in points]}
    lines_fc = {**base, "features": [_decimal_to_float(f) for f in lines]}
    nodes_path = split_dir / "nyc.nodes.geojson"
    lines_path = split_dir / "nyc.edges.geojson"
    nodes_path.write_text(json.dumps(nodes_fc, separators=(",", ":")))
    lines_path.write_text(json.dumps(lines_fc, separators=(",", ":")))
    zip_path = out.parent / "nyc-osw-osw-split.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(nodes_path, arcname=nodes_path.name)
        zf.write(lines_path, arcname=lines_path.name)
    print(f"[write] split ZIP: {zip_path.name} "
          f"({zip_path.stat().st_size/1024/1024:.1f} MB)")

    return out, zip_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--merge-tolerance-m", type=float, default=5.0)
    args = ap.parse_args()

    report = {
        "input": str(args.input),
        "started_at": _now_iso(),
        "merge_tolerance_m": args.merge_tolerance_m,
    }

    points, lines, root_meta = load(args.input)
    report["original_root_metadata"] = root_meta
    report["n_points_in"] = len(points)
    report["n_lines_in"]  = len(lines)

    topology_quickcheck(points, lines, report, "topology_before")

    # Stage A: drop self-loops + dedup
    lines = drop_self_loops(lines, report)
    lines = dedup_edges(lines, report)
    topology_quickcheck(points, lines, report, "topology_after_dedup")

    # Stage B: aggressive node merge
    points, lines = merge_nearby_nodes(points, lines,
                                       tol_m=args.merge_tolerance_m,
                                       report=report)
    topology_quickcheck(points, lines, report, "topology_after_merge")

    # Stage C: re-clean (merge can create new self-loops/dups)
    lines = drop_self_loops(lines, report)
    lines = dedup_edges(lines, report)
    topology_quickcheck(points, lines, report, "topology_after_recln")

    # Stage D: enums
    points, lines = canonicalize_enums(points, lines, report)

    # Stage E: provenance stamp
    src_ts = root_meta.get("dataTimestamp", _now_iso())
    points, lines = stamp_provenance(points, lines, report, src_ts)

    report["n_points_out"] = len(points)
    report["n_lines_out"]  = len(lines)
    report["finished_at"]  = _now_iso()

    out_path, zip_path = write_outputs(points, lines, args.output, report)

    report_path = args.output.parent / "restore-report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[done] report: {report_path}")


if __name__ == "__main__":
    main()
