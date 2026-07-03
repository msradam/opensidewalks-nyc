#!/usr/bin/env python3
"""Snap edge endpoints to their referenced node coordinates, in place.

Required post-build step after `python -m pipeline build`.

python-osw-validation >= 0.4.0 validates that each edge's start/end coordinate
matches the coordinate of the node referenced by _u_id / _v_id. The pipeline's
endpoint merge (pipeline/stages/assemble.py, _merge_near_endpoints) remaps
_u_id/_v_id to canonical node IDs without moving the edge's terminal vertices,
leaving sub-metre gaps that 0.4.x flags. This pass snaps every edge endpoint to
its node coordinate, rewrites the canonical GeoJSON in place, and emits the
split node/edge files plus the validator ZIP. Stdlib only. Idempotent.
"""

from __future__ import annotations

import argparse
import json
import time
import zipfile
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=Path("output/nyc-osw.geojson"))
    args = ap.parse_args()
    inp = args.input

    t0 = time.time()
    with inp.open() as f:
        fc = json.load(f)
    feats = fc["features"]
    print(f"[load] {inp.name}: {len(feats):,} features ({time.time() - t0:.1f}s)")

    node_coord = {}
    for ft in feats:
        if (ft.get("geometry") or {}).get("type") != "Point":
            continue
        p = ft.get("properties") or {}
        nid = p.get("_id")
        c = ft["geometry"].get("coordinates")
        if nid and c and len(c) >= 2:
            node_coord[nid] = [float(c[0]), float(c[1])]

    snapped_u = snapped_v = degenerate = unresolved = 0
    for ft in feats:
        if (ft.get("geometry") or {}).get("type") != "LineString":
            continue
        coords = ft["geometry"].get("coordinates")
        if not coords or len(coords) < 2:
            continue
        p = ft.get("properties") or {}
        cu = node_coord.get(p.get("_u_id"))
        cv = node_coord.get(p.get("_v_id"))
        if cu is None or cv is None:
            unresolved += 1
        if cu is not None and coords[0] != cu:
            coords[0] = list(cu)
            snapped_u += 1
        if cv is not None and coords[-1] != cv:
            coords[-1] = list(cv)
            snapped_v += 1
        if coords[0] == coords[-1]:
            degenerate += 1

    print(
        f"[snap] u={snapped_u:,} v={snapped_v:,} "
        f"unresolved_refs={unresolved:,} degenerate={degenerate:,} "
        f"({time.time() - t0:.1f}s)"
    )

    with inp.open("w") as f:
        json.dump(fc, f, separators=(",", ":"))

    root = {k: v for k, v in fc.items() if k != "features"}
    split_base = {k: v for k, v in root.items() if k != "region"}
    points = [ft for ft in feats if (ft.get("geometry") or {}).get("type") == "Point"]
    lines = [
        ft for ft in feats if (ft.get("geometry") or {}).get("type") == "LineString"
    ]

    split_dir = inp.parent / "osw-split"
    split_dir.mkdir(exist_ok=True)
    nodes_path = split_dir / "nyc.nodes.geojson"
    edges_path = split_dir / "nyc.edges.geojson"
    nodes_path.write_text(
        json.dumps({**split_base, "features": points}, separators=(",", ":"))
    )
    edges_path.write_text(
        json.dumps({**split_base, "features": lines}, separators=(",", ":"))
    )
    zip_path = inp.parent / "nyc-osw-osw-split.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(nodes_path, arcname=nodes_path.name)
        zf.write(edges_path, arcname=edges_path.name)
    print(f"[write] {inp.name}, {nodes_path.name}, {edges_path.name}, {zip_path.name}")

    report = {
        "input": str(inp),
        "endpoints_snapped_u": snapped_u,
        "endpoints_snapped_v": snapped_v,
        "endpoints_snapped_total": snapped_u + snapped_v,
        "edges_with_unresolved_node_ref": unresolved,
        "degenerate_after_snap": degenerate,
        "nodes": len(points),
        "edges": len(lines),
    }
    (inp.parent / "snap-report.json").write_text(json.dumps(report, indent=2))
    print(f"[done] {report}")


if __name__ == "__main__":
    main()
