#!/usr/bin/env python3
"""Snap edge endpoints to their referenced node coordinates, in place.

Required post-build step after `python -m pipeline build`.

python-osw-validation >= 0.4.0 validates that each edge's start/end coordinate
matches the coordinate of the node referenced by _u_id / _v_id. The pipeline's
endpoint merge (pipeline/stages/assemble.py, _merge_near_endpoints) remaps
_u_id/_v_id to canonical node IDs without moving the edge's terminal vertices,
leaving gaps that 0.4.x flags (typically 1 to 4 m, occasionally tens of metres
where the merge chains several endpoints). This pass snaps every edge endpoint
to its node coordinate, does the same for each Pedestrian Zone's ring vertices
(vertex i is node _w_id[i]), rewrites the canonical GeoJSON in place, and emits
the split node, edge and zone files plus the validator ZIP. Stdlib only.
Idempotent.

python-osw-validation >= 0.5.0 also rejects coordinates with more than 7
decimal places, so every coordinate is rounded to 7 places (about 1 cm) first.
"""

from __future__ import annotations

import argparse
import json
import time
import zipfile
from pathlib import Path

# Decimal places kept on every coordinate; the validator's limit since 0.5.0.
PRECISION = 7


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
            c[:2] = [round(float(c[0]), PRECISION), round(float(c[1]), PRECISION)]
            node_coord[nid] = c[:2]

    snapped_u = snapped_v = degenerate = unresolved = 0
    for ft in feats:
        if (ft.get("geometry") or {}).get("type") != "LineString":
            continue
        coords = ft["geometry"].get("coordinates")
        if not coords or len(coords) < 2:
            continue
        coords[:] = [[round(float(x), PRECISION) for x in pt] for pt in coords]
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

    # A zone's ring vertex i is its _w_id[i]: move each onto its node's
    # coordinate, as for edge ends, and fold vertices the merge made equal.
    snapped_w = 0
    for ft in feats:
        if (ft.get("geometry") or {}).get("type") != "Polygon":
            continue
        p = ft.get("properties") or {}
        w = p.get("_w_id") or []
        ring = ft["geometry"]["coordinates"][0]
        if len(ring) != len(w) + 1:
            unresolved += 1
            continue
        new_w, new_ring = [], []
        for nid, pt in zip(w, ring[:-1]):
            c = node_coord.get(nid) or [round(float(x), PRECISION) for x in pt[:2]]
            if node_coord.get(nid) is not None and list(c) != [float(x) for x in pt[:2]]:
                snapped_w += 1
            if new_w and new_w[-1] == nid:
                continue
            new_w.append(nid)
            new_ring.append(list(c))
        if len(new_w) > 1 and new_w[0] == new_w[-1]:
            new_w.pop()
            new_ring.pop()
        p["_w_id"] = new_w
        ft["geometry"]["coordinates"] = [new_ring + [new_ring[0]]]

    print(
        f"[snap] u={snapped_u:,} v={snapped_v:,} w={snapped_w:,} "
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
    zones = [ft for ft in feats if (ft.get("geometry") or {}).get("type") == "Polygon"]

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
    zones_path = split_dir / "nyc.zones.geojson"
    if zones:
        zones_path.write_text(
            json.dumps({**split_base, "features": zones}, separators=(",", ":"))
        )
    else:
        zones_path.unlink(missing_ok=True)
    zip_path = inp.parent / "nyc-osw-osw-split.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(nodes_path, arcname=nodes_path.name)
        zf.write(edges_path, arcname=edges_path.name)
        if zones:
            zf.write(zones_path, arcname=zones_path.name)
    print(f"[write] {inp.name}, {nodes_path.name}, {edges_path.name}, "
          f"{zones_path.name if zones else 'no zones'}, {zip_path.name}")

    report = {
        "input": str(inp),
        "endpoints_snapped_u": snapped_u,
        "endpoints_snapped_v": snapped_v,
        "endpoints_snapped_total": snapped_u + snapped_v,
        "edges_with_unresolved_node_ref": unresolved,
        "degenerate_after_snap": degenerate,
        "zone_vertices_snapped": snapped_w,
        "nodes": len(points),
        "edges": len(lines),
        "zones": len(zones),
    }
    (inp.parent / "snap-report.json").write_text(json.dumps(report, indent=2))
    print(f"[done] {report}")


if __name__ == "__main__":
    main()
