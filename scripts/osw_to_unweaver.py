"""Convert an OSW v0.3 FeatureCollection into the flat LineString format
Unweaver expects in `layers/*.geojson`.

Unweaver schema (per nbolten/unweaver example/layers/uw.geojson):
  Feature.properties:
    footway         str    "sidewalk" | "crossing" | etc. (or absent for streets)
    subclass        str    "footway" | "street" | ...
    curbramps       bool   On a crossing: True if the whole crossing, kerb to
                           kerb, has a Curb Node at each end. Elsewhere: True
                           if either endpoint has a Curb Node.
    incline         float  signed grade (rise/run)
    length          float  edge length, metres (great-circle)
    surface         str    OSW canonical surface
    width           float  metres if known
    description     str    name + side info if available

Geometry: 4326 LineString.

We also produce /regions.geojson (a single-feature FeatureCollection of NYC's
region polygon) since AccessMap-style deployments expect one.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import click


# ----- length helpers ------------------------------------------------------

def _haversine_m(p1, p2):
    R = 6371000.0
    lat1, lat2 = math.radians(p1[1]), math.radians(p2[1])
    dlat = lat2 - lat1
    dlon = math.radians(p2[0] - p1[0])
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _polyline_length_m(coords):
    return sum(_haversine_m(coords[i], coords[i + 1]) for i in range(len(coords) - 1))


# ----- main convert -------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, type=Path,
                    help="OSW v0.3 GeoJSON (canonical)")
    ap.add_argument("--output-layer", required=True, type=Path,
                    help="Output transportation.geojson (Unweaver layer)")
    ap.add_argument("--output-region", required=True, type=Path,
                    help="Output regions.geojson")
    args = ap.parse_args()

    print(f"[load] reading {args.input.name}...", flush=True)
    fc = json.loads(args.input.read_text())
    feats = fc["features"]

    # First pass: which node ids carry a curb-ramp annotation?
    curb_ids = set()
    for f in feats:
        gt = (f.get("geometry") or {}).get("type")
        if gt != "Point":
            continue
        p = f.get("properties") or {}
        if (p.get("barrier") == "kerb"
                or p.get("kerb") in {"lowered", "raised", "flush"}):
            nid = p.get("_id")
            if nid:
                curb_ids.add(nid)
    print(f"[curb] curb-annotated nodes: {len(curb_ids):,}")

    # A crossing is usually several edges: kerb, lane and centreline vertices
    # are all nodes. Only the two outer edges touch a kerb, so "either endpoint
    # is a curb node" fails every edge in the middle of a crossing that has a
    # ramp at both ends. Judge the crossing as a whole instead: group crossing
    # edges that meet at a node no sidewalk or footway reaches, and pass the
    # group when every node where it meets the rest of the pedestrian network
    # is a curb node.
    crossing_edges, walk_nodes = [], set()
    for f in feats:
        if (f.get("geometry") or {}).get("type") != "LineString":
            continue
        p = f.get("properties") or {}
        u, v = p.get("_u_id"), p.get("_v_id")
        if p.get("highway") == "footway" and p.get("footway") == "crossing":
            crossing_edges.append((p.get("_id"), u, v))
        elif p.get("highway") in ("footway", "steps"):
            walk_nodes.update((u, v))
    ends = curb_ids | walk_nodes
    parent = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    at_inner = {}
    for eid, u, v in crossing_edges:
        for n in (u, v):
            if n not in ends:
                parent[find(eid)] = find(at_inner.setdefault(n, eid))
    group_ends = {}
    for eid, u, v in crossing_edges:
        group_ends.setdefault(find(eid), set()).update(n for n in (u, v) if n in ends)
    crossing_ok = {eid for eid, _, _ in crossing_edges
                   if group_ends[find(eid)] and group_ends[find(eid)] <= curb_ids}
    print(f"[crossings] {len(crossing_edges):,} edges, {len(crossing_ok):,} on a crossing "
          f"with a curb node at each end")

    # Second pass: build flat-format edges
    out_features = []
    skipped_no_geom = 0
    skipped_no_uv = 0
    for f in feats:
        gt = (f.get("geometry") or {}).get("type")
        if gt != "LineString":
            continue
        p = f.get("properties") or {}
        coords = (f.get("geometry") or {}).get("coordinates", [])
        if len(coords) < 2:
            skipped_no_geom += 1
            continue
        u, v = p.get("_u_id"), p.get("_v_id")
        if not u or not v or u == v:
            skipped_no_uv += 1
            continue

        coords = [[float(c[0]), float(c[1])] for c in coords]
        length_m = round(_polyline_length_m(coords), 3)

        if p.get("footway") == "crossing":
            curbramps = p.get("_id") in crossing_ok
        else:
            curbramps = (u in curb_ids) or (v in curb_ids)

        # Subclass
        hw = p.get("highway")
        fw = p.get("footway")
        if hw == "footway" and fw == "sidewalk":
            subclass = "footway"
            footway_val = "sidewalk"
        elif hw == "footway" and fw == "crossing":
            subclass = "footway"
            footway_val = "crossing"
        elif hw == "footway":
            subclass = "footway"
            footway_val = fw or None
        elif hw == "steps":
            subclass = "steps"
            footway_val = None
        else:
            subclass = "street"
            footway_val = None

        incline = p.get("incline")
        try:
            incline = float(incline) if incline is not None else None
        except (TypeError, ValueError):
            incline = None

        surface = p.get("surface")
        width = p.get("width")
        try:
            width = float(width) if width is not None else None
        except (TypeError, ValueError):
            width = None

        name = p.get("name")
        description = name if name else f"{subclass} {p.get('_id','')}".strip()

        out = {
            "type": "Feature",
            "properties": {
                "fid":         p.get("_id"),
                "_u":          u,
                "_v":          v,
                "subclass":    subclass,
                "footway":     footway_val,
                "curbramps":   1 if curbramps else 0,
                "incline":     incline,
                "length":      length_m,
                "surface":     surface,
                "width":       width,
                "description": description,
                "ext_borough": p.get("ext:borough"),
                "ext_osm_id":  p.get("ext:osm_id"),
            },
            "geometry": {"type": "LineString", "coordinates": coords},
        }
        out_features.append(out)

    print(f"[edges] kept {len(out_features):,} | "
          f"skipped no_geom={skipped_no_geom:,} no_uv={skipped_no_uv:,}")

    layer_fc = {
        "type": "FeatureCollection",
        "name": "transportation",
        "features": out_features,
    }
    args.output_layer.parent.mkdir(parents=True, exist_ok=True)
    with args.output_layer.open("w") as f:
        json.dump(layer_fc, f)
    size_mb = args.output_layer.stat().st_size / 1_048_576
    print(f"[write] {args.output_layer}: {size_mb:.1f} MB")

    # regions.geojson
    region = fc.get("region")
    if region is None:
        # Fallback: derive bbox from edges
        all_coords = [c for ft in out_features for c in ft["geometry"]["coordinates"]]
        if all_coords:
            xs = [c[0] for c in all_coords]; ys = [c[1] for c in all_coords]
            region = {
                "type": "Polygon",
                "coordinates": [[[min(xs),min(ys)],[max(xs),min(ys)],
                                 [max(xs),max(ys)],[min(xs),max(ys)],
                                 [min(xs),min(ys)]]],
            }
    region_fc = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {"id": "nyc", "name": "New York City"},
            "geometry": region,
        }],
    }
    args.output_region.parent.mkdir(parents=True, exist_ok=True)
    args.output_region.write_text(json.dumps(region_fc))
    print(f"[write] {args.output_region}")


if __name__ == "__main__":
    main()
