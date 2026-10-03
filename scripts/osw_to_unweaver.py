"""Convert an OSW v0.3 FeatureCollection into the flat LineString format
Unweaver expects in `layers/*.geojson`.

Unweaver schema (per nbolten/unweaver example/layers/uw.geojson):
  Feature.properties:
    footway         str    "sidewalk" | "crossing" | etc. (or absent for streets)
    subclass        str    "footway" | "street" | ...
    curbramps       bool   On a crossing: True if a surveyed ramp lies within
                           5 m of each end of the whole crossing (see the
                           comment in main). Elsewhere: True if either
                           endpoint has a Curb Node.
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

from scipy.spatial import cKDTree

# How far a surveyed ramp may be from the end of a crossing and still count
# for it. The pipeline snaps a ramp to the graph within the same distance.
# Checked remotely on a stratified sample of 200 crossings rated over 2018
# aerial imagery by language-model agents following a written protocol
# (evaluation/crossing_rule/score.json); nothing was checked on the ground.
# Of the crossings this rule calls ramped, 98.8% have a surveyed ramp
# positioned to serve them at both ends, and it misses none that do. The
# strict test (ramp on the crossing's own nodes) finds 56% of them. See
# METHODOLOGY.md for the limits of that check.
RAMP_REACH_M = 5.0


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


# ----- the ramp rule ------------------------------------------------------

def crossing_ends(feats, curb_ids, node_xy, reach_m):
    """Each crossing's edges, and whether each of its ends has a ramp in reach.

    A crossing is usually several edges: the kerb, lane and centreline
    vertices of the OSM way are all nodes. A surveyed ramp is snapped to the
    nearest pedestrian vertex within 5 m, which is as often a sidewalk vertex
    beside the crossing as a node of the crossing itself. So "either endpoint
    of this edge is a curb node" fails most edges of a crossing that has a
    ramp at both corners. Judge the crossing as a whole: join crossing edges
    that meet at a node no sidewalk or footway reaches. Its ends are the nodes
    where it meets the rest of the pedestrian network, and an end has a ramp
    when a surveyed ramp lies within reach_m of it.

    Returns ({crossing: [edge ids]}, {crossing: {end node: has a ramp}}).
    """
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
    parent = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    at_inner = {}
    for eid, u, v in crossing_edges:
        for n in (u, v):
            if n not in walk_nodes:
                parent[find(eid)] = find(at_inner.setdefault(n, eid))

    # One scale for the whole city. Scaling each point's longitude by the
    # cosine of its own latitude shears the plane: a ramp 3 m due north came
    # out 3.9 m away.
    east = 111320 * math.cos(math.radians(40.7))

    def metres(lonlat):
        return (lonlat[0] * east, lonlat[1] * 111320)

    ramps = cKDTree([metres(node_xy[n]) for n in curb_ids]) if curb_ids else None
    groups, ends = {}, {}
    for eid, u, v in crossing_edges:
        g = find(eid)
        groups.setdefault(g, []).append(eid)
        for n in (u, v):
            if n in walk_nodes:
                ends.setdefault(g, {})[n] = ramps is not None and ramps.query(metres(node_xy[n]))[0] <= reach_m
    return groups, ends


def crossings_with_ramps(feats, curb_ids, node_xy, reach_m):
    """IDs of the crossing edges that lie on a crossing with a ramp at each end."""
    groups, ends = crossing_ends(feats, curb_ids, node_xy, reach_m)
    ok = {eid for g, eids in groups.items() if ends.get(g) and all(ends[g].values()) for eid in eids}
    print(f"[crossings] {sum(len(e) for e in groups.values()):,} edges in {len(groups):,} crossings; "
          f"{len(ok):,} edges on a crossing with a ramp at each end")
    return ok


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

    # First pass: node coordinates, and which node ids carry a curb-ramp annotation.
    curb_ids = set()
    node_xy = {}
    for f in feats:
        gt = (f.get("geometry") or {}).get("type")
        if gt != "Point":
            continue
        p = f.get("properties") or {}
        node_xy[p.get("_id")] = f["geometry"]["coordinates"][:2]
        if (p.get("barrier") == "kerb"
                or p.get("kerb") in {"lowered", "raised", "flush"}):
            nid = p.get("_id")
            if nid:
                curb_ids.add(nid)
    print(f"[curb] curb-annotated nodes: {len(curb_ids):,}")

    crossing_ok = crossings_with_ramps(feats, curb_ids, node_xy, RAMP_REACH_M)

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
