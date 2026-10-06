"""Convert an OSW v0.3 FeatureCollection back to OSM, for a router that reads OSM tags.

NEVER UPLOAD THE OUTPUT TO OPENSTREETMAP. It is for a local routing engine
only. It carries NYC DOT survey data and kerb tags this project invented for
the comparison, and its ids are not OpenStreetMap's.

Written for OpenRouteService's wheelchair profile, after TDEI's
osm-osw-reformatter 0.4.2 was found not to carry what that profile reads
(evaluation/compare/probes/roundtrip_reformatter.json): it writes incline
as a ratio with both directions joined ("-0.0039;0.0039"), which ORS reads as
0%, and it leaves kerb tags on the surveyed ramp nodes only, so a crossing
whose ramp sits on a neighbouring sidewalk vertex shows no kerb at all.

What this writes:
  nodes   every OSW node. A crossing end with a surveyed ramp within 5 m (the
          rule in osw_to_unweaver.py) gets kerb=lowered. With --no-ramp raised,
          an end with none gets kerb=raised and kerb:height=RAISED_HEIGHT;
          with --no-ramp unknown it gets no tag.
  ways    one per pair of opposite OSW edges, in the direction of the first.
          highway, footway, surface, name and crossing:markings as they are,
          width in metres, incline as a signed percentage ("8.3%"). A
          Pedestrian Zone becomes a closed way with area=yes.
  sidecar OUTPUT.ways.json, the OSW edge _id for each OSM way id.

With --pedestrian-only the street centrelines are left out, so a router on the
result can use only what this graph's own profiles use.

usage: python scripts/osw_to_osm.py --input OSW.geojson --output OUT.osm.pbf [--no-ramp raised|unknown] [--pedestrian-only]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import osmium
from osw_to_unweaver import PASSABLE_KERBS, RAMP_REACH_M, crossing_ends

# The height written for a kerb with no ramp. A nominal value, not a
# measurement: ORS 10.0.1 does not read kerb=raised, and reads a bare metre
# value of 0.15 or more as centimetres (0.15 becomes 0 cm), so 0.14 is the
# largest full-kerb height it parses as intended.
RAISED_HEIGHT = "0.14"
WAY_TAGS = ("highway", "footway", "surface", "name", "crossing:markings")


def kerb_tags(feats, no_ramp="raised"):
    """{OSW node id: OSM tags} for the ends of every crossing."""
    curb_ids, node_xy = set(), {}
    for f in feats:
        if (f.get("geometry") or {}).get("type") != "Point":
            continue
        p = f["properties"]
        node_xy[p["_id"]] = f["geometry"]["coordinates"][:2]
        if p.get("kerb") in PASSABLE_KERBS:
            curb_ids.add(p["_id"])
    _, ends = crossing_ends(feats, curb_ids, node_xy, RAMP_REACH_M)
    tags = {}
    for state in ends.values():
        for node, ramp in state.items():
            if ramp:
                tags[node] = {"barrier": "kerb", "kerb": "lowered"}
            elif no_ramp == "raised":
                tags[node] = {"barrier": "kerb", "kerb": "raised", "kerb:height": RAISED_HEIGHT}
    return tags


def way_tags(p):
    tags = {k: str(p[k]) for k in WAY_TAGS if p.get(k) is not None}
    if p.get("ext:sidewalk") is not None:      # a street's sidewalk tag, as OSM had it
        tags["sidewalk"] = str(p["ext:sidewalk"])
    if p.get("width") is not None:
        tags["width"] = f"{float(p['width']):g}"
    if p.get("incline") is not None:
        tags["incline"] = f"{float(p['incline']) * 100:.1f}%"
    return tags


def convert(feats, output, no_ramp="raised", pedestrian_only=False):
    """Write the PBF and the sidecar. Returns (nodes, ways) written."""
    output = Path(output)
    output.unlink(missing_ok=True)
    kerbs = kerb_tags(feats, no_ramp)
    node_id, written = {}, 0
    writer = osmium.SimpleWriter(str(output))
    for f in feats:
        if f["geometry"]["type"] == "Point":
            p = f["properties"]
            node_id[p["_id"]] = len(node_id) + 1
            x, y = f["geometry"]["coordinates"][:2]
            writer.add_node(osmium.osm.mutable.Node(id=node_id[p["_id"]], location=(x, y), tags=kerbs.get(p["_id"], {}), version=1))
    next_node = len(node_id) + 1
    done, ways, pending = set(), [], []
    for f in feats:
        if f["geometry"]["type"] != "LineString":
            continue
        p = f["properties"]
        coords = [tuple(c[:2]) for c in f["geometry"]["coordinates"]]
        # Opposite edges share a line and a class. Two ways of different classes can share a line too
        # (steps and the path beside them, drawn on the same nodes), and both must survive.
        key = (min(tuple(coords), tuple(reversed(coords))), p.get("highway"), p.get("footway"))
        if key in done or p["_u_id"] == p["_v_id"] or (pedestrian_only and p.get("highway") not in ("footway", "pedestrian", "steps")):
            continue
        done.add(key)
        refs = [node_id[p["_u_id"]]]
        for x, y in coords[1:-1]:       # vertices inside an edge are not OSW nodes
            writer.add_node(osmium.osm.mutable.Node(id=next_node, location=(x, y), version=1))
            refs.append(next_node)
            next_node += 1
        refs.append(node_id[p["_v_id"]])
        ways.append(p["_id"])
        pending.append((refs, way_tags(p)))
    for f in feats:
        if f["geometry"]["type"] != "Polygon" or not f["properties"].get("_w_id"):
            continue
        p = f["properties"]
        refs = [node_id[n] for n in p["_w_id"] if n in node_id]
        if len(refs) < 3:
            continue
        ways.append(p["_id"])
        pending.append((refs + [refs[0]], {**way_tags(p), "highway": "pedestrian", "area": "yes"}))
    for i, (refs, tags) in enumerate(pending, start=1):
        writer.add_way(osmium.osm.mutable.Way(id=i, nodes=refs, tags=tags, version=1))
        written += 1
    writer.close()
    Path(str(output) + ".ways.json").write_text(json.dumps(ways))
    return next_node - 1, written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--no-ramp", choices=("raised", "unknown"), default="raised")
    ap.add_argument("--pedestrian-only", action="store_true")
    args = ap.parse_args()
    feats = json.loads(args.input.read_text())["features"]
    nodes, ways = convert(feats, args.output, args.no_ramp, args.pedestrian_only)
    print(f"[write] {args.output}: {nodes:,} nodes, {ways:,} ways")


if __name__ == "__main__":
    main()
