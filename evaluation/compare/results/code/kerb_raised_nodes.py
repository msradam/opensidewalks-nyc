"""Routes that pass over an OSM node tagged kerb=raised, counted by node.

The raw OSM audit in tables.md flags a route when it uses a footway=crossing
way that has a kerb=raised node anywhere on it. That is a way-level flag: the
node can be at a median or at an end the route never reaches. This counts by
node instead, with one method for every router: a route is counted when a
kerb=raised node of a crossing way it uses lies within NEAR_M of the route's
own line. The way-level flag is recomputed beside it from the same inputs.

Routers: the ORS reference and ORS with no limits (plain OSM, way ids from
ORS), Valhalla's wheelchair type (way ids from Valhalla), and this graph's
wheelchair and walking profiles (the OSM way id of each edge used). This
graph's routes are taken as whole edges, so a first or last edge is counted
in full even where the trip starts part way along it; that can only raise
this graph's count.

usage: python kerb_raised_nodes.py CLIP.osm.pbf GRAPH_NPZ ROUTES_DIR PAIRS_DETAIL.jsonl.gz OUT.json
"""

import gzip
import json
import math
import pickle
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import osmium
import shapely

NEAR_M = 1.0
EAST = 111320 * math.cos(math.radians(40.7))
ENGINES = {
    "ors_a_rec_i10_k6": ("ors_armA.with_way_ids.jsonl.gz", "rec_i10_k6"),
    "ors_a_no_limits": ("ors_armA.with_way_ids.jsonl.gz", "default"),
    "valhalla_wheelchair": ("valhalla.with_way_ids.jsonl.gz", "wheelchair"),
}
OURS = {"ours_wheelchair": "wheelchair", "ours_walk": "walk"}
AREAS = ("BK", "QN", "MN", "BX", "SI", "citywide", "brownsville")


def jsonl(path):
    with gzip.open(path, "rt") as f:
        yield from map(json.loads, f)


def read_raised(pbf):
    nodes, ways = {}, {}

    class H(osmium.SimpleHandler):
        def node(self, n):
            if n.tags.get("kerb") == "raised":
                nodes[n.id] = (n.location.lon * EAST, n.location.lat * 111320)

        def way(self, w):
            if w.tags.get("footway") == "crossing":
                pts = [nodes[n.ref] for n in w.nodes if n.ref in nodes]
                if pts:
                    ways[w.id] = pts

    H().apply_file(str(pbf))
    return nodes, ways


def judge(way_ids, geom, raised_ways):
    """(way-level flag, node-level flag) for one route."""
    pts = [p for w in set(way_ids) if w in raised_ways for p in raised_ways[w]]
    if not pts:
        return False, False
    return True, bool((shapely.distance(geom, shapely.points(pts)) <= NEAR_M).any())


def main(pbf, npz, routes_dir, detail, out):
    routes_dir = Path(routes_dir)
    nodes, raised_ways = read_raised(pbf)
    area = {}
    for r in jsonl(detail):
        if r["set"] != "landmark" and r["snap_apart_m"] <= 25:
            area[r["id"]] = "brownsville" if r["set"] == "brownsville" else r["area"]
    res = {k: {a: Counter() for a in AREAS} for k in [*OURS, *ENGINES]}

    def count(key, pid, way_ids, geom):
        c = res[key][area[pid]]
        way, node = judge(way_ids, geom, raised_ways)
        c["routes"] += 1
        c["way_level"] += way
        c["node_level"] += node

    for fname in sorted({f for f, _ in ENGINES.values()}):
        keys = {cfg: k for k, (f, cfg) in ENGINES.items() if f == fname}
        for r in jsonl(routes_dir / fname):
            if (
                r["config"] in keys
                and r["found"]
                and r["id"] in area
                and len(r["coords"]) >= 2
            ):
                xy = np.array(r["coords"]) * (EAST, 111320)
                count(
                    keys[r["config"]],
                    r["id"],
                    r.get("osmid") or [],
                    shapely.linestrings(xy),
                )

    z = np.load(npz)
    osm_id, coords, offsets = z["osm_id"], z["coords"] * (EAST, 111320), z["offsets"]
    with open(routes_dir / "ours.pkl", "rb") as f:
        ours = pickle.load(f)
    for pid, by_profile in ours.items():
        for key, prof in OURS.items():
            e = by_profile[prof]["edges"]
            if e and pid in area:
                geom = shapely.multilinestrings(
                    [
                        shapely.linestrings(coords[offsets[i] : offsets[i + 1]])
                        for i in e
                    ]
                )
                count(key, pid, osm_id[e].tolist(), geom)

    for by_area in res.values():
        for c in by_area.values():
            for lvl in ("way_level", "node_level"):
                c.setdefault(lvl, 0)
                c[lvl + "_share"] = (
                    round(c[lvl] / c["routes"], 4) if c["routes"] else None
                )
    text = json.dumps(
        {
            "_meta": {
                "date": "2026-10-04",
                "code": "evaluation/compare/results/code/kerb_raised_nodes.py",
                "inputs": {
                    "osm": "engines/osm/nyc-261001.osm.pbf (the pinned clip)",
                    "graph": "research_notes/compare/graph/nyc.npz",
                    "routes": "research_notes/compare/routes/: ors_armA.with_way_ids.jsonl.gz (rec_i10_k6, default), valhalla.with_way_ids.jsonl.gz (wheelchair), ours.pkl",
                    "detail": "research_notes/compare/results/pairs_detail.jsonl.gz (which pairs are measured)",
                },
                "what": {
                    "routes": "routes found on measured pairs (snapped ends within 25 m across routers); the 17 landmark pairs are left out",
                    "way_level": "routes using a footway=crossing way with a node tagged kerb=raised anywhere on it, by the way ids of the route. tables.md counts the same flag after matching the route to this graph's edges, so the two can differ by a few routes",
                    "node_level": f"routes with such a node within {NEAR_M} m of the route's own line: the route passes over the tagged kerb",
                    "ours": "this graph's routes are counted on whole edges, the first and last included in full",
                },
                "in_the_clip": {
                    "nodes_tagged_kerb_raised": len(nodes),
                    "crossing_ways_with_such_a_node": len(raised_ways),
                },
            },
            **{k: {a: dict(c) for a, c in v.items()} for k, v in res.items()},
        },
        indent=1,
    )
    Path(out).write_text(text)
    for k, v in res.items():
        print(
            k,
            {
                a: (c["routes"], c["way_level_share"], c["node_level_share"])
                for a, c in v.items()
            },
        )


if __name__ == "__main__":
    main(*sys.argv[1:6])
