"""Why ORS on arm B strict still uses crossings the audit calls unramped.

Arm B strict is this graph converted to OSM by scripts/osw_to_osm.py: one OSM
way per OSW edge, and a crossing end with no surveyed ramp within 5 m tagged
kerb=raised, kerb:height=0.14 on the end node. ORS reads a kerb height for a
footway=crossing way from the nodes of that way. The audit (compare/measures.py)
flags every edge of a crossing when any end of the whole crossing lacks a ramp.

For each ORS route on arm B strict (rec_i10_k6) on a measured pair, this takes
the ways ORS says it used, finds the crossing edges the audit flags, and looks
at what the converted file gave ORS on each: a node tagged kerb=raised (ORS
should have refused it at a 0.06 m limit), only kerb=lowered nodes, or no kerb
tag at all. It also notes whether the route uses a street centreline that
meets the flagged edge, that is, whether it joined or left the crossing in the
roadway.

usage: python arm_b_crossings.py GRAPH_NPZ STRICT.osm.pbf STRICT.ways.json ROUTES.with_way_ids.jsonl.gz PAIRS_DETAIL.jsonl.gz OUT.json
"""

import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import osmium

CONFIG, DETAIL_KEY = "rec_i10_k6", "ors_b_strict_rec_i10_k6"
AREAS = ("BK", "QN", "MN", "BX", "SI", "citywide", "brownsville")


def jsonl(path):
    with gzip.open(path, "rt") as f:
        yield from map(json.loads, f)


class Pbf(osmium.SimpleHandler):
    """Kerb value of every tagged node, and for each crossing way the kerb values on its nodes."""

    def __init__(self):
        super().__init__()
        self.kerb, self.crossing = {}, {}

    def node(self, n):
        if "kerb" in n.tags:
            self.kerb[n.id] = n.tags["kerb"]

    def way(self, w):
        if w.tags.get("footway") == "crossing":
            self.crossing[w.id] = {
                self.kerb[n.ref] for n in w.nodes if n.ref in self.kerb
            }


def main(npz, pbf, ways_json, routes, detail, out):
    z = np.load(npz)
    kind, ramped, u, v = z["kind"], z["curbramps"], z["u"], z["v"]
    edge_of = {f: e for e, f in enumerate(z["fid"].tolist())}
    way_edge = np.array(
        [edge_of[f] for f in json.loads(Path(ways_json).read_text())]
    )  # OSM way id - 1 -> edge
    del edge_of
    h = Pbf()
    h.apply_file(pbf)
    flagged_edge = (kind == "crossing") & ~ramped
    street = kind == "street"

    area, audit_count = {}, {}
    for r in jsonl(detail):
        if r["set"] == "landmark" or r["snap_apart_m"] > 25:
            continue
        b = r["routes"].get(DETAIL_KEY)
        if b and b["found"]:
            area[r["id"]] = "brownsville" if r["set"] == "brownsville" else r["area"]
            audit_count[r["id"]] = b["crossing_no_ramp"]

    res = {a: Counter() for a in AREAS}
    edges_by = {a: Counter() for a in AREAS}
    for r in jsonl(routes):
        if r["config"] != CONFIG or r["id"] not in area:
            continue
        c, ec = res[area[r["id"]]], edges_by[area[r["id"]]]
        c["routes"] += 1
        c["flagged_by_the_published_audit"] += audit_count[r["id"]] > 0
        ways = np.array(r.get("osmid") or [], dtype=np.int64)
        e = way_edge[ways - 1]
        hit = flagged_edge[e]
        if not hit.any():
            continue
        c["with_a_flagged_crossing_edge_by_way_ids"] += 1
        street_nodes = set(u[e[street[e]]].tolist()) | set(v[e[street[e]]].tolist())
        seen = defaultdict(bool)
        for w, ed in zip(ways[hit].tolist(), e[hit].tolist(), strict=True):
            tags = h.crossing.get(w)
            what = (
                "way_not_tagged_crossing_in_the_file"
                if tags is None
                else "raised_node"
                if "raised" in tags
                else "lowered_nodes_only"
                if tags
                else "no_kerb_tag"
            )
            at_street = int(u[ed]) in street_nodes or int(v[ed]) in street_nodes
            ec[what] += 1
            ec[what + ", meets a street centreline the route uses"] += at_street
            seen[what] = True
            seen["at_street"] |= at_street and what != "raised_node"
        c["with_a_flagged_edge_that_has_a_raised_node"] += seen["raised_node"]
        c["with_no_flagged_edge_that_has_a_raised_node"] += not seen["raised_node"]
        c["of_those_joining_or_leaving_a_flagged_edge_in_the_roadway"] += (
            not seen["raised_node"]
        ) and seen["at_street"]
        c["with_any_flagged_edge_met_in_the_roadway"] += seen["at_street"]

    kerb_values = Counter(h.kerb.values())
    way_kinds = Counter(
        "raised" if "raised" in t else "lowered only" if t else "no kerb tag"
        for t in h.crossing.values()
    )
    text = json.dumps(
        {
            "_meta": {
                "date": "2026-10-04",
                "code": "evaluation/compare/results/code/arm_b_crossings.py",
                "inputs": {
                    "graph": "research_notes/compare/graph/nyc.npz (this graph as arrays, compare/graph.py)",
                    "converted_file": "research_notes/compare/armB/strict/nyc-osw-strict.osm.pbf and its .ways.json (scripts/osw_to_osm.py --no-ramp raised)",
                    "routes": "research_notes/compare/routes/ors_armB_strict.with_way_ids.jsonl.gz, config rec_i10_k6",
                    "detail": "research_notes/compare/results/pairs_detail.jsonl.gz (measured pairs and the published audit count)",
                },
                "what": {
                    "routes": "ORS routes on arm B strict at incline 10, kerb 0.06, on measured pairs",
                    "flagged_by_the_published_audit": "routes with crossing_no_ramp > 0 in pairs_detail, the tables.md row",
                    "with_a_flagged_crossing_edge_by_way_ids": "routes whose ORS way ids include a crossing edge of a crossing that lacks a ramp at some end; counted from the way ids alone, so it can exceed the audit, which also needs the edge to lie along the route line",
                    "with_a_flagged_edge_that_has_a_raised_node": "the converted way has a node tagged kerb=raised, kerb:height=0.14, which ORS refuses at 0.06 on the tag probe; such a way id on a route means ORS used the way without passing the kerb, or the audit and ORS differ",
                    "with_no_flagged_edge_that_has_a_raised_node": "every flagged edge on the route is a piece of a crossing whose converted way carries no raised node: the unramped end is on another piece",
                    "of_those_joining_or_leaving_a_flagged_edge_in_the_roadway": "the route also uses a street centreline that meets such a piece",
                    "edges": "the same per flagged edge on those routes",
                },
                "converted_file_counts": {
                    "nodes_by_kerb_value": dict(kerb_values),
                    "crossing_ways_by_kerb_on_their_nodes": dict(way_kinds),
                    "crossing_edges_the_audit_flags": int(flagged_edge.sum()),
                    "crossing_edges": int((kind == "crossing").sum()),
                },
            },
            "routes": {a: dict(res[a]) for a in AREAS},
            "edges": {a: dict(edges_by[a]) for a in AREAS},
        },
        indent=1,
    )
    Path(out).write_text(text)
    for a in AREAS:
        print(a, dict(res[a]), dict(edges_by[a]))


if __name__ == "__main__":
    main(*sys.argv[1:7])
