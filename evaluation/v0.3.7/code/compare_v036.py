"""Compare v0.3.6 with v0.3.7 feature by feature, and say which change moved what.

v0.3.7 adds cycleway and track edges that have no foot tag, moves surveyed
ramps onto crossing ends, carries OSM kerb and elevator node tags, and
writes a street's sidewalk tags. Every other feature of v0.3.6 should still
be there with the same geometry and the same values, except an incline
beside a new edge or a moved endpoint (heights are smoothed over short
edges) and the per-build provenance fields. Each difference is put in the
first group below that fits it, and whatever fits none is listed.

A surveyed ramp is matched by ext:ramp_id, because its node id is the id of
the vertex it snapped to, which this release changes on purpose.

usage: compare_v036.py V036.geojson[.gz] V037.geojson[.gz] OUT.json
Streams both files; a few GB of memory for a city file.
"""
import gzip
import hashlib
import json
import sys
from collections import Counter

import ijson

PER_BUILD = ("ext:pipeline_version", "ext:source_timestamp")
STREET_TAGS = ("ext:sidewalk", "ext:sidewalk_left", "ext:sidewalk_right")
OSM_KERB = ("barrier", "kerb", "tactile_paving", "ext:osm_kerb", "ext:osm_tactile_paving", "ext:osm_highway",
            "ext:source")
RAMP_FIELDS = ("barrier", "kerb", "tactile_paving", "ext:ramp_id", "ext:corner_id", "ext:street_1", "ext:street_2",
               "ext:running_slope_pct", "ext:cross_slope_pct", "ext:counter_slope_pct", "ext:dws_condition")


def kind(p, g):
    if g["type"] == "Point":
        return "curb_ramp" if p.get("ext:ramp_id") else "osm_kerb" if p.get("barrier") == "kerb" else "bare_node"
    if g["type"] == "Polygon":
        return "pedestrian_zone"
    hw, fw = p.get("highway"), p.get("footway")
    if hw == "footway" and fw in ("sidewalk", "crossing"):
        return fw
    if hw == "pedestrian":
        return "pedestrian_road"
    return hw if hw in ("footway", "steps") else "road"


def digest(obj):
    return hashlib.md5(json.dumps(obj, sort_keys=True).encode()).digest()[:8]


def scan(path):
    """{_id: record}, {ramp id: node id}"""
    rec, ramps = {}, {}
    with (gzip.open if str(path).endswith(".gz") else open)(path, "rb") as f:
        for ft in ijson.items(f, "features.item", use_float=True):
            p, g = ft["properties"], ft["geometry"]
            c = g["coordinates"]
            skip = (set(PER_BUILD) | set(STREET_TAGS) | set(OSM_KERB) | set(RAMP_FIELDS)
                    | {"incline", "ext:incline_unknown", "ext:elevation_m", "ext:elevation_source"})
            rec[p["_id"]] = {
                "kind": kind(p, g), "geom": digest(c), "rest": digest({a: b for a, b in p.items() if a not in skip}),
                "incline": p.get("incline"), "unknown": p.get("ext:incline_unknown"),
                "u": p.get("_u_id"), "v": p.get("_v_id"), "osm_highway": p.get("ext:osm_highway"),
                "street": {k: p.get(k) for k in STREET_TAGS if p.get(k) is not None},
                "kerb": {k: p.get(k) for k in OSM_KERB if p.get(k) is not None},
                "ramp": {k: p.get(k) for k in RAMP_FIELDS if p.get(k) is not None},
                "z": (p.get("ext:elevation_m"), p.get("ext:elevation_source")),
                "end0": tuple(c[0]) if g["type"] == "LineString" else None,
                "end1": tuple(c[-1]) if g["type"] == "LineString" else None,
                "pt": tuple(c) if g["type"] == "Point" else None}
            if p.get("ext:ramp_id"):
                ramps[p["ext:ramp_id"]] = p["_id"]
    return rec, ramps


before_path, after_path, out_path = sys.argv[1:4]
B, RB = scan(before_path)
print("v0.3.6 scanned", len(B), flush=True)
A, RA = scan(after_path)
print("v0.3.7 scanned", len(A), flush=True)
shared = B.keys() & A.keys()
only_after = A.keys() - B.keys()
only_before = B.keys() - A.keys()

# --- the new shared-path edges and their nodes ------------------------------
new_edges = {i for i in only_after if A[i]["u"]}
new_shared = {i for i in new_edges if A[i]["osm_highway"] in ("cycleway", "track")}
new_nodes = {i for i in only_after if A[i]["pt"] is not None}
nodes_of_new = {n for i in new_edges for n in (A[i]["u"], A[i]["v"])}
# A node is "touched" by a new edge when the edge ends on it: its smoothed height changes.
touched = set(nodes_of_new)
adj = {}
for i, r in A.items():
    if r["u"]:
        adj.setdefault(r["u"], set()).add(r["v"])
        adj.setdefault(r["v"], set()).add(r["u"])
# Heights are smoothed twice over edges under 5 m, so a changed neighbourhood
# reaches two hops; the edges at the third hop see a changed smoothed height.
near_new = set(touched)
for _ in range(3):
    near_new |= {m for n in list(near_new) for m in adj.get(n, ())}

# --- ramps, matched by survey id ---------------------------------------------
ramp_moved = {rid for rid in RB.keys() & RA.keys() if RB[rid] != RA[rid]}
ramp_same_values = sum(1 for rid in RB.keys() & RA.keys() if B[RB[rid]]["ramp"] == A[RA[rid]]["ramp"])
nodes_losing_ramp = {RB[rid] for rid in ramp_moved}
nodes_gaining_ramp = {RA[rid] for rid in ramp_moved}
near_ramp = set()  # a ramp's move does not change heights; only its node fields

# --- nodes ---------------------------------------------------------------------
node_group, node_examples = Counter(), {}
for i in shared:
    b, a = B[i], A[i]
    if b["pt"] is None:
        continue
    diffs = []
    if b["geom"] != a["geom"]:
        diffs.append("position")
    if b["rest"] != a["rest"]:
        diffs.append("other")
    if b["ramp"] != a["ramp"]:
        diffs.append("ramp fields")
    if b["kerb"] != a["kerb"]:
        diffs.append("kerb or elevator fields")
    if b["z"] != a["z"]:
        diffs.append("height")
    if not diffs:
        continue
    # A new edge on or beside a structure grows the region the deck rules
    # read, so a node near one can gain, lose or change a deck height.
    osm_only = a["kerb"].get("ext:source") == "osm_walk" and not a["ramp"].get("ext:ramp_id")
    on_ramp = a["ramp"].get("ext:ramp_id") and (a["kerb"].get("ext:osm_kerb") or a["kerb"].get("ext:osm_tactile_paving"))
    cause = ("ramp moved off this node" if i in nodes_losing_ramp else "ramp moved onto this node" if i in nodes_gaining_ramp
             else "osm kerb or elevator tag carried" if set(diffs) <= {"kerb or elevator fields", "ramp fields"} and osm_only
             else "osm kerb kept beside a surveyed ramp" if set(diffs) <= {"kerb or elevator fields"} and on_ramp
             else "height beside a new edge" if set(diffs) <= {"height"} and i in near_new
             else "unexplained")
    key = f"{cause}: {', '.join(diffs)}"
    node_group[key] += 1
    node_examples.setdefault(key, [])
    if len(node_examples[key]) < 6:
        node_examples[key].append([i, b["kerb"], a["kerb"], sorted(b["ramp"].items())[:3], sorted(a["ramp"].items())[:3], b["z"], a["z"]])

# --- edges ---------------------------------------------------------------------
edge_group, edge_examples = Counter(), {}
lifts = {i for i, r in A.items() if r["pt"] is not None and r["kerb"].get("ext:osm_highway") == "elevator"}
# An endpoint the near-miss merge now joins differently changes the smoothing
# around it too, so those ends are found first and their neighbourhood is
# a cause of its own.
moved_ends = set()
for i in shared:
    b, a = B[i], A[i]
    if b["u"] and (b["geom"] != a["geom"] or (b["u"], b["v"]) != (a["u"], a["v"])):
        moved_ends.update((a["u"], a["v"], b["u"], b["v"]))
near_moved = set(moved_ends)
for _ in range(3):
    near_moved |= {m for n in list(near_moved) for m in adj.get(n, ())}
for i in shared:
    b, a = B[i], A[i]
    if not b["u"]:
        continue
    diffs = []
    if b["geom"] != a["geom"]:
        diffs.append("geometry")
    if (b["u"], b["v"]) != (a["u"], a["v"]):
        diffs.append("endpoints")
    if b["rest"] != a["rest"]:
        diffs.append("other")
    if b["street"] != a["street"]:
        diffs.append("sidewalk tags")
    if b["incline"] != a["incline"] or b["unknown"] != a["unknown"]:
        diffs.append("incline")
    if not diffs:
        continue
    incline_only = set(diffs) <= {"incline", "sidewalk tags"}
    cause = ("street sidewalk tag" if set(diffs) <= {"sidewalk tags"}
             else "elevator at an end" if incline_only and (a["u"] in lifts or a["v"] in lifts)
             else "incline beside a new edge" if incline_only and (a["u"] in near_new or a["v"] in near_new)
             else "endpoint merge differs" if "geometry" in diffs or "endpoints" in diffs
             else "incline beside a merged endpoint" if incline_only and (a["u"] in near_moved or a["v"] in near_moved)
             else "unexplained")
    key = f"{cause}: {', '.join(diffs)} ({a['kind']})"
    edge_group[key] += 1
    edge_examples.setdefault(key, [])
    if len(edge_examples[key]) < 6:
        edge_examples[key].append([i, b["incline"], a["incline"], b["street"], a["street"]])
unexplained_edges = [i for i in shared if B[i]["u"] and (B[i]["incline"] != A[i]["incline"] or B[i]["unknown"] != A[i]["unknown"])
                     and B[i]["geom"] == A[i]["geom"] and (A[i]["u"], A[i]["v"]) == (B[i]["u"], B[i]["v"])
                     and not (A[i]["u"] in lifts or A[i]["v"] in lifts) and not (A[i]["u"] in near_new or A[i]["v"] in near_new)
                     and not (A[i]["u"] in near_moved or A[i]["v"] in near_moved)]
res = {
    "before_file": before_path, "after_file": after_path,
    "unexplained_incline_edges": {"count": len(unexplained_edges), "ids": unexplained_edges[:400],
                                  "largest_change": max((abs((A[i]["incline"] or 0) - (B[i]["incline"] or 0)) for i in unexplained_edges), default=0)},
    "features": {"before": len(B), "after": len(A), "shared": len(shared),
                 "only_before": len(only_before), "only_after": len(only_after)},
    "by_kind": {"before": dict(Counter(r["kind"] for r in B.values())), "after": dict(Counter(r["kind"] for r in A.values()))},
    "new_features": {"edges": len(new_edges), "of_those_cycleway_or_track": len(new_shared),
                     "new_edges_by_osm_highway": dict(Counter(A[i]["osm_highway"] or "none" for i in new_edges)),
                     "nodes": len(new_nodes), "of_those_on_a_new_edge": len(new_nodes & nodes_of_new),
                     "of_those_surveyed_ramps_at_a_new_node_id": len({i for i in new_nodes if A[i]["ramp"].get("ext:ramp_id")}),
                     "nodes_not_on_a_new_edge_and_not_a_ramp": len({i for i in new_nodes if i not in nodes_of_new and not A[i]["ramp"]})},
    "gone_features": {"count": len(only_before), "by_kind": dict(Counter(B[i]["kind"] for i in only_before)),
                      "of_those_surveyed_ramps_whose_node_id_changed": len({i for i in only_before if B[i]["ramp"].get("ext:ramp_id")}),
                      "examples": [[i, B[i]["kind"], B[i]["u"], B[i]["v"]] for i in list(only_before) if not B[i]["ramp"]][:10]},
    "ramps": {"before": len(RB), "after": len(RA), "matched_by_survey_id": len(RB.keys() & RA.keys()),
              "with_every_survey_field_equal": ramp_same_values, "node_changed": len(ramp_moved)},
    "nodes": {"shared_with_a_difference": sum(node_group.values()), "by_cause": dict(sorted(node_group.items())),
              "examples": node_examples},
    "edges": {"shared_with_a_difference": sum(edge_group.values()), "by_cause": dict(sorted(edge_group.items())),
              "examples": edge_examples,
              "geometry_same_on_shared_edges": sum(1 for i in shared if B[i]["u"] and B[i]["geom"] == A[i]["geom"]),
              "incline_same_on_shared_edges": sum(1 for i in shared if B[i]["u"] and B[i]["incline"] == A[i]["incline"] and B[i]["unknown"] == A[i]["unknown"]),
              "street_edges_with_sidewalk_tag": {"before": sum(1 for r in B.values() if r["street"]), "after": sum(1 for r in A.values() if r["street"])}},
    "zones": {"before": sum(r["kind"] == "pedestrian_zone" for r in B.values()), "after": sum(r["kind"] == "pedestrian_zone" for r in A.values()),
              "shared_and_equal": sum(1 for i in shared if B[i]["kind"] == "pedestrian_zone" and B[i]["geom"] == A[i]["geom"] and B[i]["rest"] == A[i]["rest"])},
    "note": "width, surface, names and every property not named in the script are covered by 'other'; "
            "ext:pipeline_version and ext:source_timestamp are per build and ignored",
}
with open(out_path, "w") as f:
    json.dump(res, f, indent=1)
print(json.dumps({k: v for k, v in res.items() if k not in ("by_kind",)}, indent=1, default=str)[:8000])
