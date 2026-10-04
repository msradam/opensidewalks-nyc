"""Compare two builds of the dataset feature by feature, keyed on `_id`.

Written for v0.3.4 against v0.3.5, where pedestrian areas left the Edges and
became Pedestrian Zones. Reports the counts by entity on each side, which
ids exist on one side only (and, for Edges only in BEFORE, how many lie on
the ring of a zone in AFTER), and for shared features whether geometry,
incline, elevation or any other property differs. The fields a release is
expected to change are named in EXPECTED and counted as transitions.

usage: compare_versions.py BEFORE.geojson[.gz] AFTER.geojson[.gz] OUT.json
Loads each file whole: about 12 GB of memory for a city file.
"""
import gc
import gzip
import hashlib
import json
import sys
from collections import Counter

EXPECTED = ("ext:source", "ext:osm_highway", "ext:source_timestamp", "ext:pipeline_version")
TRACKED = ("ext:source", "ext:osm_highway")


def kind(p, g):
    if g["type"] == "Point":
        return "curb_ramp" if p.get("barrier") == "kerb" else "bare_node"
    if g["type"] == "Polygon":
        return "pedestrian_zone"
    hw, fw = p.get("highway"), p.get("footway")
    if hw == "footway" and fw in ("sidewalk", "crossing"):
        return fw
    if hw == "pedestrian":
        return "pedestrian_road"
    if hw in ("footway", "steps"):
        return hw
    return "road"


def scan(path):
    op = gzip.open if str(path).endswith(".gz") else open
    with op(path, "rt") as f:
        fc = json.load(f)
    root = {k: v for k, v in fc.items() if k not in ("features", "region")}
    feats = fc.pop("features")
    del fc
    gc.collect()
    s = {"root": root, "features": len(feats), "by_kind": Counter(), "curb_source": Counter(),
         "edge_osm_highway": Counter(), "zone_osm_highway": Counter(), "zone_ring_vertices": 0,
         "pipeline_version": Counter()}
    rec, ring_pairs, uv = {}, set(), {}
    while feats:
        f = feats.pop()
        p, g = f["properties"], f["geometry"]
        k = kind(p, g)
        s["by_kind"][k] += 1
        s["pipeline_version"][p.get("ext:pipeline_version")] += 1
        if k == "curb_ramp":
            s["curb_source"][p.get("ext:source")] += 1
        elif k == "pedestrian_zone":
            w = p["_w_id"]
            s["zone_osm_highway"][p.get("ext:osm_highway") or "pedestrian"] += 1
            s["zone_ring_vertices"] += len(w)
            ring_pairs.update(frozenset((w[i], w[(i + 1) % len(w)])) for i in range(len(w)))
        elif g["type"] == "LineString":
            s["edge_osm_highway"][p.get("ext:osm_highway")] += 1
            uv[p["_id"]] = (p["_u_id"], p["_v_id"], k)
        geom = hashlib.md5(json.dumps(g["coordinates"]).encode()).digest()[:8]
        other = hashlib.md5(json.dumps({a: b for a, b in sorted(p.items()) if a not in EXPECTED and a != "incline"
                                        and a != "ext:elevation_m"}, sort_keys=True).encode()).digest()[:8]
        rec[p["_id"]] = (geom, p.get("incline"), p.get("ext:elevation_m"), other, k,
                         tuple(p.get(a) for a in TRACKED))
    gc.collect()
    return s, rec, ring_pairs, uv


before_path, after_path, out_path = sys.argv[1:4]
sb, rb, _, uvb = scan(before_path)
print("before scanned", sb["features"], flush=True)
sa, ra, rings, _ = scan(after_path)
print("after scanned", sa["features"], flush=True)

only_b, only_a, shared = rb.keys() - ra.keys(), ra.keys() - rb.keys(), rb.keys() & ra.keys()
diff = {"only_before": len(only_b), "only_after": len(only_a), "shared": len(shared),
        "only_before_by_kind": dict(Counter(rb[i][4] for i in only_b).most_common()),
        "only_after_by_kind": dict(Counter(ra[i][4] for i in only_a).most_common()),
        "only_before_edges_on_a_zone_ring_in_after": dict(Counter(
            uvb[i][2] for i in only_b if i in uvb and frozenset(uvb[i][:2]) in rings).most_common()),
        "only_before_edges_not_on_a_zone_ring": dict(Counter(
            uvb[i][2] for i in only_b if i in uvb and frozenset(uvb[i][:2]) not in rings).most_common()),
        "geometry_differs": 0, "incline_differs": 0, "elevation_differs": 0, "kind_differs": 0,
        "other_properties_differ": 0, "transitions": {n: Counter() for n in TRACKED}}
examples = {"geometry": [], "incline": [], "elevation": [], "other": [], "only_before": sorted(only_b)[:10],
            "only_after": sorted(only_a)[:10],
            "only_before_not_on_ring": sorted(i for i in only_b if i in uvb and frozenset(uvb[i][:2]) not in rings)[:10]}
for i in shared:
    b, a = rb[i], ra[i]
    for name, j in (("geometry", 0), ("incline", 1), ("elevation", 2)):
        if b[j] != a[j]:
            diff[f"{name}_differs"] += 1
            if len(examples[name]) < 10:
                examples[name].append(i if j == 0 else [i, b[j], a[j]])
    if b[3] != a[3]:
        diff["other_properties_differ"] += 1
        if len(examples["other"]) < 10:
            examples["other"].append(i)
    diff["kind_differs"] += b[4] != a[4]
    for n, x, y in zip(TRACKED, b[5], a[5]):
        if x != y:
            diff["transitions"][n][f"{a[4]}: {x} -> {y}"] += 1


def plain(s):
    return {k: (dict(v.most_common()) if isinstance(v, Counter) else v) for k, v in s.items()}


diff["transitions"] = {n: dict(c.most_common(40)) for n, c in diff["transitions"].items()}
out = {"before_file": before_path, "after_file": after_path, "expected_to_change": list(EXPECTED),
       "before": plain(sb), "after": plain(sa), "diff": diff, "examples": examples}
json.dump(out, open(out_path, "w"), indent=1, default=str)
print(json.dumps(diff, indent=1, default=str)[:4000])
