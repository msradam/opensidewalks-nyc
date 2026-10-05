"""Compare v0.3.5 with v0.3.6 feature by feature, and say which fix moved what.

v0.3.6 changes heights and inclines only, so every feature of v0.3.5 should
still be there with the same geometry and the same other properties. Each
Node whose elevation differs and each Edge whose incline differs is put in
the first group below that fits it, and whatever fits none is listed.

Nodes:
  no_data     one of the four terrain pixels the Node is interpolated from is
              the terrain service's "no data" (needs DATA_DIR/raw/dem_nyc)
  tunnel      every Edge at the Node is a tunnel Edge
  deck        the Node has a deck height (`ext:elevation_source`) on either side
Edges:
  marked      v0.3.6 says `ext:incline_unknown`
  near_node   within three Edges of a Node whose elevation differs (heights
              are smoothed over Edges under 5 m, twice, so a changed height
              reaches its neighbours' neighbours)

usage: compare_v035.py V035.geojson[.gz] V036.geojson[.gz] DATA_DIR OUT.json
Streams both files; a few GB of memory for a city file.
"""
import gzip
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import ijson
import numpy as np
import rasterio

MOVED = ("incline", "ext:elevation_m", "ext:elevation_source", "ext:incline_unknown", "ext:pipeline_version",
         "ext:source_timestamp")


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
    return hw if hw in ("footway", "steps") else "road"


def scan(path):
    """{_id: (kind, geometry hash, hash of the properties not in MOVED, incline, elevation, elevation source,
    unknown mark, u, v, structure, lon, lat)}"""
    rec = {}
    with (gzip.open if str(path).endswith(".gz") else open)(path, "rb") as f:
        for ft in ijson.items(f, "features.item", use_float=True):
            p, g = ft["properties"], ft["geometry"]
            c = g["coordinates"]
            rec[p["_id"]] = (
                kind(p, g), hashlib.md5(json.dumps(c).encode()).digest()[:8],
                hashlib.md5(json.dumps({a: b for a, b in p.items() if a not in MOVED}, sort_keys=True).encode()).digest()[:8],
                p.get("incline"), p.get("ext:elevation_m"), p.get("ext:elevation_source"), p.get("ext:incline_unknown"),
                p.get("_u_id"), p.get("_v_id"), p.get("ext:structure"),
                c[0] if g["type"] == "Point" else None, c[1] if g["type"] == "Point" else None)
    return rec


def no_data_stencil(lon, lat, tiles):
    """True where one of the four pixels a point is interpolated from is exactly 0.0."""
    lon, lat = np.asarray(lon), np.asarray(lat)
    out, seen = np.zeros(len(lon), bool), np.zeros(len(lon), bool)
    for t in tiles:
        with rasterio.open(t) as src:
            b = src.bounds
            m = ~seen & (b.left <= lon) & (lon < b.right) & (b.bottom < lat) & (lat <= b.top)
            if not m.any():
                continue
            band = src.read(1)
            c, r = ~src.transform * (lon[m], lat[m])
            c0 = np.clip(np.floor(c - 0.5).astype(int), 0, band.shape[1] - 1)
            r0 = np.clip(np.floor(r - 0.5).astype(int), 0, band.shape[0] - 1)
            c1, r1 = np.clip(c0 + 1, 0, band.shape[1] - 1), np.clip(r0 + 1, 0, band.shape[0] - 1)
            out[m] = (band[r0, c0] == 0) | (band[r0, c1] == 0) | (band[r1, c0] == 0) | (band[r1, c1] == 0)
            seen |= m
    return out


before_path, after_path, data, out_path = sys.argv[1:5]
B = scan(before_path)
print("v0.3.5 scanned", len(B), flush=True)
A = scan(after_path)
print("v0.3.6 scanned", len(A), flush=True)
shared = B.keys() & A.keys()
res = {"before_file": before_path, "after_file": after_path, "properties_expected_to_move": list(MOVED),
       "features": {"before": len(B), "after": len(A), "shared": len(shared),
                    "only_before": len(B.keys() - A.keys()), "only_after": len(A.keys() - B.keys())},
       "by_kind": {"before": dict(Counter(r[0] for r in B.values())), "after": dict(Counter(r[0] for r in A.values()))},
       "geometry_differs": sum(B[i][1] != A[i][1] for i in shared),
       "kind_differs": sum(B[i][0] != A[i][0] for i in shared),
       "other_properties_differ": sum(B[i][2] != A[i][2] for i in shared)}

# Nodes whose height or its source differs.
tun = {i for i in A if A[i][7] and A[i][9] == "tunnel"}
tunnel_nodes = {n for i in tun for n in A[i][7:9]} - {n for i in A if A[i][7] and i not in tun for n in A[i][7:9]}
changed = [i for i in shared if B[i][10] is not None and (B[i][4] != A[i][4] or B[i][5] != A[i][5])]
stencil = dict(zip(changed, no_data_stencil([A[i][10] for i in changed], [A[i][11] for i in changed],
                                             sorted(Path(data, "raw/dem_nyc").glob("dem*.tif"))).tolist()))
node_group, node_examples = Counter(), {}
for i in changed:
    g = ("tunnel" if i in tunnel_nodes else "no_data" if stencil[i] and not (B[i][5] and A[i][5]) else
         "deck" if B[i][5] or A[i][5] else "unexplained")
    how = "height removed" if A[i][4] is None else "height added" if B[i][4] is None else "height changed"
    node_group[f"{g}: {how}"] += 1
    node_examples.setdefault(f"{g}: {how}", [])
    if len(node_examples[f"{g}: {how}"]) < 8:
        node_examples[f"{g}: {how}"].append([i, A[i][10], A[i][11], B[i][4], A[i][4], B[i][5], A[i][5]])
res["nodes"] = {"elevation_or_its_source_differs": len(changed), "by_cause": dict(sorted(node_group.items())),
                "examples [id, lon, lat, before, after, source before, source after]": node_examples,
                "nodes_only_on_tunnel_edges": len(tunnel_nodes),
                "exactly_zero": {"before": sum(r[4] == 0 for r in B.values()), "after": sum(r[4] == 0 for r in A.values())},
                "without_elevation": {"before": sum(r[10] is not None and r[4] is None for r in B.values()),
                                      "after": sum(r[10] is not None and r[4] is None for r in A.values())}}

# Edges whose incline differs.
near = set(changed)
adj = {}
for i, r in A.items():
    if r[7]:
        adj.setdefault(r[7], set()).add(r[8])
        adj.setdefault(r[8], set()).add(r[7])
for _ in range(2):
    near |= {m for n in list(near) for m in adj.get(n, ())}
edge_group, edge_examples = Counter(), {}
moved = [i for i in shared if B[i][7] and B[i][3] != A[i][3]]
for i in moved:
    b, a = B[i], A[i]
    g = ("marked ext:incline_unknown" if a[6] else "near a node whose height differs" if a[7] in near or a[8] in near
         else "unexplained")
    how = "incline removed" if a[3] is None else "incline added" if b[3] is None else "incline changed"
    key = f"{g}: {how} ({a[0]})"
    edge_group[key] += 1
    edge_examples.setdefault(f"{g}: {how}", [])
    if len(edge_examples[f"{g}: {how}"]) < 8:
        edge_examples[f"{g}: {how}"].append([i, b[3], a[3]])


def edges(rec):
    return [r for r in rec.values() if r[7]]


def steep(rec, plain_only):
    return sum(r[3] is not None and abs(r[3]) >= 0.5 and not (plain_only and (r[0] == "steps" or r[9])) for r in edges(rec))


marked = [i for i in A if A[i][6]]
res["edges"] = {
    "incline_differs": len(moved), "by_cause": dict(sorted(edge_group.items())),
    "examples [id, before, after]": edge_examples,
    "marked_ext:incline_unknown": {"after": len(marked), "by_kind": dict(Counter(A[i][0] for i in marked)),
                                    "by_structure": dict(Counter(A[i][9] or "none" for i in marked)),
                                    "what_v0.3.5_had": dict(Counter(
                                        "not in v0.3.5" if i not in B else "no incline (over 100%)" if B[i][3] is None
                                        else "an incline of 50% or more" if abs(B[i][3]) >= 0.5 else "an incline under 50%"
                                        for i in marked))},
    "at_50pct_or_steeper": {"before": steep(B, False), "after": steep(A, False)},
    "at_50pct_or_steeper_not_steps_not_on_a_structure": {"before": steep(B, True), "after": steep(A, True)},
    "at_50pct_or_steeper_not_steps": {"before": sum(r[3] is not None and abs(r[3]) >= 0.5 and r[0] != "steps" for r in edges(B)),
                                      "after": sum(r[3] is not None and abs(r[3]) >= 0.5 and r[0] != "steps" for r in edges(A))},
    "with_incline": {"before": sum(r[3] is not None for r in edges(B)), "after": sum(r[3] is not None for r in edges(A))},
    "incline_same_on_shared_edges": sum(1 for i in shared if B[i][7] and B[i][3] == A[i][3]),
    "incline_same_by_kind": dict(Counter(A[i][0] for i in shared if B[i][7] and B[i][3] == A[i][3])),
    "incline_differs_by_kind": dict(Counter(A[i][0] for i in moved))}
res["unchanged"] = {
    "curb_ramps": {"before": sum(r[0] == "curb_ramp" for r in B.values()), "after": sum(r[0] == "curb_ramp" for r in A.values()),
                   "shared_with_every_other_property_equal": sum(1 for i in shared if B[i][0] == "curb_ramp" and B[i][2] == A[i][2])},
    "note": "width, surface, names, ramp fields and every property not listed in properties_expected_to_move are "
            "covered by other_properties_differ; geometry by geometry_differs"}
json.dump(res, open(out_path, "w"), indent=1)
print(json.dumps({k: v for k, v in res.items() if k != "by_kind"}, indent=1)[:6000])
