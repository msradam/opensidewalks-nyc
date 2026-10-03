"""Do OSM's kerb tags agree with this graph's ramp rule at crossing ends?

For every crossing end (a node where a crossing meets the sidewalk network): what the 5 m rule says
(a surveyed ramp within reach or not) against the kerb tag on the OSM node at that point, if any.

usage: python kerb_crosscheck.py OSW_GEOJSON PBF OUT_JSON
"""
import json, sys
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np, osmium
from scipy.spatial import cKDTree
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
from osw_to_unweaver import RAMP_REACH_M, crossing_ends

osw, pbf, out = sys.argv[1:4]
feats = json.load(open(osw))["features"]
curb, xy, boro = set(), {}, {}
for f in feats:
    p = f["properties"]
    if f["geometry"]["type"] == "Point":
        xy[p["_id"]] = f["geometry"]["coordinates"][:2]
        if p.get("barrier") == "kerb":
            curb.add(p["_id"])
    elif p.get("footway") == "crossing":
        boro[p["_u_id"]] = boro[p["_v_id"]] = p.get("ext:borough")
groups, ends = crossing_ends(feats, curb, xy, RAMP_REACH_M)
del feats

class Kerbs(osmium.SimpleHandler):
    def __init__(self):
        super().__init__(); self.pts, self.val = [], []
    def node(self, n):
        k = n.tags.get("kerb")
        if k:
            self.pts.append((n.location.lon, n.location.lat)); self.val.append(k)
h = Kerbs(); h.apply_file(pbf)
E, N = 111320 * np.cos(np.radians(40.7)), 111320
tree = cKDTree(np.array(h.pts) * [E, N])
state = {}
for g, d in ends.items():
    state.update(d)
nodes = list(state)
dist, idx = tree.query(np.array([xy[n] for n in nodes]) * [E, N])
table = defaultdict(Counter)
for n, d, i in zip(nodes, dist, idx):
    osm = h.val[i] if d <= 0.5 else "no kerb tag"
    osm = osm if osm in ("lowered", "flush", "raised", "rolled", "no", "yes", "no kerb tag") else "other"
    for area in (boro.get(n) or "?", "all"):
        table[area][("ramp within 5 m" if state[n] else "no ramp within 5 m", osm)] += 1
res = {"crossing_ends": len(nodes), "osm_kerb_nodes_in_extract": len(h.val), "by_area": {}}
for area, c in sorted(table.items()):
    row = {f"{a} | OSM {b}": v for (a, b), v in sorted(c.items())}
    ramp, none = sum(v for (a, _), v in c.items() if a.startswith("ramp")), sum(v for (a, _), v in c.items() if a.startswith("no ramp"))
    tagged_none = sum(v for (a, b), v in c.items() if a.startswith("no ramp") and b != "no kerb tag")
    row["summary"] = {
        "ends_with_a_surveyed_ramp_in_reach": ramp, "ends_without": none,
        "of_ends_with_a_ramp_osm_says_raised": c[("ramp within 5 m", "raised")],
        "of_ends_without_a_ramp_osm_says_lowered_or_flush": c[("no ramp within 5 m", "lowered")] + c[("no ramp within 5 m", "flush")],
        "share_of_ends_without_a_ramp_that_osm_tags_lowered_or_flush": round((c[("no ramp within 5 m", "lowered")] + c[("no ramp within 5 m", "flush")]) / none, 4) if none else None,
        "share_of_ends_without_a_ramp_with_any_osm_kerb_tag": round(tagged_none / none, 4) if none else None,
        "share_of_ends_with_a_ramp_that_osm_tags_raised": round(c[("ramp within 5 m", "raised")] / ramp, 4) if ramp else None,
    }
    res["by_area"][area] = row
json.dump(res, open(out, "w"), indent=1)
for area in res["by_area"]:
    print(area, res["by_area"][area]["summary"])
