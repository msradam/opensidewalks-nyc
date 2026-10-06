"""Count OSM tags relevant to v0.3.7 in the pinned extract (NYC bbox).

usage: scan_tags.py OUT_JSON  (run from the repository root, after Stage 1 has
written data/raw/osm_walk/osm_filtered.osm)
"""
import osmium, json, sys
from collections import Counter
W,S,E,N = -74.26, 40.49, -73.70, 40.92
STREETS = {"residential","service","tertiary","secondary","primary","living_street","unclassified"}
class H(osmium.SimpleHandler):
    def __init__(s):
        super().__init__()
        s.sw = Counter(); s.swside = Counter(); s.sw_by_hw = Counter(); s.hw_streets=Counter()
        s.cyc = Counter(); s.trk = Counter(); s.cyc_names = Counter(); s.elev_ways = Counter()
        s.node_kerb = Counter(); s.node_elev = 0; s.node_tp=Counter(); s.node_hw=Counter()
        s.elev_nodes = []
    def node(s, n):
        if not n.location.valid(): return
        if not (W <= n.location.lon <= E and S <= n.location.lat <= N): return
        t = n.tags
        if "kerb" in t or t.get("barrier") == "kerb":
            s.node_kerb[(t.get("barrier"), t.get("kerb"))] += 1
            if "tactile_paving" in t: s.node_tp[t["tactile_paving"]] += 1
        if t.get("highway") == "elevator":
            s.node_elev += 1
            if len(s.elev_nodes) < 400: s.elev_nodes.append((n.id, n.location.lon, n.location.lat, dict(t)))
        if "highway" in t: s.node_hw[t["highway"]] += 1
    def way(s, w):
        t = w.tags
        hw = t.get("highway")
        if hw is None: return
        if hw == "elevator": s.elev_ways[(t.get("foot"), t.get("wheelchair"))] += 1
        if hw in STREETS:
            s.hw_streets[hw] += 1
            s.sw[t.get("sidewalk")] += 1
            s.sw_by_hw[(hw, t.get("sidewalk"))] += 1
            side = tuple((k, t.get(k)) for k in ("sidewalk:left","sidewalk:right","sidewalk:both") if k in t)
            if side: s.swside[(t.get("sidewalk"), side)] += 1
        if hw in ("cycleway","track"):
            c = s.cyc if hw=="cycleway" else s.trk
            c[(t.get("foot"), t.get("oneway"), t.get("segregated"), t.get("access"))] += 1
            if hw=="cycleway" and t.get("foot") is None:
                s.cyc_names[t.get("name")] += 1
# Ways: the filtered regional XML lacks ways outside the filter (cycleway/track/streets are in it; elevator is not),
# so scan the PBF for ways too, bbox by first-node? Ways have no location in a plain pass; count all NY state
# for elevator (rare) and use the XML for the rest.
h = H()
h.apply_file("data/raw/osm_walk/new-york-261001.osm.pbf", locations=False)
print("STATE-WIDE elevator ways", dict(h.elev_ways))
h2 = H()
h2.apply_file("data/raw/osm_walk/osm_filtered.osm")
out = {
 "streets_by_highway": dict(h2.hw_streets),
 "sidewalk_values_on_streets": {str(k): v for k, v in h2.sw.most_common()},
 "sidewalk_by_highway": {str(k): v for k, v in h2.sw_by_hw.most_common(60)},
 "sidewalk_side_keys": {str(k): v for k, v in h2.swside.most_common(40)},
 "cycleway_(foot,oneway,segregated,access)": {str(k): v for k, v in h2.cyc.most_common(40)},
 "track_(foot,oneway,segregated,access)": {str(k): v for k, v in h2.trk.most_common(20)},
 "cycleway_nofoot_names": {str(k): v for k, v in h2.cyc_names.most_common(40)},
 "pbf_nyc_nodes_kerb": {str(k): v for k, v in h.node_kerb.most_common()},
 "pbf_nyc_nodes_tactile_on_kerb": dict(h.node_tp),
 "pbf_nyc_nodes_elevator": h.node_elev,
 "pbf_nyc_node_highway_values": dict(h.node_hw.most_common(15)),
 "elevator_nodes_sample": h.elev_nodes[:400],
}
json.dump(out, open(sys.argv[1], "w"), indent=1)
for k, v in out.items():
    if k != "elevator_nodes_sample": print(k, json.dumps(v)[:1500])
