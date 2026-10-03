"""For every end of every sampled crossing, what each matching rule can see:
the graph's curb nodes and the survey's ramps around it, with each ramp's
offset along and across the crossing's axis and the street it is on.

usage: features.py TABLES_PKL GROUPS_PKL RAW_RAMPS_GEOJSON SAMPLE_CSV OUT_JSON
"""
import json, math, pickle, re, sys
import numpy as np, pandas as pd
from scipy.spatial import cKDTree
from shapely.geometry import LineString

_, N, E = pickle.load(open(sys.argv[1], "rb"))
edges, ends, cr = pickle.load(open(sys.argv[2], "rb"))
S = pd.read_csv(sys.argv[4])
EAST = 111320 * math.cos(math.radians(40.7))
M = lambda lon, lat: np.c_[np.asarray(lon) * EAST, np.asarray(lat) * 111320]

curb = N[N.barrier == "kerb"]
on_graph = set(E._u_id) | set(E._v_id)
curb_tree = cKDTree(M(curb.lon, curb.lat))
curb_ids = set(curb._id)
raw = json.load(open(sys.argv[3]))["features"]
R = pd.DataFrame([{**f["properties"], "lon": f["geometry"]["coordinates"][0], "lat": f["geometry"]["coordinates"][1]} for f in raw])
raw_tree = cKDTree(M(R.lon, R.lat))
street = E[~E.highway.isin(["footway", "steps"])]
sx = M((street.x0 + street.x1) / 2, (street.y0 + street.y1) / 2)
street_tree = cKDTree(sx)


def norm(name):
    """Street names as OSM and DOT write them, reduced to a comparable key."""
    s = re.sub(r"[^A-Z0-9 ]", " ", str(name).upper())
    s = re.sub(r"\b(\d+)(ST|ND|RD|TH)\b", r"\1", s)
    for a, b in [("AVENUE", "AV"), ("AVE", "AV"), ("STREET", "ST"), ("BOULEVARD", "BLVD"), ("ROAD", "RD"), ("PLACE", "PL"),
                 ("DRIVE", "DR"), ("PARKWAY", "PKWY"), ("EAST", "E"), ("WEST", "W"), ("NORTH", "N"), ("SOUTH", "S"),
                 ("LANE", "LN"), ("COURT", "CT"), ("TERRACE", "TER"), ("EXPRESSWAY", "EXPY"), ("TURNPIKE", "TPKE")]:
        s = re.sub(rf"\b{a}\b", b, s)
    return " ".join(s.split())


out = []
for _, c in S.iterrows():
    eids = edges.loc[edges.crossing == c.crossing, "edge"].values
    line_pts = [p for e in eids for p in E.coords[e]]
    mid = M([(c.a_lon + c.b_lon) / 2], [(c.a_lat + c.b_lat) / 2])[0]
    # Streets this crossing passes over: street edges that cross any of its edges.
    near = street.iloc[street_tree.query_ball_point(mid, c.length_m / 2 + 60)]
    geoms = [LineString(E.coords[e]) for e in eids]
    crossed = sorted({norm(nm) for nm, co in zip(near.name, near.coords) if isinstance(nm, str)
                      and any(LineString(co).intersects(g) for g in geoms)})
    for end, other in (("A", "B"), ("B", "A")):
        p = M([c[f"{end.lower()}_lon"]], [c[f"{end.lower()}_lat"]])[0]
        q = M([c[f"{other.lower()}_lon"]], [c[f"{other.lower()}_lat"]])[0]
        axis = (q - p) / max(np.hypot(*(q - p)), 1e-9)          # from this end into the street
        d_graph, _ = curb_tree.query(p)
        cands = []
        for j in raw_tree.query_ball_point(p, 15):
            off = M([R.lon[j]], [R.lat[j]])[0] - p
            cands.append({"ramp_id": R.rampid[j], "corner_id": R.cornerid[j], "dist": round(float(np.hypot(*off)), 2),
                          "along": round(float(off @ axis), 2), "across": round(float(abs(off[0] * axis[1] - off[1] * axis[0])), 2),
                          "on_street": norm(R.ramp_onstr[j]), "streets": [norm(R.stname1[j]), norm(R.stname2[j])]})
        node = c[f"{end.lower()}_node"]
        out.append({"n": int(c.n), "end": end, "borough": c.borough, "length_m": c.length_m, "node": node,
                    "node_is_curb": node in curb_ids, "nearest_graph_curb_m": round(float(d_graph), 2),
                    "crossed_streets": crossed, "ramps": sorted(cands, key=lambda r: r["dist"])})
json.dump(out, open(sys.argv[5], "w"), indent=1)
F = pd.DataFrame(out)
print(len(F), "ends;", "node is curb:", int(F.node_is_curb.sum()), "| graph curb within 5 m:", int((F.nearest_graph_curb_m <= 5).sum()),
      "| has a surveyed ramp within 15 m:", int((F.ramps.str.len() > 0).sum()), "| crossed street named:", int((F.crossed_streets.str.len() > 0).sum()))
