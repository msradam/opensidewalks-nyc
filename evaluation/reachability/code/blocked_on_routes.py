"""Which edges of each landmark route (wheelchair profile without its incline
limits) are outside the limits? usage: blocked_on_routes.py TABLES_PKL ROUTING_DIR OUT_JSON"""
import json, pickle, sys
import numpy as np, pandas as pd
_, N, E = pickle.load(open(sys.argv[1], "rb"))
paths = json.load(open(sys.argv[2] + "/route_paths.json"))
key = {(round(x0, 7), round(y0, 7), round(x1, 7), round(y1, 7)): i for i, (x0, y0, x1, y1) in enumerate(zip(E.x0, E.y0, E.x1, E.y1))}
src = N.set_index("_id")["ext:elevation_source"]; el = N.set_index("_id")["ext:elevation_m"]
out = {}
for k, r in paths.items():
    label, profile = k.split("|")
    if profile != "wheelchair, fixed, no incline limit":
        continue
    rows = []
    for seg, kind in zip(r["segments"], r["kinds"]):
        i = key.get((round(seg[0][0], 7), round(seg[0][1], 7), round(seg[-1][0], 7), round(seg[-1][1], 7)))
        if i is None:
            continue
        e = E.iloc[i]
        inc = e.incline
        if inc is not None and not (isinstance(inc, float) and np.isnan(inc)) and (inc > 0.083 or inc < -0.1):
            L = float(np.hypot((e.x1 - e.x0) * 84400, (e.y1 - e.y0) * 111320))
            rows.append({"incline": float(inc), "length_m": round(L, 1), "kind": kind, "name": None if pd.isna(e["name"]) else e["name"],
                         "structure": None if pd.isna(e["ext:structure"]) else e["ext:structure"],
                         "z_u": None if pd.isna(el.get(e._u_id)) else float(el[e._u_id]), "z_v": None if pd.isna(el.get(e._v_id)) else float(el[e._v_id]),
                         "src_u": None if pd.isna(src.get(e._u_id)) else src[e._u_id], "src_v": None if pd.isna(src.get(e._v_id)) else src[e._v_id],
                         "lon": round(float(e.x0), 5), "lat": round(float(e.y0), 5)})
    out[label] = {"edges": len(r["segments"]), "outside_limits": rows}
    print(label, "| edges", len(r["segments"]), "| outside limits:", len(rows))
    for row in rows:
        print("   ", row)
json.dump(out, open(sys.argv[3], "w"), indent=1)
