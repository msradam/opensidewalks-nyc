"""Add each Pedestrian Zone's ring to the edge table of the post-build checks,
so the bridge test counts a plaza's outline as walkable, as v0.3.5's did.

usage: bridges_with_zones.py CHECKS.tables.pkl ZONES.geojson OUT.tables.pkl
The bridge test itself (bridges.py, in the working archive) then reads OUT.
"""
import json
import math
import pickle
import sys

import pandas as pd

N, E = pickle.load(open(sys.argv[1], "rb"))
xy = N.set_index("id")[["lon", "lat"]]
rows = []
for f in json.load(open(sys.argv[2]))["features"]:
    w = f["properties"]["_w_id"]
    for a, b in zip(w, w[1:] + w[:1]):
        (x0, y0), (x1, y1) = xy.loc[a], xy.loc[b]
        L = math.hypot((x1 - x0) * 111320 * math.cos(math.radians(y0)), (y1 - y0) * 111320)
        rows += [{"u": a, "v": b, "kind": "footway", "length": L}, {"u": b, "v": a, "kind": "footway", "length": L}]
E = pd.concat([E, pd.DataFrame(rows)], ignore_index=True)
pickle.dump((N, E), open(sys.argv[3], "wb"))
print("ring edges added:", len(rows))
