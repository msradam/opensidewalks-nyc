"""ORS on this graph's pedestrian edges only (arm C), against this graph's own wheelchair search.

Same data, same kerb rule, no roadway: what is left is the engine and the incline rule (ORS: 10% either way,
rounded to a whole percent; this profile: 8.3% up, 10% down). usage: python arm_c.py GRAPH_NPZ PAIRS OURS_PKL ARMC_JSONL_GZ OUT_JSON
"""
import gzip, json, pickle, statistics, sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from compare.graph import Graph
from compare.measures import line, near, overlap
npz, pairs_json, ours_pkl, armc, out = sys.argv[1:6]
g = Graph(npz)
pairs = json.load(open(pairs_json))["pairs"]
ours = pickle.load(open(ours_pkl, "rb"))
ors = {}
for row in gzip.open(armc, "rt"):
    r = json.loads(row)
    ors[(r["id"], r["config"])] = r
res = {}
for cfg in ("i10_k6", "rec_i10_k6"):
    by = defaultdict(lambda: {"table": Counter(), "ratio": [], "same": 0, "both": 0})
    for p in pairs:
        o, r = ours[p["id"]]["wheelchair"], ors[(p["id"], cfg)]
        a = by["/".join((p["set"], p["area"]))]
        a["table"][(o["edges"] is not None, r["found"])] += 1
        if o["edges"] and r["found"] and len(r["coords"]) > 1 and o["length_m"] > 0:
            mine, theirs = line(g.line(o["edges"], o["skip_first_m"], o["skip_last_m"])), line(r["coords"])
            if mine.length and theirs.length:
                a["both"] += 1
                a["ratio"].append(r["length_m"] / o["length_m"])
                a["same"] += min(overlap(mine, near(theirs)), overlap(theirs, near(mine))) >= 0.9
    res[cfg] = {k: {"both": v["table"][(True, True)], "ours_only": v["table"][(True, False)], "ors_only": v["table"][(False, True)], "neither": v["table"][(False, False)],
                    "found_agreement": round((v["table"][(True, True)] + v["table"][(False, False)]) / sum(v["table"].values()), 4),
                    "same_route_share_of_both_found": round(v["same"] / v["both"], 4) if v["both"] else None,
                    "median_length_ratio_ors_over_ours": round(statistics.median(v["ratio"]), 4) if v["ratio"] else None,
                    "share_within_1pct_length": round(sum(abs(x - 1) <= 0.01 for x in v["ratio"]) / len(v["ratio"]), 4) if v["ratio"] else None} for k, v in by.items()}
json.dump(res, open(out, "w"), indent=1)
for cfg, t in res.items():
    print(cfg)
    for k, v in t.items():
        print("  ", k, v)
