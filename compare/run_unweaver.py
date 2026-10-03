"""Check this repo's re-implemented search against Unweaver itself.

Both run on the same layer with the same cost function. Each pair's ends are
snapped to the nearest node the profile can use, Unweaver is asked for the
route between those node coordinates, and the two routes are compared by
length and by the share of each lying within 1 m of the other.

usage: python compare/run_unweaver.py BASE_URL GRAPH_NPZ PAIRS_JSON SET OUT_JSON
"""
import json
import sys
from pathlib import Path

import requests
from shapely.geometry import LineString

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare.graph import Graph, metres, read_json, write_json

PROFILES = {"wheelchair": ("wheelchair", {"avoidCurbs": "true", "uphill": "0.083", "downhill": "-0.1"}),
            "distance": ("distance", {})}


def main(base, npz, pairs_json, which, out):
    assert base.startswith(("http://localhost", "http://127.0.0.1")), "local engines only"
    g = Graph(npz)
    pairs = [p for p in read_json(pairs_json)["pairs"] if p["set"] == which]
    rows = []
    for name, (profile, args) in PROFILES.items():
        for p in pairs:
            (s, _), (t, _) = g.snap(p["o"], name), g.snap(p["d"], name)
            if s == t:
                continue
            (x1, y1), (x2, y2) = g.xy[s], g.xy[t]
            edges = g.routes(s, [t], name)[0]
            j = requests.get(f"{base}/shortest_path/{profile}.json", timeout=120,
                             params={"lon1": x1, "lat1": y1, "lon2": x2, "lat2": y2, **args}).json()
            row = {"id": p["id"], "profile": name, "ours_found": edges is not None, "unweaver_status": j.get("status") or j.get("code")}
            if edges is not None and j.get("status") == "Ok":
                ours = LineString(metres(g.line(edges)))
                theirs = LineString(metres([c for e in j["edges"] for c in e["geom"]["coordinates"]]))
                row.update(ours_m=round(float(g.length[edges].sum()), 1), unweaver_m=round(sum(e["length"] for e in j["edges"]), 1),
                           ours_within_1m=round(ours.intersection(theirs.buffer(1)).length / ours.length, 3),
                           unweaver_within_1m=round(theirs.intersection(ours.buffer(1)).length / theirs.length, 3))
            rows.append(row)
    both = [r for r in rows if "ours_m" in r]
    summary = {
        "pairs_asked": len(rows),
        "found_agrees": sum(r["ours_found"] == (r["unweaver_status"] == "Ok") for r in rows),
        "both_found": len(both),
        "length_within_1m": sum(abs(r["ours_m"] - r["unweaver_m"]) <= 1 for r in both),
        "same_path_99pct_both_ways": sum(min(r["ours_within_1m"], r["unweaver_within_1m"]) >= 0.99 for r in both),
        "largest_length_difference_m": max((abs(r["ours_m"] - r["unweaver_m"]) for r in both), default=None),
        "found_disagreements": [r for r in rows if r["ours_found"] != (r["unweaver_status"] == "Ok")][:20],
        "length_disagreements": sorted((r for r in both if abs(r["ours_m"] - r["unweaver_m"]) > 1), key=lambda r: -abs(r["ours_m"] - r["unweaver_m"]))[:20],
    }
    write_json({"summary": summary, "rows": rows}, out, indent=1)
    print(json.dumps({k: v for k, v in summary.items() if not k.endswith("disagreements")}, indent=1))


if __name__ == "__main__":
    main(*sys.argv[1:6])
