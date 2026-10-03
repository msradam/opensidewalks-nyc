"""Route every pair on this graph under each profile.

Each end is snapped to the nearest point on an edge that profile can use,
which is what the reference engines do with a coordinate. For the random
pairs the wheelchair profile, and the same profile with no incline limit,
are also run from the drawn nodes with no snap ("wheelchair_as_drawn",
"wheelchair_no_incline_as_drawn"), which is how the reachability check
(evaluation/reachability/reach.json) counted.

usage: python compare/run_ours.py GRAPH_NPZ PAIRS_JSON OUT_PKL [PROCESSES]
"""
import pickle
import sys
from collections import defaultdict
from multiprocessing import get_context
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare.graph import Graph, read_json

PROFILES = ("wheelchair", "walk")
AS_DRAWN = ("wheelchair_as_drawn", "wheelchair_no_incline_as_drawn")
G = None


def _work(task):
    i, prof, o, d = task
    if prof.endswith("_as_drawn"):
        edges = G.routes(o, [d], prof[:-9])[0]
        if edges is None:
            return i, prof, {"edges": None, "snap_m": [0.0, 0.0]}
        return i, prof, {"edges": edges, "skip_first_m": 0.0, "skip_last_m": 0.0, "length_m": round(float(G.length[edges].sum()), 1), "snap_m": [0.0, 0.0]}
    return i, prof, G.route(o, d, prof)


def main(npz, pairs_json, out, procs=8):
    global G
    G = Graph(npz)
    pairs = read_json(pairs_json)["pairs"]
    tasks = [(i, prof, p["o"], p["d"]) for i, p in enumerate(pairs) for prof in PROFILES]
    tasks += [(i, prof, p["o_node"], p["d_node"]) for i, p in enumerate(pairs) if "o_node" in p for prof in AS_DRAWN]
    for prof in PROFILES:       # build the matrices and the snap indexes before the fork so the workers share them
        G.csr(prof)
        G.snap_edge(pairs[0]["o"], prof)
    G.csr("wheelchair_no_incline")
    res = {p["id"]: {} for p in pairs}
    with get_context("fork").Pool(procs) as pool:
        for n, (i, prof, r) in enumerate(pool.imap_unordered(_work, tasks, chunksize=16)):
            res[pairs[i]["id"]][prof] = r
            if n % 4000 == 0:
                print(n, len(tasks), flush=True)
    with open(out, "wb") as f:
        pickle.dump(res, f)
    for key in (*PROFILES, *AS_DRAWN):
        by = defaultdict(lambda: [0, 0])
        for p in pairs:
            if key in res[p["id"]]:
                by[(p["set"], p["area"])][0] += res[p["id"]][key]["edges"] is not None
                by[(p["set"], p["area"])][1] += 1
        print(key, {f"{a}/{b}": f"{k}/{n}" for (a, b), (k, n) in by.items()})


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], *(int(x) for x in sys.argv[4:5]))
