"""Route every pair on this graph under each profile.

Each end is snapped to the nearest node a passable edge of that profile
touches, which is what the reference engines do with a coordinate. For the
random pairs the wheelchair profile is also run from the drawn nodes with no
snap ("wheelchair_as_drawn"), which is how reach.json counted.

usage: python compare/run_ours.py GRAPH_NPZ PAIRS_JSON OUT_PKL [PROCESSES]
"""
import pickle
import sys
from collections import defaultdict
from multiprocessing import get_context
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare.graph import Graph, read_json

PROFILES = ("wheelchair", "wheelchair_no_incline", "walk")
G = None


def _work(task):
    profile, s, targets = task
    return profile, s, targets, G.routes(s, targets, profile)


def main(npz, pairs_json, out, procs=8):
    global G
    G = Graph(npz)
    pairs = read_json(pairs_json)["pairs"]
    res = {p["id"]: {} for p in pairs}
    want = defaultdict(lambda: defaultdict(list))   # (profile, source) -> target -> [(pair, key)]
    for p in pairs:
        for prof in PROFILES:
            (s, ds), (t, dt) = G.snap(p["o"], prof), G.snap(p["d"], prof)
            res[p["id"]][prof] = {"snap_m": [round(float(ds), 1), round(float(dt), 1)], "edges": None}
            want[(prof, int(s))][int(t)].append((p["id"], prof))
        if "o_node" in p:
            res[p["id"]]["wheelchair_as_drawn"] = {"snap_m": [0.0, 0.0], "edges": None}
            want[("wheelchair", p["o_node"])][p["d_node"]].append((p["id"], "wheelchair_as_drawn"))
    for prof in PROFILES:
        G.csr(prof)     # build before the fork so the workers share it
    tasks = [(prof, s, list(ts)) for (prof, s), ts in want.items()]
    with get_context("fork").Pool(procs) as pool:   # fork: the workers share the loaded graph
        for n, (prof, s, targets, routes) in enumerate(pool.imap_unordered(_work, tasks, chunksize=8)):
            for t, edges in zip(targets, routes):
                for pid, key in want[(prof, s)][t]:
                    res[pid][key]["edges"] = edges
            if n % 2000 == 0:
                print(n, len(tasks), flush=True)
    with open(out, "wb") as f:
        pickle.dump(res, f)
    for key in PROFILES + ("wheelchair_as_drawn",):
        by = defaultdict(lambda: [0, 0])
        for p in pairs:
            if key in res[p["id"]]:
                by[(p["set"], p["area"])][0] += res[p["id"]][key]["edges"] is not None
                by[(p["set"], p["area"])][1] += 1
        print(key, {f"{a}/{b}": f"{k}/{n}" for (a, b), (k, n) in by.items()})


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], *(int(x) for x in sys.argv[4:5]))
