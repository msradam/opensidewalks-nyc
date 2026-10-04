"""Reachability of the same node pairs on two versions of the graph.

evaluation/reachability/code/reach.py draws its 2,000 pairs per borough from
each borough's pool of nodes on walkable edges. A release that changes which
nodes are in a pool changes the pairs drawn, so two runs of reach.py are not
the same trips. This script draws the pairs exactly as reach.py does and
writes them as node ids (--dump-pairs), or routes pairs given as node ids
(--use-pairs). A pair with a node this graph lacks counts as no route.

The profiles and the search are reach.py's; the landmark routes are left out.

usage: reach_same_pairs.py OSW_GEOJSON OUTDIR (--dump-pairs FILE | --use-pairs FILE)
"""
import importlib.util, json, math, subprocess, sys
from collections import Counter, deque
from pathlib import Path
import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

root = Path(__file__).resolve().parents[3]
osw, outdir, mode, pairs_file = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4]
PAIRS, SEED = 2000, 20261002
outdir.mkdir(parents=True, exist_ok=True)
layer = outdir / "layers/transportation.geojson"
subprocess.run([sys.executable, str(root / "scripts/osw_to_unweaver.py"), "--input", str(osw),
                "--output-layer", str(layer), "--output-region", str(outdir / "regions.geojson")], check=True)

def load_cost(path):
    spec = importlib.util.spec_from_file_location(path.stem.replace("-", "_"), path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m.cost_fun_generator
fixed = load_cost(root / "unweaver-project/cost-wheelchair.py")

props = [f["properties"] for f in json.load(open(layer))["features"]]
ids, inv = np.unique(np.array([p["_u"] for p in props] + [p["_v"] for p in props]), return_inverse=True)
m = len(props); u, v = inv[:m], inv[m:]; n = len(ids)
subclass = np.array([p["subclass"] for p in props])
borough = np.array([p["ext_borough"] or "" for p in props])

def mask_of(gen, **kw):
    fn = gen(None, **kw)
    return np.array([fn(p["_u"], p["_v"], p) is not None for p in props])
profiles = {
    "distance (every edge)": np.ones(m, bool),
    "walk (no street centrelines)": subclass != "street",
    "wheelchair, fixed": mask_of(fixed),
    "wheelchair, fixed, no incline limit": mask_of(fixed, uphill=math.inf, downhill=-math.inf),
    "no steps, no streets, no curb or incline rule": ~np.isin(subclass, ["steps", "street"]),
}
out = {"input": str(osw), "edges": m, "nodes": n, "pairs_per_borough": PAIRS, "seed": SEED,
       "edges_by_subclass": dict(Counter(subclass.tolist())), "profiles": {}}

# Sample pools and pairs, as in reach.py.
walkable = (subclass != "street") & (subclass != "steps")
rng = np.random.default_rng(SEED)
pools = {}
for b in sorted(set(borough[walkable].tolist()) - {""}):
    e = walkable & (borough == b)
    pools[b] = np.unique(np.r_[u[e], v[e]])
pools["citywide"] = np.unique(np.r_[u[walkable], v[walkable]])
pairs = {b: (rng.choice(p, PAIRS), rng.choice(p, PAIRS)) for b, p in pools.items()}
out["sample_pool_nodes"] = {b: int(len(p)) for b, p in pools.items()}
if mode == "--dump-pairs":
    json.dump({b: [ids[ss].tolist(), ids[tt].tolist()] for b, (ss, tt) in pairs.items()}, open(pairs_file, "w"))
    out["pairs"] = "drawn here and written to " + Path(pairs_file).name
else:
    index = {i: k for k, i in enumerate(ids.tolist())}
    pairs = {b: (np.array([index.get(i, -1) for i in ss]), np.array([index.get(i, -1) for i in tt]))
             for b, (ss, tt) in json.load(open(pairs_file)).items()}
    out["pairs"] = "read from " + Path(pairs_file).name
    out["pairs_with_a_node_not_in_this_graph"] = {b: int(((ss < 0) | (tt < 0)).sum()) for b, (ss, tt) in pairs.items()}

def reach_fn(mask):
    g = coo_matrix((np.ones(int(mask.sum())), (u[mask], v[mask])), shape=(n, n)).tocsr()
    k, lab = connected_components(g, directed=True, connection="strong")
    cu, cv = lab[u[mask]], lab[v[mask]]; d = cu != cv
    fwd, back = {}, {}
    for a, b in set(zip(cu[d].tolist(), cv[d].tolist())):
        fwd.setdefault(a, []).append(b); back.setdefault(b, []).append(a)
    giant = int(np.bincount(lab).argmax())
    def closure(adj, start, stop=None):
        seen = {start}; q = deque([start])
        while q:
            x = q.popleft()
            for y in adj.get(x, ()):
                if y not in seen and y != stop:
                    seen.add(y); q.append(y)
        return seen
    below, above = closure(fwd, giant), closure(back, giant)
    def reach(s, t):
        if s < 0 or t < 0:
            return False
        ls, lt = int(lab[s]), int(lab[t])
        if ls == lt or (ls in above and lt in below):
            return True
        return lt in closure(fwd, ls, stop=giant)
    return reach

def wilson(k, nn):
    z = 1.96; p = k / nn; den = 1 + z * z / nn
    c = (p + z * z / (2 * nn)) / den; h = z * math.sqrt(p * (1 - p) / nn + z * z / (4 * nn * nn)) / den
    return [round(c - h, 4), round(c + h, 4)]
for name, mask in profiles.items():
    reach = reach_fn(mask)
    res = {"edges_passing": int(mask.sum()), "reachable": {}, "routed": {}}
    for b, (ss, tt) in pairs.items():
        hits = [bool(reach(s, t)) for s, t in zip(ss, tt)]
        res["reachable"][b] = {"pairs": PAIRS, "with_a_route": sum(hits), "share": round(sum(hits) / PAIRS, 4),
                               "ci95": wilson(sum(hits), PAIRS)}
        res["routed"][b] = "".join("1" if h else "0" for h in hits)
    out["profiles"][name] = res
    print(name, json.dumps(res["reachable"]), flush=True)
json.dump(out, open(outdir / "reach_same_pairs.json", "w"), indent=1)
