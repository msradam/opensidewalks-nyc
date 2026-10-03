"""Routing check on a built OSW file, using the repo's own Unweaver inputs.

Unweaver itself needs mod_spatialite, which is not installed here (a global
install). So this runs scripts/osw_to_unweaver.py to make the layer Unweaver
would read, applies unweaver-project/cost-wheelchair.py edge by edge, and
does the graph search with scipy. Only the search is re-implemented.

usage: reach.py OSW_GEOJSON OUTDIR [PAIRS_PER_BOROUGH]
"""
import importlib.util, json, math, subprocess, sys
from collections import Counter, deque
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components, dijkstra

root = Path(__file__).resolve().parents[3]
osw, outdir = Path(sys.argv[1]), Path(sys.argv[2])
PAIRS = int(sys.argv[3]) if len(sys.argv) > 3 else 2000
SEED = 20261002
outdir.mkdir(parents=True, exist_ok=True)
layer = outdir / "layers/transportation.geojson"
subprocess.run([sys.executable, str(root / "scripts/osw_to_unweaver.py"), "--input", str(osw),
                "--output-layer", str(layer), "--output-region", str(outdir / "regions.geojson")], check=True)

def load_cost(path):
    spec = importlib.util.spec_from_file_location(path.stem.replace("-", "_"), path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m.cost_fun_generator
fixed = load_cost(root / "unweaver-project/cost-wheelchair.py")
shipped = load_cost(Path(__file__).parent / "cost_wheelchair_as_shipped.py")

feats = json.load(open(layer))["features"]
props = [f["properties"] for f in feats]
ids, inv = np.unique(np.array([p["_u"] for p in props] + [p["_v"] for p in props]), return_inverse=True)
m = len(props); u, v = inv[:m], inv[m:]; n = len(ids)
length = np.array([p["length"] or 0.0 for p in props])
subclass = np.array([p["subclass"] for p in props])
footway = np.array([p["footway"] or "" for p in props])
kind = np.where(subclass == "footway", np.where(footway == "", "footway", footway), subclass)
borough = np.array([p["ext_borough"] or "" for p in props])
xy = np.zeros((n, 2))
for f, a, b in zip(feats, u, v):
    c = f["geometry"]["coordinates"]; xy[a] = c[0]; xy[b] = c[-1]

def mask_of(gen, **kw):
    fn = gen(None, **kw)
    return np.array([fn(p["_u"], p["_v"], p) is not None for p in props])
profiles = {
    "distance (every edge)": np.ones(m, bool),
    "walk (no street centrelines)": subclass != "street",
    "wheelchair as shipped in v0.3.1": mask_of(shipped),
    "wheelchair, fixed": mask_of(fixed),
    "wheelchair, fixed, no incline limit": mask_of(fixed, uphill=math.inf, downhill=-math.inf),
    "no steps, no streets, no curb or incline rule": ~np.isin(subclass, ["steps", "street"]),
}
out = {"input": str(osw), "edges": m, "nodes": n, "pairs_per_borough": PAIRS, "seed": SEED,

       "edges_by_kind": dict(Counter(kind.tolist())), "profiles": {}}
ped = subclass != "street"
inc = np.array([np.nan if p["incline"] is None else p["incline"] for p in props])
cross = kind == "crossing"
curbr = np.array([bool(p["curbramps"]) for p in props])
out["crossings"] = {"edges": int(cross.sum()), "with_curb_node_at_an_end": int((cross & curbr).sum()),
                    "share": round(float((cross & curbr).sum() / max(cross.sum(), 1)), 4)}
walkable = ped & (subclass != "steps")
steep = (inc > 0.083) | (inc < -0.1)
out["incline"] = {"walkable_edges": int(walkable.sum()), "with_incline": int((walkable & ~np.isnan(inc)).sum()),
                  "outside_limits": int((walkable & steep).sum()),
                  "share_outside_limits": round(float((walkable & steep).sum() / walkable.sum()), 4),
                  "exactly_zero": int((walkable & (inc == 0)).sum())}

# Sample pools: nodes on a walkable (non-steps pedestrian) edge, by the edge's borough.
rng = np.random.default_rng(SEED)
pools = {}
for b in sorted(set(borough[walkable].tolist()) - {""}):
    e = walkable & (borough == b)
    pools[b] = np.unique(np.r_[u[e], v[e]])
pools["citywide"] = np.unique(np.r_[u[walkable], v[walkable]])
pairs = {b: (rng.choice(p, PAIRS), rng.choice(p, PAIRS)) for b, p in pools.items()}
out["sample_pool_nodes"] = {b: int(len(p)) for b, p in pools.items()}

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
        ls, lt = int(lab[s]), int(lab[t])
        if ls == lt or (ls in above and lt in below):
            return True
        return lt in closure(fwd, ls, stop=giant)
    sizes = np.bincount(lab)
    return reach, {"largest_strong_component_nodes": int(sizes.max())}

def wilson(k, nn):
    z = 1.96; p = k / nn; den = 1 + z * z / nn
    c = (p + z * z / (2 * nn)) / den; h = z * math.sqrt(p * (1 - p) / nn + z * z / (4 * nn * nn)) / den
    return [round(c - h, 4), round(c + h, 4)]
for name, mask in profiles.items():
    reach, info = reach_fn(mask)
    res = {"edges_passing": int(mask.sum()), "by_kind": dict(Counter(kind[mask].tolist())), **info, "reachable": {}}
    for b, (ss, tt) in pairs.items():
        k = sum(reach(s, t) for s, t in zip(ss, tt))
        res["reachable"][b] = {"pairs": PAIRS, "with_a_route": int(k), "share": round(k / PAIRS, 4), "ci95": wilson(k, PAIRS)}
    out["profiles"][name] = res
    print(name, json.dumps(res["reachable"]), flush=True)

# Landmark routes. Unweaver snaps a waypoint to the nearest edge its profile can use, so for each
# profile snap to the nearest node, in the landmark's own borough, that a passable edge touches.
sys.path.insert(0, str(root / "scripts"))
from route_test import LANDMARKS, ROUTES
node_boro = {}
for a_, b_, bo in zip(u, v, borough):
    node_boro.setdefault(int(a_), set()).add(bo); node_boro.setdefault(int(b_), set()).add(bo)
def snapper(mask):
    touched = np.unique(np.r_[u[mask], v[mask]])
    def snap(lat, lon, boro):
        cand = np.array([i for i in touched if boro in node_boro[int(i)]], dtype=int)
        if len(cand) == 0:
            return None, None
        d = np.hypot((xy[cand, 0] - lon) * 111320 * math.cos(math.radians(lat)), (xy[cand, 1] - lat) * 111320)
        return int(cand[d.argmin()]), round(float(d.min()), 1)
    return snap
snaps_by_profile = {name: [snapper(mask)(lat, lon, bo) for _, lat, lon, bo in LANDMARKS] for name, mask in profiles.items()}
out["landmarks"] = [{"name": l[0], "borough": l[3], "snap_m": {name: s[i][1] for name, s in snaps_by_profile.items()}} for i, l in enumerate(LANDMARKS)]
paths = {}; out["routes"] = []
for a, b, label in ROUTES:
    la, lb = LANDMARKS[a], LANDMARKS[b]
    row = {"route": label, "straight_line_m": round(float(np.hypot((la[2] - lb[2]) * 84400, (la[1] - lb[1]) * 111320)))}
    for name, mask in profiles.items():
        s, t = snaps_by_profile[name][a][0], snaps_by_profile[name][b][0]
        if s is None or t is None:
            row[name] = "no passable edge in the borough"; continue
        # One weight per directed pair (the shortest): a sparse matrix would add parallel edges together.
        dd = pd.DataFrame({"a": u[mask], "b": v[mask], "w": length[mask]}).groupby(["a", "b"], as_index=False).w.min()
        g = coo_matrix((dd.w.values + 1e-9, (dd.a.values, dd.b.values)), shape=(n, n)).tocsr()
        dist, pred = dijkstra(g, directed=True, indices=s, return_predecessors=True)
        if not np.isfinite(dist[t]):
            row[name] = "NoPath"; continue
        seq = [t]
        while seq[-1] != s:
            seq.append(int(pred[seq[-1]]))
        seq.reverse()
        # Edge lookup for the path: cheapest passing edge between consecutive nodes.
        kinds, total, by, coords = [], 0.0, Counter(), []
        for a_, b_ in zip(seq[:-1], seq[1:]):
            cand = np.flatnonzero((u == a_) & (v == b_) & mask)
            e = cand[length[cand].argmin()]
            kinds.append(str(kind[e])); total += length[e]; by[str(kind[e])] += length[e]
            coords.append(feats[e]["geometry"]["coordinates"])
        row[name] = {"length_m": round(total), "edges": len(kinds), "snap_m": [snaps_by_profile[name][a][1], snaps_by_profile[name][b][1]],
                     "share_by_kind": {k: round(v_ / total, 3) for k, v_ in sorted(by.items())},
                     "detour_ratio": round(total / max(row["straight_line_m"], 1), 2)}
        paths[f"{label}|{name}"] = {"segments": coords, "kinds": kinds}
    out["routes"].append(row)
    print(label, json.dumps({k: (v_.get("length_m") if isinstance(v_, dict) else v_) for k, v_ in row.items() if k != "route"}), flush=True)
json.dump(out, open(outdir / "reach.json", "w"), indent=1)
json.dump(paths, open(outdir / "route_paths.json", "w"))
