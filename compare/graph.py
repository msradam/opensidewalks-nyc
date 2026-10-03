"""This graph as arrays, and routes on it under the repo's own cost function.

The input is the layer scripts/osw_to_unweaver.py writes (the one Unweaver
reads). `build` turns it into one .npz; `Graph` loads that, applies
unweaver-project/cost-wheelchair.py edge by edge to get each profile's
passable edges, and searches with scipy. Only the search is re-implemented.

usage: python compare/graph.py LAYER_GEOJSON OUT_NPZ
"""
import importlib.util
import json
import math
import sys
from itertools import pairwise
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[1]
# One scale for the whole city, as in scripts/osw_to_unweaver.py.
EAST = 111320 * math.cos(math.radians(40.7))
NORTH = 111320.0
UPHILL, DOWNHILL = 0.083, -0.1


def read_json(path):
    with open(path) as f:
        return json.load(f)


def write_json(obj, path, **kw):
    with open(path, "w") as f:
        json.dump(obj, f, **kw)


def metres(lonlat):
    a = np.asarray(lonlat, dtype=float)
    return a * np.array([EAST, NORTH])


def _cost_generator():
    path = ROOT / "unweaver-project/cost-wheelchair.py"
    spec = importlib.util.spec_from_file_location("cost_wheelchair", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.cost_fun_generator


def build(layer, out):
    feats = read_json(layer)["features"]
    props = [f["properties"] for f in feats]
    m = len(props)
    ids, inv = np.unique(np.array([p["_u"] for p in props] + [p["_v"] for p in props]), return_inverse=True)
    u, v = inv[:m].astype(np.int32), inv[m:].astype(np.int32)
    subclass = np.array([p["subclass"] for p in props])
    footway = np.array([p["footway"] or "" for p in props])
    kind = np.where(subclass == "footway", np.where(footway == "", "footway", footway), subclass)
    gen = _cost_generator()

    def mask_of(**kw):
        fn = gen(None, **kw)
        return np.array([fn(p["_u"], p["_v"], p) is not None for p in props])

    counts = np.array([len(f["geometry"]["coordinates"]) for f in feats])
    coords = np.array([c[:2] for f in feats for c in f["geometry"]["coordinates"]], dtype=float)
    offsets = np.r_[0, np.cumsum(counts)]
    xy = np.zeros((len(ids), 2))
    xy[u] = coords[offsets[:-1]]
    xy[v] = coords[offsets[1:] - 1]
    np.savez_compressed(
        out, ids=ids, u=u, v=v, kind=kind, xy=xy, coords=coords, offsets=offsets,
        fid=np.array([p["fid"] for p in props]),
        length=np.array([p["length"] or 0.0 for p in props]),
        incline=np.array([np.nan if p["incline"] is None else p["incline"] for p in props]),
        width=np.array([np.nan if p["width"] is None else p["width"] for p in props]),
        curbramps=np.array([bool(p["curbramps"]) for p in props]),
        borough=np.array([p["ext_borough"] or "" for p in props]),
        surface=np.array([p["surface"] or "" for p in props]),
        name=np.array([p["description"] if not str(p["description"]).startswith(p["subclass"]) else "" for p in props]),
        osm_id=np.array([int(p["ext_osm_id"]) if str(p["ext_osm_id"] or "").isdigit() else -1 for p in props]),
        wheelchair=mask_of(), wheelchair_no_incline=mask_of(uphill=math.inf, downhill=-math.inf))


class Graph:
    """Profiles: wheelchair (the repo's rule), wheelchair_no_incline, walk
    (pedestrian edges, steps allowed), distance (every edge)."""

    def __init__(self, npz):
        d = np.load(npz)
        for k in d.files:
            setattr(self, k, d[k])
        self.n, self.m = len(self.ids), len(self.u)
        self.masks = {"wheelchair": self.wheelchair, "wheelchair_no_incline": self.wheelchair_no_incline,
                      "walk": self.kind != "street", "distance": np.ones(self.m, bool)}
        self._csr, self._tree, self._best = {}, {}, {}

    def edge_coords(self, e):
        return self.coords[self.offsets[e]:self.offsets[e + 1]]

    def csr(self, profile):
        if profile not in self._csr:
            k = self.masks[profile]
            # One weight per directed node pair, the shortest: a sparse matrix
            # would add parallel edges together.
            dd = pd.DataFrame({"a": self.u[k], "b": self.v[k], "w": self.length[k], "e": np.flatnonzero(k)})
            dd = dd.sort_values("w").drop_duplicates(["a", "b"])
            self._csr[profile] = coo_matrix((dd.w.values + 1e-9, (dd.a.values, dd.b.values)), shape=(self.n, self.n)).tocsr()
            self._best[profile] = dict(zip(zip(dd.a.tolist(), dd.b.tolist()), dd.e.tolist()))
        return self._csr[profile]

    def snap(self, lonlat, profile):
        """Nearest node a passable edge touches, and the distance in metres."""
        if profile not in self._tree:
            k = self.masks[profile]
            nodes = np.unique(np.r_[self.u[k], self.v[k]])
            self._tree[profile] = (cKDTree(metres(self.xy[nodes])), nodes)
        tree, nodes = self._tree[profile]
        d, i = tree.query(metres(lonlat))
        return nodes[i], d

    def routes(self, s, targets, profile):
        """Edge lists from node s to each target (None where there is no path)."""
        g = self.csr(profile)
        dist, pred = dijkstra(g, directed=True, indices=int(s), return_predecessors=True)
        best, out = self._best[profile], []
        for t in targets:
            if not np.isfinite(dist[t]):
                out.append(None)
                continue
            seq = [int(t)]
            while seq[-1] != s:
                seq.append(int(pred[seq[-1]]))
            seq.reverse()
            out.append([best[pair] for pair in pairwise(seq)])
        return out

    def line(self, edges):
        """Route coordinates (lon, lat) for an edge list."""
        if not edges:
            return np.zeros((0, 2))
        parts = [self.edge_coords(edges[0])] + [self.edge_coords(e)[1:] for e in edges[1:]]
        return np.vstack(parts)


if __name__ == "__main__":
    build(sys.argv[1], sys.argv[2])
