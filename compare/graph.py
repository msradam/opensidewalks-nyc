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
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree
from shapely import STRtree, linestrings
from shapely.geometry import LineString, Point
from shapely.ops import substring

ROOT = Path(__file__).resolve().parents[1]
# One scale for the whole city, as in scripts/osw_to_unweaver.py.
EAST = 111320 * math.cos(math.radians(40.7))
NORTH = 111320.0
UPHILL, DOWNHILL = 0.083, -0.1
MIN_NETWORK_EDGES = 200     # GraphHopper, under ORS, drops networks smaller than this
SNAP_SLACK_M = 25.0


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

    def _search(self, profile, sources, targets):
        """Dijkstra from each source node, exact for every target.

        Searching the whole city for each pair is slow, so the first pass
        stops at a distance a real route rarely exceeds; only where that
        finds nothing is the search repeated without a limit.
        """
        g = self.csr(profile)
        crow = float(np.hypot(*(metres(self.xy[sources])[:, None] - metres(self.xy[targets])[None]).T).max())
        dist, pred = dijkstra(g, directed=True, indices=sources, return_predecessors=True, limit=3 * crow + 2000)
        if not np.isfinite(dist[:, targets]).all():
            dist, pred = dijkstra(g, directed=True, indices=sources, return_predecessors=True)
        return dist, pred

    def routes(self, s, targets, profile):
        """Edge lists from node s to each target (None where there is no path)."""
        dist, pred = self._search(profile, [int(s)], [int(t) for t in targets])
        dist, pred = dist[0], pred[0]
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

    def snap_edge(self, lonlat, profile):
        """Nearest point on a passable edge: (edge, metres to it, metres along it).

        This is how the reference engines treat a coordinate. Snapping to a
        node instead puts the start up to half a block from theirs.

        The nearest passable edge is sometimes a fragment cut off from
        everything else (a stub of path between two flights of steps). ORS
        drops such fragments when it builds its graph. Here the nearest edge
        of a real network (MIN_NETWORK_EDGES or more) is taken instead when it
        is no more than SNAP_SLACK_M further away; a point whose only nearby
        edges are an island stays on the island and gets no route.
        """
        if ("edge", profile) not in self._tree:
            k = self.masks[profile]
            idx = np.flatnonzero(k)
            _, label = connected_components(coo_matrix((np.ones(len(idx)), (self.u[idx], self.v[idx])), shape=(self.n, self.n)), directed=False)
            size = np.bincount(label[self.u[idx]])      # edges per component
            big = size[label[self.u[idx]]] >= MIN_NETWORK_EDGES
            counts = (self.offsets[1:] - self.offsets[:-1])[idx]
            take = np.concatenate([np.arange(self.offsets[e], self.offsets[e + 1]) for e in idx])
            geoms = linestrings(metres(self.coords[take]), indices=np.repeat(np.arange(len(idx)), counts))
            where = np.flatnonzero(big)
            self._tree[("edge", profile)] = (STRtree(geoms), geoms, idx, big, STRtree(geoms[where]), where)
        tree, geoms, idx, big, big_tree, where = self._tree[("edge", profile)]
        p = Point(metres(lonlat))
        i = tree.nearest(p)
        if not big[i] and len(where):
            j = where[big_tree.nearest(p)]
            if geoms[j].distance(p) <= geoms[i].distance(p) + SNAP_SLACK_M:
                i = j
        return int(idx[i]), float(geoms[i].distance(p)), float(geoms[i].project(p))

    def route(self, o, d, profile):
        """Shortest passable route between two coordinates, each snapped to its nearest passable edge.

        Returns the two snap distances and, where there is a path, the edges
        in order ("edges" is None otherwise), the metres of the first and
        last edge that lie outside the route, and its length.
        """
        self.csr(profile)
        best, length = self._best[profile], self.length
        (eo, do, fo), (ed, dd, fd) = self.snap_edge(o, profile), self.snap_edge(d, profile)
        uo, vo, ud, vd = int(self.u[eo]), int(self.v[eo]), int(self.u[ed]), int(self.v[ed])
        # (node, metres from the snapped point to it, first edge, metres of that edge skipped)
        starts = [(vo, length[eo] - fo, eo, fo)]
        if (vo, uo) in best:        # the opposite direction is passable too
            starts.append((uo, fo, best[(vo, uo)], length[eo] - fo))
        ends = [(ud, fd, ed, length[ed] - fd)]
        if (vd, ud) in best:
            ends.append((vd, length[ed] - fd, best[(vd, ud)], fd))
        # Both ends on one edge: travel along it, or along its opposite, without leaving.
        found, twin = None, (uo, vo) == (vd, ud)
        along = fd if eo == ed else length[eo] - fd if twin else None     # the destination, measured along eo
        if along is not None and along >= fo:
            found = (along - fo, [eo], fo, length[eo] - along)
        elif along is not None and (vo, uo) in best:
            found = (fo - along, [best[(vo, uo)]], length[eo] - fo, along)
        dist, pred = self._search(profile, [s[0] for s in starts], [t[0] for t in ends])
        for i, (s, cs, first, skip_first) in enumerate(starts):
            for t, ct, last, skip_last in ends:
                total = cs + dist[i][t] + ct
                if np.isfinite(total) and (found is None or total < found[0]):
                    seq = [t]
                    while seq[-1] != s:
                        seq.append(int(pred[i][seq[-1]]))
                    seq.reverse()
                    found = (total, [first] + [best[pair] for pair in pairwise(seq)] + [last], skip_first, skip_last)
        if found is None:
            return {"edges": None, "snap_m": [round(do, 1), round(dd, 1)]}
        return {"edges": [int(e) for e in found[1]], "skip_first_m": round(float(found[2]), 2), "skip_last_m": round(float(found[3]), 2),
                "length_m": round(float(found[0]), 1), "snap_m": [round(do, 1), round(dd, 1)]}

    def line(self, edges, skip_first_m=0.0, skip_last_m=0.0):
        """Route coordinates (lon, lat) for an edge list, less the given metres at each end."""
        if not edges:
            return np.zeros((0, 2))
        parts = [self.edge_coords(edges[0])] + [self.edge_coords(e)[1:] for e in edges[1:]]
        coords = np.vstack(parts)
        if skip_first_m > 0 or skip_last_m > 0:
            whole = LineString(metres(coords))
            cut = substring(whole, min(skip_first_m, whole.length), max(whole.length - skip_last_m, skip_first_m))
            coords = np.array(cut.coords).reshape(-1, 2) / np.array([EAST, NORTH])
        return coords


if __name__ == "__main__":
    build(sys.argv[1], sys.argv[2])
