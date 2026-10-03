"""Measures on routes: overlap, where two routes part, and an audit of any
route against this graph's data.

A route from another engine is a polyline. Every engine here is built from
the same OSM extract as this graph, so its vertices coincide with this
graph's: an edge of this graph is "on" the route when the whole edge lies
within MATCH_M of the polyline. No map matching is needed.
"""
import numpy as np
from shapely import (
    STRtree,
    distance,
    get_point,
    length,
    line_locate_point,
    linestrings,
    union_all,
)
from shapely.geometry import LineString, Point

from compare.graph import DOWNHILL, UPHILL, metres

MATCH_M = 1.0       # an edge this close to the route along its whole length is on it
ON_VERTEX_M = 0.3   # a short edge must also have both ends this close
OVERLAP_M = 10.0


def line(coords):
    """A LineString in metres from (lon, lat) coordinates, or None for under two points."""
    return LineString(metres(coords)) if len(coords) >= 2 else None


def near(a, tol=OVERLAP_M):
    """The area within tol metres of a route. Make it once per route: it is the costly part."""
    return a.buffer(tol, quad_segs=4)


def overlap(a, b_near):
    """Share of a's length inside b_near, the area near route b."""
    return float(a.intersection(b_near).length / a.length) if a.length else 1.0


def parting(a, b_near):
    """Metres along a where it first leaves b_near, or None if it never does."""
    away = a.difference(b_near)
    if away.is_empty:
        return None
    parts = getattr(away, "geoms", [away])
    return float(min(a.project(Point(part.coords[0])) for part in parts if part.geom_type == "LineString"))


class Matcher:
    """Finds the edges of this graph that a polyline runs along."""

    def __init__(self, g):
        self.g = g
        # The opposite of an edge is the edge of the same OSM way between the same nodes, the other way round.
        self.twin = {}
        for e, key in enumerate(zip(g.u.tolist(), g.v.tolist(), g.osm_id.tolist(), strict=True)):
            self.twin.setdefault(key, e)
        # One geometry per pair of opposite edges.
        keep = np.array([a < b or (b, a, w) not in self.twin for a, b, w in zip(g.u.tolist(), g.v.tolist(), g.osm_id.tolist(), strict=True)])
        self.base = np.flatnonzero(keep)
        self.base_way = g.osm_id[self.base]
        counts = (g.offsets[1:] - g.offsets[:-1])[self.base]
        take = np.concatenate([np.arange(g.offsets[e], g.offsets[e + 1]) for e in self.base])
        self.geoms = linestrings(metres(g.coords[take]), indices=np.repeat(np.arange(len(self.base)), counts))
        self.tree = STRtree(self.geoms)

    def base_of(self, edges):
        """Index into the matcher's geometries for each edge (or its opposite)."""
        g, where = self.g, {int(e): i for i, e in enumerate(self.base)}
        return np.array([where.get(e, where.get(self.twin.get((int(g.v[e]), int(g.u[e]), int(g.osm_id[e])), -1), -1)) for e in edges])

    def match(self, route, ways=None, among=None):
        """(edges, unmatched share): directed edge indices in route order, and
        the share of the route's length that lies on no edge of this graph.

        Two ways can share a line: steps and the path beside them, drawn on
        the same nodes. Where the engine names the ways it used, pass their
        OSM ids as `ways`, or the matcher's own geometry indices as `among`,
        and only those are matched. Otherwise a flight of steps is dropped
        when another matched edge joins the same two nodes.
        """
        g = self.g
        hit = self.tree.query(route.buffer(MATCH_M, quad_segs=4), predicate="contains")
        if ways is not None:
            hit = hit[np.isin(self.base_way[hit], ways)]
        if among is not None:
            hit = hit[np.isin(hit, among)]
        if len(hit) == 0:
            return [], 1.0
        geoms = self.geoms[hit]
        start, end = get_point(geoms, 0), get_point(geoms, -1)
        # A short edge inside the buffer may be a stub beside the route: it must also have both ends on it.
        stub = (length(geoms) < 3 * MATCH_M) & (np.maximum(distance(route, start), distance(route, end)) > ON_VERTEX_M)
        if ways is None and among is None:
            e = self.base[hit]
            pair = np.minimum(g.u[e], g.v[e]).astype(np.int64) * g.n + np.maximum(g.u[e], g.v[e])
            steps = g.kind[e] == "steps"
            stub |= steps & np.isin(pair, pair[~steps])
        hit, start, end = hit[~stub], start[~stub], end[~stub]
        if len(hit) == 0:
            return [], 1.0
        a, b = line_locate_point(route, start), line_locate_point(route, end)
        edges = self.base[hit].tolist()
        for i in np.flatnonzero(b < a):     # the route runs v to u: take the opposite edge where the graph has one
            e = edges[i]
            edges[i] = self.twin.get((int(g.v[e]), int(g.u[e]), int(g.osm_id[e])), e)
        on = route.intersection(union_all(self.geoms[hit]).buffer(MATCH_M, quad_segs=4))
        order = np.argsort(np.minimum(a, b), kind="stable")
        return [edges[i] for i in order], max(0.0, float(1 - on.length / route.length)) if route.length else 0.0


def audit(g, edges):
    """What this graph's data says about a route given as its edges.

    Barriers are what the wheelchair profile refuses: steps, a crossing with
    no surveyed ramp within reach of both ends, an edge steeper than the
    limits in the direction of travel, and a street centreline.
    """
    e = np.asarray(edges, dtype=int)
    if len(e) == 0:
        return {"steps": 0, "crossing_no_ramp": 0, "over_incline": 0, "street_m": 0.0, "first_barrier": None}
    kind, inc = g.kind[e], g.incline[e]
    steps = kind == "steps"
    no_ramp = (kind == "crossing") & ~g.curbramps[e]
    steep = ~steps & (kind != "street") & ((inc > UPHILL) | (inc < DOWNHILL))
    street = kind == "street"
    why = np.select([steps, no_ramp, steep, street], ["steps", "crossing_no_ramp", "over_incline", "street"], "")
    barred = np.flatnonzero(why != "")
    first = {"edge": int(e[barred[0]]), "why": str(why[barred[0]])} if len(barred) else None
    # Consecutive edges of one crossing or one flight count once.
    runs = lambda mask: int(np.sum(mask & ~np.r_[False, mask[:-1]]))
    return {"steps": runs(steps), "crossing_no_ramp": runs(no_ramp), "over_incline": int(steep.sum()),
            "street_m": round(float(g.length[e][street].sum()), 1), "first_barrier": first}
