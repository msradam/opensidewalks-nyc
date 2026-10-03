"""The comparison's measures, on a graph small enough to check by hand.

Run with `python tests/test_compare_measures.py`. The graph is an L: a
sidewalk east, a crossing with no ramp, then a choice of steps north or a
steep path, plus a street beside the first sidewalk.
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare.graph import EAST, NORTH, Graph, build
from compare.measures import Matcher, audit, line, near, overlap, parting


def ll(x, y):
    """(lon, lat) of a point x metres east and y metres north of the origin."""
    return [round(-73.91 + x / EAST, 7), round(40.66 + y / NORTH, 7)]


def feat(i, u, v, a, b, subclass="footway", footway="sidewalk", incline=0.0, curbramps=0):
    return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [a, b]},
            "properties": {"fid": i, "_u": u, "_v": v, "subclass": subclass, "footway": footway, "curbramps": curbramps,
                           "incline": incline, "length": ((a[0] - b[0]) ** 2 * EAST ** 2 + (a[1] - b[1]) ** 2 * NORTH ** 2) ** 0.5,
                           "surface": None, "width": None, "description": f"{subclass} {i}", "ext_borough": "BK", "ext_osm_id": "1"}}


def graph(min_network=1):
    import compare.graph as cg
    cg.MIN_NETWORK_EDGES = min_network      # the fixture is smaller than a real network
    return _graph()


def _graph():
    A, B, C, D, E = ll(0, 0), ll(100, 0), ll(112, 0), ll(112, 50), ll(160, 0)
    both = lambda i, u, v, a, b, inc=0.0, **kw: [feat(i, u, v, a, b, incline=inc, **kw), feat(i + "r", v, u, b, a, incline=-inc, **kw)]
    feats = (both("s", "A", "B", A, B) + both("x", "B", "C", B, C, footway="crossing")
             + both("t", "C", "D", C, D, subclass="steps", footway=None)
             + both("p", "C", "E", C, E, inc=0.09, footway=None)
             + both("r", "A2", "B2", ll(0, 20), ll(100, 20), subclass="street", footway=None))
    with tempfile.TemporaryDirectory() as d:
        layer, npz = Path(d) / "layer.geojson", Path(d) / "g.npz"
        layer.write_text(json.dumps({"features": feats}))
        build(layer, npz)
        return Graph(npz)


def fid(g, edges):
    return [str(g.fid[e]) for e in edges]


def test_cost_function_masks():
    g = graph()
    passable = set(fid(g, [e for e in range(g.m) if g.masks["wheelchair"][e]]))
    assert passable == {"s", "sr", "pr"}, passable      # downhill 9% passes, uphill does not
    assert g.masks["walk"].sum() == g.m - 2


def test_route_and_snap():
    g = graph()
    s, d = g.snap(ll(1, 2), "walk")
    assert str(g.ids[s]) == "A" and 2.0 < d < 2.5
    t, _ = g.snap(ll(112, 49), "walk")
    assert fid(g, g.routes(s, [t], "walk")[0]) == ["s", "x", "t"]
    assert g.routes(s, [t], "wheelchair")[0] is None


def test_route_snaps_to_the_nearest_point_on_an_edge():
    g = graph()
    r = g.route(ll(50, 1), ll(113, 30), "walk")
    assert fid(g, r["edges"]) == ["s", "x", "t"], fid(g, r["edges"])
    assert abs(r["length_m"] - (50 + 12 + 30)) < 0.5, r
    assert abs(r["skip_first_m"] - 50) < 0.5 and abs(r["skip_last_m"] - 20) < 0.5
    assert 0.9 < r["snap_m"][0] < 1.1
    shape = line(g.line(r["edges"], r["skip_first_m"], r["skip_last_m"]))
    assert abs(shape.length - r["length_m"]) < 0.5, "the drawn route starts and ends at the snapped points"
    back = g.route(ll(80, 0), ll(20, 0), "walk")
    assert fid(g, back["edges"]) == ["sr"] and abs(back["length_m"] - 60) < 0.5, back
    down = g.route(ll(150, 0), ll(120, 0), "wheelchair")
    assert fid(g, down["edges"]) == ["pr"] and abs(down["length_m"] - 30) < 0.5, down
    assert g.route(ll(120, 0), ll(150, 0), "wheelchair")["edges"] is None, "9% uphill is over the limit"
    assert g.route(ll(150, 0), ll(10, 0), "wheelchair")["edges"] is None, "the crossing has no ramp"


def test_snap_prefers_a_real_network_to_a_nearby_fragment():
    import compare.graph as cg
    g = graph()
    try:
        cg.MIN_NETWORK_EDGES = 4        # the street (2 edges) is a fragment; the walk network (8) is not
        e, d, _ = g.snap_edge(ll(50, 12), "distance")
        assert str(g.fid[e]) in ("s", "sr") and 11.9 < d < 12.1, "8 m from the fragment, 12 m from the network: take the network"
        cg.SNAP_SLACK_M = 2.0
        g._tree.clear()
        e, d, _ = g.snap_edge(ll(50, 12), "distance")
        assert str(g.fid[e]) in ("r", "rr") and 7.9 < d < 8.1, "the network is too much further: the point is on the fragment"
    finally:
        cg.MIN_NETWORK_EDGES, cg.SNAP_SLACK_M = 200, 25.0


def test_match_restores_direction_and_order():
    g = graph()
    m = Matcher(g)
    edges, unmatched = m.match(line([ll(112, 50), ll(112, 0), ll(100, 0), ll(0, 0)]))
    assert fid(g, edges) == ["tr", "xr", "sr"], fid(g, edges)
    assert unmatched < 0.01


def test_match_reports_length_off_the_graph():
    g = graph()
    edges, unmatched = Matcher(g).match(line([ll(0, 0), ll(100, 0), ll(100, -100)]))
    assert fid(g, edges) == ["s"]
    assert 0.49 < unmatched < 0.51


def test_match_tells_coincident_ways_apart_by_their_ids():
    g = graph()
    m = Matcher(g)
    route = line([ll(112, 0), ll(112, 50)])
    assert fid(g, m.match(route)[0]) == ["t"]
    assert m.match(route, ways=[999])[0] == [], "the engine says it used another way on this line"
    assert fid(g, m.match(route, ways=[1])[0]) == ["t"]
    assert fid(g, m.match(route, among=m.base_of([e for e in range(g.m) if str(g.fid[e]) == "tr"]))[0]) == ["t"]


def test_audit_names_the_first_barrier_and_respects_direction():
    g = graph()
    m = Matcher(g)
    up, _ = m.match(line([ll(0, 0), ll(100, 0), ll(112, 0), ll(160, 0)]))
    a = audit(g, up)
    assert (a["crossing_no_ramp"], a["over_incline"], a["steps"]) == (1, 1, 0), a
    assert a["first_barrier"]["why"] == "crossing_no_ramp"
    down, _ = m.match(line([ll(160, 0), ll(112, 0)]))
    assert audit(g, down)["over_incline"] == 0, "9% downhill is inside the 10% limit"
    street, _ = m.match(line([ll(0, 20), ll(100, 20)]))
    assert audit(g, street)["street_m"] > 99


def test_overlap_and_parting():
    a = line([ll(0, 0), ll(100, 0), ll(100, 100)])
    b = line([ll(0, 0), ll(100, 0), ll(200, 0)])
    assert 0.54 < overlap(a, near(b)) < 0.56      # 100 m shared, then 10 m inside the tolerance, of 200 m
    assert 109 <= parting(a, near(b)) <= 111
    assert parting(a, near(a)) is None
    assert parting(line([ll(0, 50), ll(0, 0), ll(100, 0)]), near(b)) == 0.0, "a route that starts away from the other parts at once"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
