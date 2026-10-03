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
from compare.measures import Matcher, audit, line, overlap, parting


def ll(x, y):
    """(lon, lat) of a point x metres east and y metres north of the origin."""
    return [round(-73.91 + x / EAST, 7), round(40.66 + y / NORTH, 7)]


def feat(i, u, v, a, b, subclass="footway", footway="sidewalk", incline=0.0, curbramps=0):
    return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [a, b]},
            "properties": {"fid": i, "_u": u, "_v": v, "subclass": subclass, "footway": footway, "curbramps": curbramps,
                           "incline": incline, "length": ((a[0] - b[0]) ** 2 * EAST ** 2 + (a[1] - b[1]) ** 2 * NORTH ** 2) ** 0.5,
                           "surface": None, "width": None, "description": f"{subclass} {i}", "ext_borough": "BK", "ext_osm_id": "1"}}


def graph():
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
    assert 0.54 < overlap(a, b) < 0.56      # 100 m shared, then 10 m inside the tolerance, of 200 m
    assert 105 <= parting(a, b) <= 115
    assert parting(a, a) is None


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
