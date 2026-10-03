"""The OSW to OSM converter keeps what a wheelchair router reads.

Run with `python tests/test_osw_to_osm.py`. The fixture is one block corner:
a sidewalk, a crossing with a surveyed ramp 3 m from one end and none at the
other, and a steep path.
"""

import json
import sys
import tempfile
from pathlib import Path

import osmium

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from osw_to_osm import RAISED_HEIGHT, convert

M = 1 / 111320      # one metre of latitude


def node(i, x, y, **props):
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [x, y]}, "properties": {"_id": i, **props}}


def edge(i, u, v, coords, **props):
    return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {"_id": i, "_u_id": u, "_v_id": v, **props}}


def fixture():
    a, b, c, d = [-73.91, 40.66], [-73.91, 40.66 + 12 * M], [-73.91, 40.66 + 30 * M], [-73.91, 40.66 - 20 * M]
    mid = [-73.91, 40.66 + 6 * M]
    ramp = [-73.91 + 3 * M, 40.66]
    side = {"highway": "footway", "footway": "sidewalk", "width": 3.3, "surface": "concrete"}
    cross = {"highway": "footway", "footway": "crossing", "crossing:markings": "yes"}
    return [
        node("a", *a), node("b", *b), node("c", *c), node("d", *d), node("m", *mid),
        node("r", *ramp, barrier="kerb", kerb="lowered"),
        edge("s1", "d", "a", [d, a], incline=0.0123, **side), edge("s1r", "a", "d", [a, d], incline=-0.0123, **side),
        edge("x1", "a", "m", [a, mid], incline=0.02, **cross), edge("x1r", "m", "a", [mid, a], incline=-0.02, **cross),
        edge("x2", "m", "b", [mid, b], incline=0.0, **cross), edge("x2r", "b", "m", [b, mid], incline=0.0, **cross),
        edge("p1", "b", "c", [b, [-73.9101, 40.66 + 20 * M], c], incline=0.095, highway="footway"),
        edge("p1r", "c", "b", [c, [-73.9101, 40.66 + 20 * M], b], incline=-0.095, highway="footway"),
    ]


class Read(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.nodes, self.ways = {}, {}

    def node(self, n):
        self.nodes[n.id] = ((n.location.lon, n.location.lat), dict(n.tags))

    def way(self, w):
        self.ways[w.id] = ([n.ref for n in w.nodes], dict(w.tags))


def read(feats, **kw):
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "t.osm.pbf"
        convert(feats, out, **kw)
        r = Read()
        r.apply_file(str(out))
        return r, json.loads(Path(str(out) + ".ways.json").read_text())


def at(r, xy):
    return next(t for (lon, lat), t in r.nodes.values() if abs(lon - xy[0]) < 1e-7 and abs(lat - xy[1]) < 1e-7)


def test_one_way_per_pair_of_opposite_edges():
    r, ways = read(fixture())
    assert ways == ["s1", "x1", "x2", "p1"], ways
    assert len(r.ways) == 4
    assert len(r.nodes) == 6 + 1, "the six OSW nodes and the one vertex inside p1"


def test_incline_is_a_signed_percentage_in_the_way_direction():
    r, ways = read(fixture())
    by = {ways[i - 1]: tags for i, (_, tags) in r.ways.items()}
    assert by["s1"]["incline"] == "1.2%", by["s1"]
    assert by["p1"]["incline"] == "9.5%", by["p1"]
    assert by["x2"]["incline"] == "0.0%"


def test_width_surface_and_class_survive():
    r, ways = read(fixture())
    by = {ways[i - 1]: tags for i, (_, tags) in r.ways.items()}
    assert by["s1"]["width"] == "3.3" and by["s1"]["surface"] == "concrete"
    assert by["s1"]["footway"] == "sidewalk" and by["x1"]["footway"] == "crossing"
    assert by["x1"]["crossing:markings"] == "yes"
    assert "width" not in by["p1"]


def test_crossing_ends_carry_the_ramp_rule():
    feats = fixture()
    r, _ = read(feats)
    a, b, m = feats[0]["geometry"]["coordinates"], feats[1]["geometry"]["coordinates"], feats[4]["geometry"]["coordinates"]
    assert at(r, a) == {"barrier": "kerb", "kerb": "lowered"}, "a ramp 3 m from this end serves it"
    assert at(r, b) == {"barrier": "kerb", "kerb": "raised", "kerb:height": RAISED_HEIGHT}, "no ramp within 5 m"
    assert at(r, m) == {}, "a vertex inside the crossing is not an end"
    ramp = feats[5]["geometry"]["coordinates"]
    assert at(r, ramp) == {}, "the ramp node itself is on no crossing, so ORS would not read it"


def test_unknown_mode_leaves_unramped_ends_untagged():
    feats = fixture()
    r, _ = read(feats, no_ramp="unknown")
    assert at(r, feats[1]["geometry"]["coordinates"]) == {}
    assert at(r, feats[0]["geometry"]["coordinates"])["kerb"] == "lowered"


def test_pedestrian_only_leaves_out_street_centrelines():
    feats = fixture()
    a, d = feats[0]["geometry"]["coordinates"], feats[3]["geometry"]["coordinates"]
    street = [edge("st", "d", "a", [d, a], highway="residential", name="Test Street"), edge("str", "a", "d", [a, d], highway="residential", name="Test Street")]
    _, ways = read(feats + street)
    assert ways == ["s1", "x1", "x2", "p1", "st"], "a street on the same line as a sidewalk is a way of its own"
    _, ways = read(feats + street, pedestrian_only=True)
    assert ways == ["s1", "x1", "x2", "p1"], ways


def test_steps_and_a_path_on_one_line_are_both_kept():
    feats = fixture()
    b, c = feats[1]["geometry"]["coordinates"], feats[2]["geometry"]["coordinates"]
    via = [-73.9101, 40.66 + 20 * M]
    steps = [edge("t1", "b", "c", [b, via, c], highway="steps"), edge("t1r", "c", "b", [c, via, b], highway="steps")]
    r, ways = read(feats + steps)
    assert ways == ["s1", "x1", "x2", "p1", "t1"], ways
    assert [tags["highway"] for _, tags in r.ways.values()][-2:] == ["footway", "steps"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
