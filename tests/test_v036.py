"""Checks for the v0.3.6 fixes: terrain "no data" is not a height of 0 m, a
node inside a tunnel has no height, two heights that cannot be a slope are
not written as one, the wheelchair profile does not walk such an edge as
level, and every GraphML edge has its length.

Run: python tests/test_v036.py
"""

import importlib.util
import json
import math
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, Point

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import osw_to_unweaver
import to_graphml
from pipeline.stages.assemble import _compute_edge_inclines, _sample_terrain
from pipeline.utils.deck import grade
from pipeline.utils.zones import zone_edges

PX = 2e-5        # one pixel of the test tile, in degrees: about 1.7 m east, 2.2 m north


def _at(col, row):
    """The centre of a pixel of the test tile (fractions allowed)."""
    return (-74.0 + (col + 0.5) * PX, 40.701 - (row + 0.5) * PX)


def _built(tmp):
    """A tile and a small graph on it, through Stage 4's incline step.

    The tile is ground at 10 m, a terrace at 16 m from column 20, and open
    water (the service's "no data", written as 0.0) from column 40.
    """
    band = np.full((60, 60), 10.0, dtype="float32")
    band[:, 20:] = 16.0
    band[:, 40:] = 0.0
    tile = Path(tmp) / "dem.tif"
    with rasterio.open(tile, "w", driver="GTiff", height=60, width=60, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(-74.0, 40.701, PX, PX)) as dst:
        dst.write(band, 1)
    xy = {"a": _at(5, 30), "b": _at(10, 30),              # level ground
          "c": _at(17, 30), "d": _at(22, 30),             # the ground and the terrace, 8 m apart
          "c2": _at(17, 40), "d2": _at(22, 40),           # the same, joined by steps
          "p": _at(30, 30), "w": _at(50, 30),             # the terrace and a pier over the water
          "q": _at(39.25, 30),                            # the shoreline: a quarter of the way into the water
          "t0": _at(5, 10), "t1": _at(10, 10), "t2": _at(15, 10), "o": _at(2, 10)}   # a tunnel and its mouths
    rows = [("a", "b", "footway", None), ("c", "d", "footway", None), ("c2", "d2", "steps", None),
            ("p", "w", "footway", None), ("p", "q", "footway", None),
            ("o", "t0", "footway", None), ("t0", "t1", "footway", "tunnel"), ("t1", "t2", "footway", "tunnel"),
            ("t2", "a", "footway", None)]
    edges = gpd.GeoDataFrame(
        {"_u_id": [r[0] for r in rows], "_v_id": [r[1] for r in rows], "highway": [r[2] for r in rows],
         "ext:structure": [r[3] for r in rows]},
        geometry=[LineString([xy[r[0]], xy[r[1]]]) for r in rows], crs="EPSG:4326")
    nodes = gpd.GeoDataFrame({"_id": list(xy)}, geometry=[Point(p) for p in xy.values()], crs="EPSG:4326")
    edges = _compute_edge_inclines(edges, nodes, [tile])
    def known(values):
        return [None if pd.isna(v) else v for v in values]
    key = list(zip(edges["_u_id"], edges["_v_id"]))
    marks = edges["ext:incline_unknown"] if "ext:incline_unknown" in edges else [None] * len(edges)
    return (dict(zip(nodes["_id"], known(nodes["ext:elevation_m"]))), dict(zip(key, known(edges["incline"]))),
            dict(zip(key, known(marks))), edges)


def test_terrain_no_data_is_not_a_height_of_zero():
    with tempfile.TemporaryDirectory() as tmp:
        z, incline, _, _ = _built(tmp)
    assert z["p"] == 16.0
    # A node over the water has no height, not 0 m, and its edge no incline
    # (read as 0 m, the edge onto the pier fell 16 m in 34 m).
    assert z["w"] is None and incline[("p", "w")] is None
    # On the shoreline the height comes from the pixels that hold data: the
    # terrace's 16 m, not 16 m blended a quarter of the way toward 0.
    assert z["q"] == 16.0 and incline[("p", "q")] == 0.0
    # With less than half the weight on data there is no height to give.
    band = np.array([[5.0, 0.0], [5.0, 0.0]])
    out = _sample_terrain(band, np.array([1.0, 1.0, 1.0]), np.array([0.5, 0.9, 1.25]))
    assert out[0] == 5.0 and out[1] == 5.0 and np.isnan(out[2])


def test_a_node_inside_a_tunnel_does_not_take_the_ground_above():
    with tempfile.TemporaryDirectory() as tmp:
        z, incline, _, _ = _built(tmp)
    assert z["t1"] is None, "every edge at t1 is a tunnel edge"
    # The mouths, where a tunnel edge meets an open one, are in the open.
    assert z["t0"] == 10.0 and z["t2"] == 10.0
    assert incline[("o", "t0")] == 0.0


def test_a_jump_between_two_surfaces_is_not_written_as_a_slope():
    with tempfile.TemporaryDirectory() as tmp:
        z, incline, unknown, _ = _built(tmp)
    assert z["c"] == 10.0 and z["d"] == 16.0
    # 6 m in 8 m on a plain footway: the two ends are on two surfaces. No
    # incline, and the edge says its grade is unknown.
    assert incline[("c", "d")] is None and unknown[("c", "d")] == "yes"
    # Steps do climb like that, and keep their incline.
    assert 0.6 < incline[("c2", "d2")] < 0.8 and unknown[("c2", "d2")] is None
    # Level ground, a tunnel and a pier with no height are not marked: there
    # nothing was measured, or nothing is wrong.
    assert [k for k, v in unknown.items() if v] == [("c", "d")]
    # The rule itself: a staircase's pitch on a plain edge, 1.0 on steps.
    assert grade(0.49, 1.0) == (0.49, False) and grade(-0.5, 1.0) == (None, True)
    assert grade(1.0, 1.0, steps=True) == (1.0, False) and grade(1.01, 1.0, steps=True) == (None, True)
    assert grade(1.0, 0.0) == (None, False)
    # A plaza edge across a change of level is marked the same way.
    tri = ({"properties": {"_id": "s", "_w_id": ["a", "b", "c"]}},
           {"a": (-74, 40.7), "b": (-74 + 3e-5, 40.7), "c": (-74, 40.7 + 3e-5)})
    ab = next(e for e in zone_edges(*tri, referenced={"a", "b"}, node_z={"a": 1.0, "b": 12.0})
              if (e["_u_id"], e["_v_id"]) == ("a", "b"))
    assert ab["incline"] is None and ab["ext:incline_unknown"] == "yes"
    flat = zone_edges(*tri, referenced={"a", "b"}, node_z={"a": 1.0, "b": 1.2})
    assert all(e["ext:incline_unknown"] is None for e in flat)


def _feature(geom, **props):
    return {"type": "Feature", "geometry": geom, "properties": props}


def _small_file(tmp):
    """Two nodes 100 m apart joined both ways, one way marked unknown."""
    a, b = [-74.0, 40.7], [-74.0, 40.7 + 100 / 111195]
    feats = [_feature({"type": "Point", "coordinates": a}, _id="a"),
             _feature({"type": "Point", "coordinates": b}, _id="b"),
             _feature({"type": "LineString", "coordinates": [a, b]}, _id="ab", _u_id="a", _v_id="b",
                      highway="footway", **{"ext:incline_unknown": "yes"}),
             _feature({"type": "LineString", "coordinates": [b, a]}, _id="ba", _u_id="b", _v_id="a",
                      highway="footway")]
    path = Path(tmp) / "in.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    return path


def test_the_wheelchair_profile_does_not_walk_an_unknown_grade_as_level():
    spec = importlib.util.spec_from_file_location("cost_wheelchair", ROOT / "unweaver-project/cost-wheelchair.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cost = module.cost_fun_generator(None)
    edge = {"subclass": "footway", "footway": None, "curbramps": 0, "length": 5.0}
    assert cost("u", "v", {**edge, "incline": None, "incline_unknown": 1}) is None
    # No incline because nothing measured the edge (a tunnel): walked, as before.
    assert cost("u", "v", {**edge, "incline": None, "incline_unknown": 0}) == 5.0
    assert cost("u", "v", {**edge, "incline": 0.02, "incline_unknown": 0}) == 5.0
    # With the limits taken off, the profile asks nothing of incline at all.
    free = module.cost_fun_generator(None, uphill=math.inf, downhill=-math.inf)
    assert free("u", "v", {**edge, "incline": None, "incline_unknown": 1}) == 5.0
    # The layer the profile reads carries the mark from the file.
    with tempfile.TemporaryDirectory() as tmp:
        argv, sys.argv = sys.argv, ["x", "--input", str(_small_file(tmp)), "--output-layer", f"{tmp}/layer.geojson",
                                    "--output-region", f"{tmp}/region.geojson"]
        try:
            osw_to_unweaver.main()
        finally:
            sys.argv = argv
        layer = {f["properties"]["fid"]: f["properties"] for f in json.loads(Path(tmp, "layer.geojson").read_text())["features"]}
    assert layer["ab"]["incline_unknown"] == 1 and layer["ba"]["incline_unknown"] == 0


def test_every_graphml_edge_has_its_length():
    with tempfile.TemporaryDirectory() as tmp:
        src = _small_file(tmp)
        for flag, n_edges in ((False, 2), (True, 1)):
            to_graphml.main(src, Path(tmp) / "g.graphml", undirected=flag)
            G = nx.read_graphml(Path(tmp) / "g.graphml")
            lengths = [d.get("length_m") for _, _, d in G.edges(data=True)]
            assert len(lengths) == n_edges and all(99.5 < x < 100.5 for x in lengths), lengths


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
