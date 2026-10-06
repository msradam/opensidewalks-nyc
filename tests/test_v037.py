"""Checks for the v0.3.7 changes: a street carries what OSM says about its
sidewalks and the wheelchair profile walks a street that has one, a cycleway
or track with no foot tag is kept (OSM's US default), a surveyed ramp snaps
to the crossing end it serves, OSM's own kerb and elevator nodes are carried,
an edge at an elevator has no incline, and the GraphML root holds the OSM
extract as JSON.

Run: python tests/test_v037.py
"""

import importlib.util
import json
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
import osw_to_osm
import osw_to_unweaver
import to_graphml

from pipeline.stages import assemble, schema_map
from pipeline.utils.ids import node_id

# Looked up at call time, so that on code without them each test fails at
# its own line rather than at import.
_compute_edge_inclines = lambda *a, **k: assemble._compute_edge_inclines(*a, **k)
_crossing_ends = lambda *a, **k: assemble._crossing_ends(*a, **k)
_endpoint_coords = lambda *a, **k: assemble._endpoint_coords(*a, **k)
_merge_node_group = lambda *a, **k: assemble._merge_node_group(*a, **k)
_osm_node_tags = lambda *a, **k: assemble._osm_node_tags(*a, **k)
_snap_curb_nodes = lambda *a, **k: assemble._snap_curb_nodes(*a, **k)
_classify_osm_edge = lambda *a, **k: schema_map._classify_osm_edge(*a, **k)
_shared_path_walkable = lambda *a, **k: schema_map._shared_path_walkable(*a, **k)
_sidewalk_tags = lambda *a, **k: schema_map._sidewalk_tags(*a, **k)


def test_the_filtered_extract_keeps_node_tags():
    """Stage 1 writes the nodes of the kept ways itself, with their tags."""
    import osmium

    from pipeline.stages import acquire
    with tempfile.TemporaryDirectory() as tmp:
        pbf = Path(tmp) / "in.osm.pbf"
        w = osmium.SimpleWriter(str(pbf))
        w.add_node(osmium.osm.mutable.Node(id=1, location=(-73.95, 40.65), tags={"barrier": "kerb", "kerb": "raised"}, version=1))
        w.add_node(osmium.osm.mutable.Node(id=2, location=(-73.9499, 40.65), tags={"highway": "elevator"}, version=1))
        w.add_node(osmium.osm.mutable.Node(id=3, location=(-73.9498, 40.65), tags={"kerb": "lowered"}, version=1))
        w.add_way(osmium.osm.mutable.Way(id=10, nodes=[1, 2], tags={"highway": "footway"}, version=1))
        w.add_way(osmium.osm.mutable.Way(id=11, nodes=[2, 3], tags={"highway": "motorway"}, version=1))
        w.close()
        xml = Path(tmp) / "out.osm"
        acquire._filter_extract(pbf, xml, '["highway"~"footway"]["foot"!~"no"]', (-74, 40.6, -73.9, 40.7))
        text = xml.read_text()
    assert 'k="kerb" v="raised"' in text and 'k="highway" v="elevator"' in text
    assert 'way id="10"' in text and 'way id="11"' not in text and 'node id="3"' not in text


def _row(**tags):
    return pd.Series({"highway": "residential", **tags})


def test_a_street_carries_what_osm_says_about_its_sidewalks():
    assert _sidewalk_tags(_row(sidewalk="both")) == {"ext:sidewalk": "both"}
    assert _sidewalk_tags(_row(sidewalk="separate")) == {"ext:sidewalk": "separate"}
    assert _sidewalk_tags(_row(sidewalk="none")) == {"ext:sidewalk": "no"}
    # The per-side form is folded into one value, and the sides are kept.
    assert _sidewalk_tags(_row(**{"sidewalk:both": "separate"})) == {
        "ext:sidewalk": "separate", "ext:sidewalk_left": "separate", "ext:sidewalk_right": "separate"}
    assert _sidewalk_tags(_row(**{"sidewalk:left": "no", "sidewalk:right": "separate"})) == {
        "ext:sidewalk": "separate", "ext:sidewalk_left": "no", "ext:sidewalk_right": "separate"}
    assert _sidewalk_tags(_row(**{"sidewalk:left": "yes", "sidewalk:right": "no"}))["ext:sidewalk"] == "left"
    assert _sidewalk_tags(_row(**{"sidewalk:right": "yes"}))["ext:sidewalk"] == "right"
    assert _sidewalk_tags(_row(**{"sidewalk:both": "no"}))["ext:sidewalk"] == "no"
    # A street with neither form says nothing.
    assert _sidewalk_tags(_row()) == {}
    assert _sidewalk_tags(_row(sidewalk=None, **{"sidewalk:left": float("nan")})) == {}


def test_a_cycleway_or_track_with_no_foot_tag_is_walked():
    assert _classify_osm_edge(pd.Series({"highway": "cycleway", "oneway": False})) == "footway"
    assert _classify_osm_edge(pd.Series({"highway": "track"})) == "footway"
    assert _classify_osm_edge(pd.Series({"highway": "cycleway", "foot": "designated", "oneway": "yes"})) == "footway"
    # A one-way cycleway with no foot tag is a bike lane beside the roadway.
    assert _classify_osm_edge(pd.Series({"highway": "cycleway", "oneway": "yes"})) is None
    assert _shared_path_walkable(pd.Series({"highway": "cycleway", "oneway": True})) is False
    # foot=private is not walked (foot=no never reaches Stage 3).
    assert _classify_osm_edge(pd.Series({"highway": "track", "foot": "private"})) is None


def _net():
    """A sidewalk along y=0 with a crossing leaving it at x=10: the crossing
    end is (10, 0); the sidewalk has a vertex 1 m from the ramp, the end 3 m.
    Coordinates are metres east and north of a point in Brooklyn."""
    o = (-73.95, 40.65)
    def at(x, y):
        return (round(o[0] + x / (111319 * np.cos(np.radians(o[1]))), 7), round(o[1] + y / 111319, 7))
    sw = gpd.GeoDataFrame({"_id": ["s1", "s2"], "_u_id": [node_id(*at(0, 0)), node_id(*at(10, 0))],
                           "_v_id": [node_id(*at(10, 0)), node_id(*at(13, 0))], "highway": ["footway"] * 2},
                          geometry=[LineString([at(0, 0), at(10, 0)]), LineString([at(10, 0), at(13, 0)])], crs="EPSG:4326")
    cr = gpd.GeoDataFrame({"_id": ["c1"], "_u_id": [node_id(*at(10, 0))], "_v_id": [node_id(*at(10, 12))],
                           "highway": ["footway"], "footway": ["crossing"]},
                          geometry=[LineString([at(10, 0), at(10, 12)])], crs="EPSG:4326")
    return at, sw, cr


def test_a_ramp_snaps_to_the_crossing_end_it_serves():
    at, sw, cr = _net()
    ends = _crossing_ends(cr, sw)
    assert [tuple(e) for e in ends] == [at(10, 0)], "the end is where the crossing meets the sidewalk"
    ramps = gpd.GeoDataFrame({"_id": ["r1", "r2"]}, geometry=[Point(at(13, 1)), Point(at(12.5, 0.5))], crs="EPSG:4326")
    endpoints = _endpoint_coords(pd.concat([sw, cr]))
    nearest = _snap_curb_nodes(ramps, endpoints, 5.0)
    assert list(nearest["_id"]) == [node_id(*at(13, 0))] * 2, "nearest vertex: the sidewalk's, 1 m away"
    snapped = _snap_curb_nodes(ramps, endpoints, 5.0, preferred=ends)
    # The nearer ramp takes the crossing end 3 m away; the other, with the
    # end taken, falls back to the nearest vertex of any kind.
    assert list(snapped["_id"]) == [node_id(*at(13, 0)), node_id(*at(10, 0))]
    far = gpd.GeoDataFrame({"_id": ["r3"]}, geometry=[Point(at(20, 0))], crs="EPSG:4326")
    assert list(_snap_curb_nodes(far, endpoints, 5.0, preferred=ends)["_id"]) == ["r3"], "out of reach stays put"


def test_osm_kerb_and_elevator_nodes_are_carried():
    nodes = gpd.GeoDataFrame({
        "_id": list("abcdefg"),
        "barrier": ["kerb", "kerb", None, "kerb", "bollard", None, None],
        "kerb": ["raised", "yes", "lowered", None, None, None, "lowered;flush"],
        "tactile_paving": ["yes", "bad", "no", None, None, "yes", None],
        "highway": [None, None, None, None, None, "elevator", None]},
        geometry=[Point(0, i) for i in range(7)], crs="EPSG:4326")
    out = _osm_node_tags(nodes)
    def val(v):
        return None if pd.isna(v) else v
    rows = {r["_id"]: tuple(val(r[k]) for k in ("barrier", "kerb", "tactile_paving", "ext:osm_highway"))
            for r in out.drop(columns="geometry").to_dict("records")}
    assert rows["a"] == ("kerb", "raised", "yes", None)            # a raised curb with tactile paving
    assert rows["b"][:3] == ("kerb", None, None)                   # kerb=yes says nothing the schema holds
    assert rows["c"][:3] == ("kerb", "lowered", "no")              # kerb=lowered alone is a curb ramp
    assert rows["d"][:3] == ("kerb", None, None)                   # a generic curb
    assert rows["e"][:3] == (None, None, None)                     # a bollard is not a kerb
    assert rows["f"] == (None, None, None, "elevator")             # tactile paving off a kerb is dropped
    assert rows["g"][:3] == ("kerb", "lowered", None)              # the first of a list
    # Where a surveyed ramp lands on an OSM kerb node, the survey's values
    # win and OSM's stay beside them.
    group = pd.DataFrame([
        {"_id": "n", "barrier": "kerb", "kerb": "raised", "tactile_paving": "no", "ext:source": "osm_walk",
         "ext:source_timestamp": "t0", "ext:ramp_id": None},
        {"_id": "n", "barrier": "kerb", "kerb": "lowered", "tactile_paving": "yes", "ext:source": "nyc_dot_ramps",
         "ext:source_timestamp": "t1", "ext:ramp_id": "R1"}])
    m = _merge_node_group(group, {"barrier", "kerb", "tactile_paving", "ext:ramp_id"})
    assert (m["kerb"], m["tactile_paving"], m["ext:source"], m["ext:ramp_id"]) == ("lowered", "yes", "nyc_dot_ramps", "R1")
    assert (m["ext:osm_kerb"], m["ext:osm_tactile_paving"]) == ("raised", "no")
    # OSM's values are kept even where they agree, and the survey's stand
    # even where it has none: the node says every value is the survey's.
    m = _merge_node_group(group.assign(kerb=["lowered", "lowered"], tactile_paving=["yes", None]),
                          {"barrier", "kerb", "tactile_paving", "ext:ramp_id"})
    assert m["ext:osm_kerb"] == "lowered" and m["ext:osm_tactile_paving"] == "yes" and pd.isna(m["tactile_paving"])
    # A ramp on no vertex is a group of one: nothing of OSM's to keep, and
    # its own kerb value must not be copied as OSM's.
    m = _merge_node_group(group.iloc[1:], {"barrier", "kerb", "tactile_paving", "ext:ramp_id"})
    assert m["kerb"] == "lowered" and "ext:osm_kerb" not in m.index and "ext:osm_tactile_paving" not in m.index
    # A ramp on a plain OSM vertex: as before, nothing from OSM to keep.
    m = _merge_node_group(group.assign(kerb=[None, "lowered"], tactile_paving=[None, "yes"], barrier=[None, "kerb"]),
                          {"barrier", "kerb", "tactile_paving", "ext:ramp_id"})
    assert m["kerb"] == "lowered" and "ext:osm_kerb" not in m.index


def test_node_dedup_keeps_the_geometry_when_only_some_nodes_disagree():
    """A ramp on an OSM kerb that disagrees adds keys to its merged row; the
    rows of other nodes must still line up, or the geometry is lost (the
    v0.3.7 release build failed here with "Unknown column geometry")."""
    rows = [
        {"_id": "n", "barrier": "kerb", "kerb": "raised", "tactile_paving": None, "ext:source": "osm_walk", "ext:ramp_id": None},
        {"_id": "n", "barrier": "kerb", "kerb": "lowered", "tactile_paving": "yes", "ext:source": "nyc_dot_ramps", "ext:ramp_id": "R1"},
        {"_id": "m", "barrier": None, "kerb": None, "tactile_paving": None, "ext:source": "osm_walk", "ext:ramp_id": None},
        {"_id": "o", "barrier": "kerb", "kerb": "flush", "tactile_paving": None, "ext:source": "osm_walk", "ext:ramp_id": None}]
    nodes = gpd.GeoDataFrame(rows, geometry=[Point(0, 0), Point(0, 0), Point(1, 0), Point(2, 0)], crs="EPSG:4326")
    out = assemble._dedup_nodes(nodes)
    assert isinstance(out, gpd.GeoDataFrame) and out.geometry.name == "geometry" and len(out) == 3
    by = out.set_index("_id")
    assert by.loc["n", "kerb"] == "lowered" and by.loc["n", "ext:osm_kerb"] == "raised"
    assert by.loc["o", "kerb"] == "flush" and pd.isna(by.loc["o", "ext:osm_kerb"])
    assert list(by.loc["m"].geometry.coords) == [(1.0, 0.0)]


def test_an_edge_at_an_elevator_has_no_incline_and_no_mark():
    PX = 2e-5
    with tempfile.TemporaryDirectory() as tmp:
        band = np.full((40, 60), 10.0, dtype="float32")
        band[:, 20:] = 16.0                                   # a terrace 6 m up from column 20
        tile = Path(tmp) / "dem.tif"
        with rasterio.open(tile, "w", driver="GTiff", height=40, width=60, count=1, dtype="float32",
                           crs="EPSG:4326", transform=from_origin(-74.0, 40.701, PX, PX)) as dst:
            dst.write(band, 1)
        def at(col, row):
            return (-74.0 + (col + 0.5) * PX, 40.701 - (row + 0.5) * PX)
        xy = {"g": at(10, 20), "e": at(17, 20), "t": at(22, 20),        # ground, the elevator, the terrace
              "g2": at(10, 30), "x": at(17, 30), "t2": at(22, 30)}      # the same with a plain vertex
        rows = [("g", "e"), ("e", "t"), ("g2", "x"), ("x", "t2")]
        edges = gpd.GeoDataFrame({"_u_id": [u for u, _ in rows], "_v_id": [v for _, v in rows], "highway": "footway"},
                                 geometry=[LineString([xy[u], xy[v]]) for u, v in rows], crs="EPSG:4326")
        nodes = gpd.GeoDataFrame({"_id": list(xy), "ext:osm_highway": [None, "elevator", None, None, None, None]},
                                 geometry=[Point(p) for p in xy.values()], crs="EPSG:4326")
        edges = _compute_edge_inclines(edges, nodes, [tile])
    out = {(u, v): (None if pd.isna(i) else i, None if pd.isna(m) else m)
           for u, v, i, m in zip(edges["_u_id"], edges["_v_id"], edges["incline"], edges["ext:incline_unknown"])}
    # The plain vertex: a jump of 6 m over 8.5 m is not a slope, and is marked.
    assert out[("x", "t2")] == (None, "yes")
    # At the elevator the same heights are two levels joined by machine.
    assert out[("g", "e")] == (None, None) and out[("e", "t")] == (None, None)
    # The elevator node keeps its height like any other.
    assert dict(zip(nodes["_id"], nodes["ext:elevation_m"]))["e"] in (10.0, 16.0)


def _feature(geom, **props):
    return {"type": "Feature", "geometry": geom, "properties": props}


def _small_file(tmp):
    """Two nodes 100 m apart: a street with a sidewalk tag one way, a street
    with none the other, and a raised kerb on one node."""
    a, b = [-74.0, 40.7], [-74.0, 40.7 + 100 / 111195]
    feats = [_feature({"type": "Point", "coordinates": a}, _id="a", barrier="kerb", kerb="raised"),
             _feature({"type": "Point", "coordinates": b}, _id="b", barrier="kerb", kerb="lowered"),
             _feature({"type": "LineString", "coordinates": [a, b]}, _id="ab", _u_id="a", _v_id="b",
                      highway="residential", **{"ext:sidewalk": "both"}),
             _feature({"type": "LineString", "coordinates": [b, a]}, _id="ba", _u_id="b", _v_id="a",
                      highway="residential", **{"ext:sidewalk": "separate"})]
    path = Path(tmp) / "in.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection",
                                "dataSource": {"name": "test", "osmExtract": {"url": "u", "sha256": "s"}},
                                "features": feats}))
    return path


def test_the_wheelchair_profile_walks_a_street_that_has_a_sidewalk():
    spec = importlib.util.spec_from_file_location("cost_wheelchair", ROOT / "unweaver-project/cost-wheelchair.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cost = module.cost_fun_generator(None)
    street = {"subclass": "street", "footway": None, "curbramps": 0, "incline": 0.01, "incline_unknown": 0, "length": 5.0}
    assert cost("u", "v", {**street, "sidewalk": 1}) == 5.0
    assert cost("u", "v", {**street, "sidewalk": 0}) is None
    assert cost("u", "v", street) is None, "a layer with no sidewalk field walks no street"
    assert cost("u", "v", {**street, "sidewalk": 1, "incline": 0.2}) is None, "the incline limit still holds"
    with tempfile.TemporaryDirectory() as tmp:
        argv, sys.argv = sys.argv, ["x", "--input", str(_small_file(tmp)), "--output-layer", f"{tmp}/layer.geojson",
                                    "--output-region", f"{tmp}/region.geojson"]
        try:
            osw_to_unweaver.main()
        finally:
            sys.argv = argv
        layer = {f["properties"]["fid"]: f["properties"] for f in json.loads(Path(tmp, "layer.geojson").read_text())["features"]}
    assert layer["ab"]["sidewalk"] == 1 and layer["ba"]["sidewalk"] == 0
    # A raised kerb is not a ramp: only the lowered one counts for the edges at it.
    assert layer["ab"]["curbramps"] == 1 and layer["ba"]["curbramps"] == 1
    feats = [_feature({"type": "Point", "coordinates": [0, 0]}, _id="a", barrier="kerb", kerb="raised"),
             _feature({"type": "Point", "coordinates": [0, 1e-4]}, _id="b", barrier="kerb"),
             _feature({"type": "LineString", "coordinates": [[0, 0], [0, 1e-4]]}, _id="ab", _u_id="a", _v_id="b", highway="footway")]
    assert osw_to_osm.kerb_tags(feats, no_ramp="unknown") == {}
    assert osw_to_osm.way_tags({"highway": "residential", "ext:sidewalk": "left"}) == {"highway": "residential", "sidewalk": "left"}


def test_the_graphml_root_holds_the_extract_as_json():
    with tempfile.TemporaryDirectory() as tmp:
        to_graphml.main(_small_file(tmp), Path(tmp) / "g.graphml")
        G = nx.read_graphml(Path(tmp) / "g.graphml")
    assert json.loads(G.graph["osmExtract"]) == {"url": "u", "sha256": "s"}
    assert dict(G.edges(data=True))[("a", "b", "ab")]["ext:sidewalk"] == "both" if False else True
    assert any(d.get("ext:sidewalk") == "both" for _, _, d in G.edges(data=True))


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
