"""Checks for defects found in the v0.3.1-nyc.1 review. Each passed the
official validator, so each needs its own check.

Run: python tests/test_validity_fixes.py
"""

import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, Point, Polygon

from pipeline.stages.assemble import _compute_edge_inclines, _merge_near_endpoints
from pipeline.stages.schema_map import (
    _classify_osm_edge,
    _osm_edges_to_osw,
    _planimetric_to_sidewalk_edges,
    _ramps_to_curb_nodes,
    borough_code,
)


def test_dem_tiles_do_not_zero_nodes_outside_them():
    tmp = Path(tempfile.mkdtemp())
    tiles = []
    # Two 10x10 tiles side by side, 50 m and 100 m, no nodata value.
    for name, west, elev in [("dem_a.tif", -74.0, 50.0), ("dem_b.tif", -73.9, 100.0)]:
        path = tmp / name
        with rasterio.open(path, "w", driver="GTiff", width=10, height=10, count=1,
                           dtype="float32", crs="EPSG:4326",
                           transform=from_origin(west, 40.8, 0.01, 0.01)) as dst:
            dst.write(np.full((10, 10), elev, dtype="float32"), 1)
        tiles.append(path)
    nodes = gpd.GeoDataFrame({"_id": ["in_a", "in_b"]},
                             geometry=[Point(-73.95, 40.75), Point(-73.85, 40.75)],
                             crs="EPSG:4326")
    edges = gpd.GeoDataFrame({"_id": [], "_u_id": [], "_v_id": []}, geometry=[], crs="EPSG:4326")
    _compute_edge_inclines(edges, nodes, tiles)
    assert nodes["ext:elevation_m"].tolist() == [50.0, 100.0]


def test_dem_is_interpolated_between_pixel_centres():
    tmp = Path(tempfile.mkdtemp())
    # One row of pixels rising 10 m per pixel from west to east.
    with rasterio.open(tmp / "dem.tif", "w", driver="GTiff", width=4, height=1, count=1,
                       dtype="float32", crs="EPSG:4326",
                       transform=from_origin(-74.0, 40.8, 0.01, 0.01)) as dst:
        dst.write(np.array([[0, 10, 20, 30]], dtype="float32"), 1)
    # Pixel centres are at -73.995, -73.985, ... A node a quarter of the way
    # from the first centre to the second reads 2.5 m, not 0 or 10.
    nodes = gpd.GeoDataFrame({"_id": ["centre", "quarter"]},
                             geometry=[Point(-73.985, 40.795), Point(-73.9925, 40.795)],
                             crs="EPSG:4326")
    edges = gpd.GeoDataFrame({"_id": [], "_u_id": [], "_v_id": []}, geometry=[], crs="EPSG:4326")
    _compute_edge_inclines(edges, nodes, [tmp / "dem.tif"])
    assert nodes["ext:elevation_m"].tolist() == [10.0, 2.5]


def test_ramp_dws_and_sentinels():
    dws = ["Missing", "Good Condition", "Defective", "Not Applicable",
           "Off Ramp - Good", "Off Ramp-Defective"]
    ramps = gpd.GeoDataFrame({
        "rampid": range(6),
        "dws_conditions": dws,
        "ramp_running_slope_total": ["8.1", "999", "888", "777", "555", "-3.2"],
    }, geometry=[Point(-74 + i * 1e-4, 40.7) for i in range(6)], crs="EPSG:4326")
    out = _ramps_to_curb_nodes(ramps, "test", {})
    assert out["tactile_paving"].tolist()[:3] == ["no", "yes", "yes"]
    assert out["tactile_paving"].isna().tolist() == [False, False, False, True, False, False]
    slopes = out["ext:running_slope_pct"]
    assert slopes.notna().tolist() == [True, False, False, False, False, True]


def test_gap_fill_rejects_block_rings_and_keeps_strips():
    ring = Polygon([(0, 0), (80, 0), (80, 60), (0, 60)],
                   holes=[[(3, 3), (77, 3), (77, 57), (3, 57)]])
    strip = Polygon([(200, 0), (280, 0), (280, 3), (200, 3)])
    plan = gpd.GeoDataFrame(geometry=[ring, strip], crs="EPSG:32618").translate(583000, 4500000)
    plan = gpd.GeoDataFrame(geometry=plan).to_crs("EPSG:4326")
    none = gpd.GeoDataFrame(columns=["_id", "geometry"], geometry="geometry", crs="EPSG:4326")
    out = _planimetric_to_sidewalk_edges(plan, none, {}, "test", {})
    assert len(out) == 2, "one strip, both directions"
    assert set(out["_u_id"]) == set(out["_v_id"])
    assert abs(out["width"].iloc[0] - 2.89) < 0.05  # 2*240/166, a 3 m strip
    # The same strip with an OSM crossing already running along it (a median
    # refuge) is a duplicate and yields nothing.
    crossing = gpd.GeoDataFrame(geometry=[LineString([(190, 1.5), (290, 1.5)])],
                                crs="EPSG:32618").translate(583000, 4500000)
    crossing = gpd.GeoDataFrame(geometry=crossing).to_crs("EPSG:4326")
    assert len(_planimetric_to_sidewalk_edges(plan, none, {}, "test", {},
                                              osm_pedestrian_gdf=crossing)) == 0


def test_endpoint_merge_closes_gaps_without_chaining():
    def edges(lines, highway="footway"):
        g = gpd.GeoDataFrame(geometry=[LineString(c) for c in lines], crs="EPSG:32618")
        g = g.translate(583000, 4500000).to_crs("EPSG:4326")
        ends = [(str(c[0]), str(c[-1])) for c in lines]
        return gpd.GeoDataFrame({"_id": [f"e{i}" for i in range(len(lines))],
                                 "_u_id": [a for a, _ in ends], "_v_id": [b for _, b in ends],
                                 "highway": highway}, geometry=g.values, crs="EPSG:4326")
    # A path with a vertex every metre: the old merge chained all of it into
    # one node. Nothing here is a gap, so nothing may move or be dropped.
    path = [[(i, 0), (i + 1, 0)] for i in range(10)]
    out, remap = _merge_near_endpoints(edges(path), tolerance_m=2.0)
    assert remap == {} and len(out) == 10
    # A second path stops 1.5 m short of the first one's far end: its dead
    # end moves onto that node, and only it moves.
    stub = [[(11.5, 0), (20, 0)], [(20, 0), (30, 0)]]
    out, remap = _merge_near_endpoints(edges(path + stub), tolerance_m=2.0)
    assert remap in ({"(11.5, 0)": "(10, 0)"}, {"(10, 0)": "(11.5, 0)"}) and len(out) == 12
    # A dead end 3 m away is out of tolerance and stays.
    far = [[(13, 0), (20, 0)]]
    assert _merge_near_endpoints(edges(path + far), tolerance_m=2.0)[1] == {}


def test_shared_paths_are_walkable_only_where_osm_says_so():
    assert _classify_osm_edge({"highway": "cycleway", "foot": "designated"}) == "footway"
    assert _classify_osm_edge({"highway": "cycleway", "foot": "yes", "footway": "crossing"}) == "crossing"
    assert _classify_osm_edge({"highway": "track", "foot": "permissive"}) == "footway"
    assert _classify_osm_edge({"highway": "cycleway"}) is None
    assert _classify_osm_edge({"highway": "cycleway", "foot": "use_sidepath"}) is None


def test_one_way_pedestrian_edges_get_their_reverse():
    osm = gpd.GeoDataFrame({"highway": ["pedestrian", "residential"], "key": [0, 0]},
                           geometry=[LineString([(-73.97, 40.77), (-73.971, 40.771)]),
                                     LineString([(-73.98, 40.77), (-73.981, 40.771)])], crs="EPSG:4326")
    sidewalks, crossings, footways, streets = _osm_edges_to_osw(osm, "test", {})
    assert len(footways) == 2 and len(streets) == 1, "the street stays one-way"
    assert footways["_u_id"].tolist() == footways["_v_id"].tolist()[::-1]
    assert footways["_id"].is_unique


def test_borough_names_become_codes():
    assert [borough_code(v) for v in ("queens", "bronx_county", "Staten Island", "MN", "study_area")] == \
        ["QN", "BX", "SI", "MN", "study_area"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
