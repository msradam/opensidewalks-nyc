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
from shapely.geometry import Point, Polygon

from pipeline.stages.assemble import _compute_edge_inclines
from pipeline.stages.schema_map import (
    _planimetric_to_sidewalk_edges,
    _ramps_to_curb_nodes,
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


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
