"""Checks for the v0.3.5 changes: pedestrian areas as Pedestrian Zones and
how a route crosses one, the root timestamps, the provenance of a ramp on an
OSM vertex, and the origin of an OSM path.

Run: python tests/test_v035.py
"""

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

from pipeline.stages.assemble import _merge_node_group, root_metadata
from pipeline.stages.schema_map import _osm_edges_to_osw
from pipeline.utils.ids import node_id
from pipeline.utils.zones import zone_edges


def _square(osmid, x0, highway, **tags):
    """The four edges of a closed area way, as OSMnx hands them to Stage 3."""
    pts = [(x0, 40.77), (x0 + 1e-3, 40.77), (x0 + 1e-3, 40.771), (x0, 40.771)]
    rows, geoms = [], []
    for i in range(4):
        rows.append({"key": 0, "osmid": osmid, "highway": highway, "area": "yes", **tags})
        geoms.append(LineString([pts[i], pts[(i + 1) % 4]]))
    return rows, geoms


def test_area_ways_become_pedestrian_zones_and_linear_ways_stay_edges():
    rows, geoms = _square(1, -73.97, "pedestrian", name="Plaza", surface="paving_stones")
    r2, g2 = _square(2, -73.96, "footway")
    rows += r2 + [{"key": 0, "osmid": 3, "highway": "pedestrian", "area": None}]
    geoms += g2 + [LineString([(-73.95, 40.77), (-73.949, 40.77)])]
    osm = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    _, _, footways, _, zones = _osm_edges_to_osw(osm, "test", {})
    assert len(zones) == 2 and set(zones["highway"]) == {"pedestrian"}
    plaza = zones[zones["ext:osm_id"] == "1"].iloc[0]
    assert plaza["name"] == "Plaza" and plaza["surface"] == "paving_stones"
    ring = list(plaza.geometry.exterior.coords)
    # _w_id[i] is ring vertex i, so a consumer can walk the ring from the ids.
    assert plaza["_w_id"] == [node_id(x, y) for x, y in ring[:-1]] and len(plaza["_w_id"]) == 4
    # A footway area is a zone too, and keeps what OSM called it.
    assert zones[zones["ext:osm_id"] == "2"].iloc[0]["ext:osm_highway"] == "footway"
    # The area's edges are gone; the linear pedestrian way is a Pedestrian Road, both ways.
    assert set(footways["ext:osm_id"]) == {"3"} and len(footways) == 2


def test_a_route_crosses_a_plaza_between_entrances_but_not_outside_it():
    # An L-shaped plaza. Entrances a and c sit on one straight side, so their
    # chord lies inside; the chord from d to f cuts across the notch, outside.
    xy = {"a": (0, 0), "b": (2, 0), "c": (4, 0), "d": (4, 2), "e": (2, 2), "f": (2, 4), "g": (0, 4)}
    xy = {k: (-74 + x * 1e-4, 40.7 + y * 1e-4) for k, (x, y) in xy.items()}
    zone = {"properties": {"_id": "z", "_w_id": list("abcdefg"), "name": "L"}}
    out = zone_edges(zone, xy, referenced={"a", "c", "d", "f"}, node_z={"a": 10.0, "c": 10.5})
    pairs = {(e["_u_id"], e["_v_id"]) for e in out}
    assert ("a", "c") in pairs and ("c", "a") in pairs       # a chord inside the plaza, both ways
    assert ("d", "f") not in pairs and ("c", "f") not in pairs    # across the notch
    assert ("b", "d") not in pairs                           # b is no entrance
    assert ("a", "b") in pairs and ("g", "a") in pairs       # the ring itself
    ac = next(e for e in out if (e["_u_id"], e["_v_id"]) == ("a", "c"))
    assert ac["ext:zone"] == "z" and ac["name"] == "L" and 33 < ac["length_m"] < 35
    assert abs(ac["incline"] - 0.5 / ac["length_m"]) < 1e-3
    # No incline without a height at both ends.
    ab = next(e for e in out if (e["_u_id"], e["_v_id"]) == ("a", "b"))
    assert ab["incline"] is None
    # Nor on a chord under 5 m.
    short = zone_edges({"properties": {"_id": "s", "_w_id": ["a", "b", "c"]}},
                       {"a": (-74, 40.7), "b": (-74 + 3e-5, 40.7), "c": (-74, 40.7 + 3e-5)},
                       referenced={"a", "b"}, node_z={"a": 1.0, "b": 2.0})
    assert all(e["incline"] is None for e in short)


def test_root_timestamps_mean_what_the_schema_says():
    extract = {"url": "u", "content_hash": "h", "osm_data_timestamp": "2026-10-01T20:22:06Z"}
    root = root_metadata({}, extract, "0.3.5+nyc.1", "abc1234")
    assert root["dataTimestamp"] == "2026-10-01T20:22:06Z"
    assert root["pipelineVersion"]["builtAt"] != root["dataTimestamp"]
    assert root["pipelineVersion"]["name"] == "opensidewalks-nyc"
    assert root["pipelineVersion"]["url"].endswith("/tree/abc1234")
    assert "independent" in root["dataSource"]["attribution"]
    assert root["dataSource"]["name"].startswith("OpenStreetMap")


def test_a_ramp_on_an_osm_vertex_says_its_values_are_the_surveys():
    group = pd.DataFrame([
        {"_id": "n", "ext:source": "osm_walk", "ext:source_timestamp": None, "barrier": None, "kerb": None},
        {"_id": "n", "ext:source": "nyc_dot_ramps", "ext:source_timestamp": "2026-10-04T02:21:00Z",
         "barrier": "kerb", "kerb": "lowered"},
    ])
    merged = _merge_node_group(group, {"barrier", "kerb"})
    assert merged["barrier"] == "kerb" and merged["ext:source"] == "nyc_dot_ramps"
    assert merged["ext:source_timestamp"] == "2026-10-04T02:21:00Z"
    plain = _merge_node_group(group.iloc[:1], {"barrier", "kerb"})
    assert plain["ext:source"] == "osm_walk"


def test_an_osm_path_keeps_its_origin():
    osm = gpd.GeoDataFrame([{"key": 0, "osmid": 1, "highway": "path"}, {"key": 0, "osmid": 2, "highway": "footway"}],
                           geometry=[LineString([(-73.97, 40.77), (-73.969, 40.77)]),
                                     LineString([(-73.96, 40.77), (-73.959, 40.77)])], crs="EPSG:4326")
    footways = _osm_edges_to_osw(osm, "test", {})[2]
    by_way = dict(zip(footways["ext:osm_id"], footways["ext:osm_highway"]))
    assert by_way["1"] == "path" and set(footways["highway"]) == {"footway"} and pd.isna(by_way["2"])


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
