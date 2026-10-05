"""Checks for the schema deviations of v0.3.3-nyc.1 that v0.3.4 fixed (crossing:markings, Pedestrian Road, foot, the warning surface
condition, ramp and width outliers, the sidecar licence, the version).

Run: python tests/test_schema_deviations.py
"""

import importlib.util
import json
import tempfile
import tomllib
from importlib.metadata import version
from pathlib import Path

import geopandas as gpd
import osmnx as ox
import yaml
from shapely.geometry import LineString, Point

from pipeline.stages.acquire import EXTRA_WAY_TAGS
from pipeline.stages.assemble import _merge_near_endpoints
from pipeline.stages.export import export_gapfill_sidecar
from pipeline.stages.schema_map import (
    _osm_crossing_markings,
    _osm_edges_to_osw,
    _osm_foot,
    _ramps_to_curb_nodes,
)
from scripts.osw_to_unweaver import crossing_ends, edge_class

ROOT = Path(__file__).resolve().parents[1]


def _ways(rows):
    """OSM edge rows (dicts of tags) on short separate lines, as Stage 3 reads them."""
    geoms = [LineString([(-73.97 + i * 1e-3, 40.77), (-73.9705 + i * 1e-3, 40.7705)])
             for i in range(len(rows))]
    return gpd.GeoDataFrame([{"key": 0, **r} for r in rows], geometry=geoms, crs="EPSG:4326")


def test_crossing_markings_reads_the_osm_tag_then_unambiguous_crossing_values():
    # OSM's own crossing:markings tag wins over crossing=*.
    assert _osm_crossing_markings("ladder", "traffic_signals") == "ladder"
    assert _osm_crossing_markings("no", "unmarked") == "no"
    assert _osm_crossing_markings("zebra", "uncontrolled") == "zebra"
    # Values the schema does not list: a variant becomes its base type, a
    # list of marking types says only that markings exist.
    assert _osm_crossing_markings("zebra:skewed", None) == "zebra"
    assert _osm_crossing_markings("zebra;lines;pictograms", None) == "yes"
    # With no usable tag, crossing=* as the schema's guidance reads it.
    assert _osm_crossing_markings(None, "marked") == "yes"
    assert _osm_crossing_markings(None, "zebra") == "yes"
    assert _osm_crossing_markings(None, "unmarked") == "no"
    assert _osm_crossing_markings("longitudinal_barss", "unmarked") == "no"
    # uncontrolled and traffic_signals say nothing about paint.
    assert _osm_crossing_markings(None, "uncontrolled") is None
    assert _osm_crossing_markings(None, "traffic_signals") is None
    assert _osm_crossing_markings(float("nan"), "nan") is None
    # Through Stage 3, with the tag as its own column.
    osm = _ways([
        {"osmid": 1, "highway": "footway", "footway": "crossing", "crossing": "uncontrolled"},
        {"osmid": 2, "highway": "footway", "footway": "crossing", "crossing": "unmarked"},
        {"osmid": 3, "highway": "footway", "footway": "crossing", "crossing": "traffic_signals",
         "crossing:markings": "ladder"},
    ])
    crossings = _osm_edges_to_osw(osm, "test", {})[1]
    got = dict(zip(crossings["ext:osm_id"], crossings["crossing:markings"]))
    assert got["3"] == "ladder" and got["2"] == "no" and str(got["1"]) == "nan"


def test_osm_crossing_markings_tag_is_requested_from_the_extract():
    assert "crossing:markings" in EXTRA_WAY_TAGS
    xml = """<?xml version='1.0' encoding='UTF-8'?>
<osm version="0.6">
 <node id="1" lat="40.7700" lon="-73.9700" version="1"/>
 <node id="2" lat="40.7701" lon="-73.9701" version="1"/>
 <way id="10" version="1"><nd ref="1"/><nd ref="2"/>
  <tag k="highway" v="footway"/><tag k="footway" v="crossing"/>
  <tag k="crossing" v="traffic_signals"/><tag k="crossing:markings" v="zebra"/>
 </way>
</osm>"""
    path = Path(tempfile.mkdtemp()) / "crossing.osm"
    path.write_text(xml)
    saved = ox.settings.useful_tags_way
    try:
        ox.settings.useful_tags_way = list(dict.fromkeys(saved + EXTRA_WAY_TAGS))
        G = ox.graph_from_xml(path, simplify=False, retain_all=True)
    finally:
        ox.settings.useful_tags_way = saved
    assert {d.get("crossing:markings") for _, _, d in G.edges(data=True)} == {"zebra"}


def test_pedestrian_ways_are_pedestrian_roads():
    osm = _ways([
        {"highway": "pedestrian"},
        {"highway": "pedestrian", "footway": "sidewalk"},
        {"highway": "path"},
        {"highway": "steps"},
    ])
    sidewalks, _, footways, _, _ = _osm_edges_to_osw(osm, "test", {})
    assert sorted(set(footways["highway"])) == ["footway", "pedestrian", "steps"]
    ped = footways[footways["highway"] == "pedestrian"]
    assert len(ped) == 2
    assert "footway" not in ped.columns or ped["footway"].isna().all()
    # A pedestrian way OSM marks as a sidewalk stays a Sidewalk.
    assert len(sidewalks) == 2 and set(sidewalks["highway"]) == {"footway"}


def test_pedestrian_road_is_walked_like_a_footway_by_the_routing_layer():
    assert edge_class({"highway": "pedestrian"}) == ("footway", None)
    assert edge_class({"highway": "footway", "footway": "sidewalk"}) == ("footway", "sidewalk")
    assert edge_class({"highway": "steps"}) == ("steps", None)
    assert edge_class({"highway": "residential"}) == ("street", None)
    spec = importlib.util.spec_from_file_location("cost_wheelchair", ROOT / "unweaver-project/cost-wheelchair.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    cost = mod.cost_fun_generator(None)
    subclass, footway = edge_class({"highway": "pedestrian"})
    edge = {"subclass": subclass, "footway": footway, "curbramps": 0, "incline": 0.01, "length": 12.0}
    assert cost("a", "b", edge) == 12.0
    # A crossing that ends on a Pedestrian Road ends on the network.
    line = {"type": "LineString", "coordinates": [[0, 0], [0, 0]]}
    feats = [
        {"geometry": line, "properties": {"_id": "x", "_u_id": "p", "_v_id": "q",
                                          "highway": "footway", "footway": "crossing"}},
        {"geometry": line, "properties": {"_id": "r", "_u_id": "q", "_v_id": "s", "highway": "pedestrian"}},
    ]
    _, ends = crossing_ends(feats, set(), {}, 5.0)
    assert list(ends.values()) == [{"q": False}]


def test_endpoint_merge_counts_pedestrian_roads_as_pedestrian():
    # Two walkways 1.5 m apart, each joined to the same street: one graph,
    # two pedestrian components. The merge joins them whatever the walkway's
    # highway value, as it did when pedestrian ways were written as footway.
    lines = [[(0, 0), (10, 0)], [(10, 0), (10, 10)], [(11.5, 0), (20, 0)], [(11.5, 0), (10, 10)]]
    for second in ("footway", "pedestrian"):
        highway = ["footway", "residential", second, "residential"]
        g = gpd.GeoDataFrame(geometry=[LineString(c) for c in lines], crs="EPSG:32618")
        g = g.translate(583000, 4500000).to_crs("EPSG:4326")
        edges = gpd.GeoDataFrame({"_id": [f"e{i}" for i in range(4)],
                                  "_u_id": [str(c[0]) for c in lines], "_v_id": [str(c[-1]) for c in lines],
                                  "highway": highway}, geometry=g.values, crs="EPSG:4326")
        _, remap = _merge_near_endpoints(edges, tolerance_m=2.0)
        assert len(remap) == 1, (second, remap)


def test_foot_is_carried_in_the_schema_values():
    assert [_osm_foot(v) for v in ("yes", "use_sidepath", "private", "customers", None, "nan")] == \
        ["yes", "use_sidepath", "private", None, None, None]
    osm = _ways([
        {"highway": "primary", "foot": "use_sidepath"},
        {"highway": "footway", "foot": "designated"},
        {"highway": "residential"},
    ])
    _, _, footways, streets, _ = _osm_edges_to_osw(osm, "test", {})
    assert set(footways["foot"]) == {"designated"}
    by_hw = dict(zip(streets["highway"], streets["foot"]))
    assert by_hw["primary"] == "use_sidepath" and str(by_hw["residential"]) == "nan"


def test_ramp_keeps_the_survey_condition_and_drops_slope_outliers():
    dws = ["Defective", "Off Ramp - Good", "Missing", "Not Applicable", None]
    ramps = gpd.GeoDataFrame({
        "rampid": range(5),
        "dws_conditions": dws,
        "counter_slope": ["4.5", "-300.3", "473.0", "100", "999"],
    }, geometry=[Point(-74 + i * 1e-4, 40.7) for i in range(5)], crs="EPSG:4326")
    out = _ramps_to_curb_nodes(ramps, "test", {})
    assert out["ext:dws_condition"].tolist()[:4] == dws[:4]
    assert out["ext:dws_condition"].isna().tolist()[4]
    # tactile_paving is unchanged: any surface is yes, Missing is no.
    assert out["tactile_paving"].tolist()[:3] == ["yes", "yes", "no"]
    assert out["ext:counter_slope_pct"].notna().tolist() == [True, False, False, True, False]


def test_zero_and_negative_widths_are_dropped():
    osm = _ways([{"highway": "footway", "width": w} for w in ("0", "-1", "2.5 m")])
    footways = _osm_edges_to_osw(osm, "test", {})[2]
    assert sorted(footways["width"].dropna().unique()) == [2.5]


def test_gap_fill_sidecar_is_odbl_with_the_graph_attribution():
    tmp = Path(tempfile.mkdtemp())
    (tmp / "gapfill_sidewalks.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": []}))
    source = {"license": "ODbL-1.0", "licenseUrl": "https://opendatacommons.org/licenses/odbl/1-0/",
              "attribution": "© OpenStreetMap contributors", "osmExtract": {"sha256": "x"}}
    out = export_gapfill_sidecar(tmp, {"dataSource": source, "pipelineVersion": {}}, tmp)
    root = json.loads(out.read_text())
    for key, value in source.items():
        assert root["dataSource"][key] == value
    assert "Public Domain" not in json.dumps(root)


def test_sources_manifest_describes_the_survey_and_carries_no_public_domain_label():
    text = (ROOT / "config/sources.yaml").read_text()
    sources = yaml.safe_load(text)["sources"]
    ramps = sources["nyc_dot_ramps"]["description"]
    assert "Cyclomedia" in ramps and "March 2017" in ramps and "January 2020" in ramps
    assert "April 2018" not in text
    assert not [k for k, s in sources.items() if "public domain" in s["license"].lower()]


def test_version_is_0_3_6():
    with open(ROOT / "pyproject.toml", "rb") as f:
        assert tomllib.load(f)["project"]["version"] == "0.3.6+nyc.1"
    # The installed metadata, which ext:pipeline_version is read from, follows
    # after uv pip install -e .
    assert version("opensidewalks-nyc") == "0.3.6+nyc.1"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
