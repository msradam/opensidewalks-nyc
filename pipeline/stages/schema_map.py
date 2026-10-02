"""Stage 3: Map cleaned source data to OpenSidewalks v0.3 schema features.

Input:  data/clean/{source_id}.geojson
Output: data/staged/{feature_type}.geojson

Transformations:
  OSM edges → Sidewalk Edges, Crossing Edges, Footway Edges, Street Edges
  DOT ramps → CurbRamp Point Nodes (barrier=kerb, kerb=lowered)
  Planimetric polygons → gap-fill Sidewalk Edges (centerline derived)
  Borough boundaries → per-feature ext:borough tags + root region MultiPolygon

OSW v0.3 schema reference: https://sidewalks.washington.edu/opensidewalks/0.3/schema.json

Key schema rules implemented here:
  - Crossings exist only on road surfaces (footway=crossing, highway=footway)
  - Sidewalks are first-class edges (highway=footway, footway=sidewalk), not
    attributes of adjacent streets
  - Curb interfaces are Point Nodes (barrier=kerb), never edge attributes
  - Non-canonical fields are prefixed ext:
"""

import hashlib
import json
from pathlib import Path

import click
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, mapping
from shapely.ops import unary_union

from pipeline.utils.ids import edge_id, feature_id, node_id
from pipeline.utils.provenance import load_manifest, provenance_fields


# ---------------------------------------------------------------------------
# OSW v0.3 canonical surface and crossing:markings enums
# (verified from the OpenSidewalks-Schema v0.3 subschemas)
# ---------------------------------------------------------------------------

SURFACE_ENUM = frozenset([
    "asphalt", "concrete", "dirt", "grass", "grass_paver",
    "gravel", "paved", "paving_stones", "unpaved",
])

_BOROUGH_CODES = {
    "manhattan": "MN",
    "brooklyn": "BK",
    "queens": "QN",
    "bronx": "BX",
    "bronx_county": "BX",
    "the_bronx": "BX",
    "staten_island": "SI",
}



def borough_code(value):
    """MN/BK/QN/BX/SI for any of the ways the sources name a borough."""
    if pd.isna(value):
        return value
    return _BOROUGH_CODES.get(str(value).strip().lower().replace(" ", "_"), value)


CROSSING_MARKINGS_ENUM = frozenset([
    "zebra", "zebra:double", "zebra:paired", "zebra:bicolour",
    "lines", "lines:paired", "lines:rainbow",
    "dashes", "dots",
    "ladder", "ladder:paired", "ladder:skewed",
    "pictograms", "rainbow", "surface", "yes", "no",
])

# NYC DOT ramp survey codes. The slope and dimension fields use these values
# for "no measurement" (999 is mostly cut-through ramps, which have no slope).
_DOT_SENTINELS = frozenset([555.0, 777.0, 888.0, 999.0])

# DWS_CONDITIONS values that mean a detectable warning surface is there.
_DWS_PRESENT = frozenset([
    "good condition", "defective", "off ramp - good", "off ramp-defective",
])

# OSM highway tags that map to OSW footway/sidewalk edges.
FOOTWAY_TYPES = frozenset(["footway", "path", "pedestrian", "steps"])

# OSM highway tags that are walkable only where OSM says so: a cycleway or a
# track becomes a footway edge when it carries foot=yes, designated or
# permissive, and is dropped otherwise. Many bridge paths and greenways are
# mapped this way (the Williamsburg, Third Avenue and Kosciuszko bridge paths
# among them); dropping them all cut those bridges.
SHARED_TYPES = frozenset(["cycleway", "track"])
FOOT_ALLOWED = frozenset(["yes", "designated", "permissive"])

# OSM highway tags that map to OSW street edges (not sidewalk-class).
STREET_TYPES = frozenset([
    "residential", "service", "tertiary", "secondary", "primary",
    "living_street", "unclassified",
])


# ---------------------------------------------------------------------------
# Helper: polygon centerline via minimum rotated rectangle
# ---------------------------------------------------------------------------

def _polygon_centerline(polygon) -> LineString | None:
    """Extract an approximate centerline from an elongated polygon.

    Uses the minimum rotated rectangle (MRR) approach: find the bounding
    rectangle with minimum area, then return the line connecting midpoints of
    the two short sides. This is O(1) per polygon and works well for the
    elongated strip geometry typical of sidewalk polygons.

    For irregular polygons where the MRR aspect ratio is near 1 (blob-shaped),
    the MRR centerline is less meaningful but still geometrically valid.

    Handles both Polygon and MultiPolygon geometries.
    """
    import numpy as np

    if polygon is None or polygon.is_empty:
        return None

    # For MultiPolygon, process the largest component.
    if polygon.geom_type == "MultiPolygon":
        parts = sorted(polygon.geoms, key=lambda p: p.area, reverse=True)
        for part in parts:
            result = _polygon_centerline(part)
            if result is not None:
                return result
        return None

    if polygon.geom_type != "Polygon":
        return None

    # Get the minimum rotated rectangle (4 corners in order).
    mrr = polygon.minimum_rotated_rectangle
    if mrr is None or mrr.is_empty:
        return None

    coords = list(mrr.exterior.coords)[:-1]  # drop closing duplicate
    if len(coords) != 4:
        return None

    # Identify the two pairs of opposite sides; pick the pair of SHORT sides.
    # Short sides are perpendicular to the long axis. Their midpoints define
    # the centerline endpoints.
    c = np.array(coords)
    sides = [
        (c[0], c[1]),   # side 0
        (c[1], c[2]),   # side 1
        (c[2], c[3]),   # side 2
        (c[3], c[0]),   # side 3
    ]
    lengths = [np.linalg.norm(b - a) for a, b in sides]

    # The MRR has two pairs of parallel sides: (0,2) and (1,3).
    # The short pair gives the centerline; the long pair is the length axis.
    if lengths[0] + lengths[2] <= lengths[1] + lengths[3]:
        # sides 0 and 2 are the short pair → midpoints define the centerline
        mid0 = ((sides[0][0] + sides[0][1]) / 2)
        mid2 = ((sides[2][0] + sides[2][1]) / 2)
    else:
        # sides 1 and 3 are the short pair
        mid0 = ((sides[1][0] + sides[1][1]) / 2)
        mid2 = ((sides[3][0] + sides[3][1]) / 2)

    centerline = LineString([mid0.tolist(), mid2.tolist()])
    if centerline.length < 0.5:
        return None
    return centerline


# ---------------------------------------------------------------------------
# OSM → OSW edge mapping
# ---------------------------------------------------------------------------

def _osm_surface(osm_surface: str | None) -> str | None:
    """Map an OSM surface tag to the nearest OSW surface enum value."""
    if not osm_surface:
        return None
    s = str(osm_surface).lower().strip().split("|")[0]  # take first if pipe-joined
    if s in SURFACE_ENUM:
        return s
    # Common OSM variants not in enum → nearest canonical.
    mapping_table = {
        "tar": "asphalt", "tarmac": "asphalt", "bituminous": "asphalt",
        "cobblestone": "paving_stones", "sett": "paving_stones",
        "stone": "paving_stones", "brick": "paving_stones",
        "compacted": "unpaved", "fine_gravel": "gravel",
        "sand": "unpaved", "earth": "dirt", "mud": "dirt",
        "wood": "paved", "metal": "paved",
    }
    return mapping_table.get(s, None)


def _osm_crossing_markings(osm_crossing: str | None) -> str | None:
    """Map OSM crossing tag to OSW crossing:markings enum."""
    if not osm_crossing:
        return None
    c = str(osm_crossing).lower().strip()
    if c in CROSSING_MARKINGS_ENUM:
        return c
    mapping_table = {
        "marked": "yes", "uncontrolled": "zebra",
        "traffic_signals": "yes", "toucan": "yes",
        "pelican": "yes", "pegasus": "yes",
        "zebra_old_style": "zebra",
    }
    return mapping_table.get(c, None)


def _classify_osm_edge(row: pd.Series) -> str | None:
    """Return the OSW feature type for an OSM edge row, or None to skip.

    Returns one of: 'sidewalk', 'crossing', 'footway', 'street', None.

    Note: OSMnx 2.0 does not include 'footway' or 'crossing' sub-tags in its
    default useful_tags_way; acquire_osm extends useful_tags_way to preserve
    them. When those columns are absent (downloads made without that setting),
    all highway=footway edges fall back to generic 'footway' rather than
    'sidewalk'/'crossing'.
    """
    highway = str(row.get("highway", "")).lower().split("|")[0].strip()

    # footway sub-tag: may be absent in older downloads (see note above).
    footway_raw = row.get("footway")
    footway = "" if footway_raw is None or str(footway_raw) in ("nan", "None", "") else str(footway_raw).lower().strip()

    # crossing tag: alternate indicator that an edge is a road crossing.
    crossing_raw = row.get("crossing")
    has_crossing = crossing_raw is not None and str(crossing_raw) not in ("nan", "None", "no", "")

    foot = str(row.get("foot", "")).lower().strip()
    if highway in FOOTWAY_TYPES or (highway in SHARED_TYPES and foot in FOOT_ALLOWED):
        if footway == "crossing" or has_crossing:
            return "crossing"
        elif footway == "sidewalk":
            return "sidewalk"
        elif highway == "steps":
            return "footway"
        else:
            return "footway"
    elif highway in STREET_TYPES:
        return "street"
    return None


def _osm_edges_to_osw(edges_gdf: gpd.GeoDataFrame, pipeline_version: str,
                       manifest: dict) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame,
                                                gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Convert OSM edges GeoDataFrame to four OSW edge GeoDataFrames.

    Returns (sidewalks, crossings, footways, streets).
    """
    prov = provenance_fields("osm_walk", manifest, pipeline_version)

    sidewalk_rows  = []
    crossing_rows  = []
    footway_rows   = []
    street_rows    = []

    for _, row in edges_gdf.iterrows():
        edge_type = _classify_osm_edge(row)
        if edge_type is None:
            continue

        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        coords = list(geom.coords)
        u_lon, u_lat = coords[0]
        v_lon, v_lat = coords[-1]

        uid = node_id(u_lon, u_lat)
        vid = node_id(v_lon, v_lat)
        # Build a geometry-stable edge ID. OSMnx MultiDiGraph can produce parallel
        # edges with identical u/v endpoints and key. Include a hash of intermediate
        # coords to guarantee global uniqueness while keeping IDs deterministic.
        osm_key  = str(row.get("key", "0"))
        mid_sig  = hashlib.md5(
            "|".join(f"{c[0]:.6f},{c[1]:.6f}" for c in coords[1:-1]).encode()
        ).hexdigest()[:8]
        eid = edge_id(u_lon, u_lat, v_lon, v_lat, f"{edge_type}_{osm_key}_{mid_sig}", "osm_walk")

        props = {
            "_id":    eid,
            "_u_id":  uid,
            "_v_id":  vid,
            "highway": "footway" if edge_type in ("sidewalk", "crossing", "footway") else row.get("highway", ""),
            **prov,
        }

        # ext:borough from OSMnx merge step.
        if "ext:borough" in row:
            props["ext:borough"] = row["ext:borough"]
        elif "ext_borough" in row:
            props["ext:borough"] = row["ext_borough"]

        surface = _osm_surface(str(row.get("surface", "")))
        if surface:
            props["surface"] = surface

        if "name" in row and row["name"] and str(row["name"]) not in ("nan", "None", ""):
            props["name"] = str(row["name"])

        # Width from OSM (in metres if numeric).
        width_raw = row.get("width")
        if width_raw and str(width_raw) not in ("nan", "None", ""):
            try:
                props["width"] = float(str(width_raw).replace("m", "").strip())
            except ValueError:
                pass

        osmid = row.get("osmid")
        if osmid is not None and str(osmid) not in ("nan", "None", ""):
            props["ext:osm_id"] = str(osmid)

        # The terrain model is bare earth: on a bridge or in a tunnel it gives
        # the ground or water below or above, so Stage 4 leaves incline off
        # these edges. A building passage is at ground level and keeps it.
        bridge = str(row.get("bridge") or "").split("|")[0].lower()
        tunnel = str(row.get("tunnel") or "").split("|")[0].lower()
        if bridge not in ("", "nan", "none", "no"):
            props["ext:structure"] = "bridge"
        elif tunnel not in ("", "nan", "none", "no", "building_passage"):
            props["ext:structure"] = "tunnel"

        # A shared path is written as highway=footway; keep what OSM called it.
        highway_osm = str(row.get("highway", "")).split("|")[0]
        if highway_osm in SHARED_TYPES and edge_type != "street":
            props["ext:osm_highway"] = highway_osm

        if edge_type == "sidewalk":
            props["footway"] = "sidewalk"
            sidewalk_rows.append({**props, "geometry": geom})

        elif edge_type == "crossing":
            props["footway"] = "crossing"
            cross_mark = _osm_crossing_markings(str(row.get("crossing", "")))
            if cross_mark:
                props["crossing:markings"] = cross_mark
            crossing_rows.append({**props, "geometry": geom})

        elif edge_type == "footway":
            if str(row.get("highway", "")).split("|")[0] == "steps":
                props["highway"] = "steps"
            footway_rows.append({**props, "geometry": geom})

        elif edge_type == "street":
            highway_raw = str(row.get("highway", "")).split("|")[0]
            props["highway"] = highway_raw
            street_rows.append({**props, "geometry": geom})

    # OSM's oneway=yes binds vehicles and bicycles, not people on foot, but
    # the graph builder honours it for every way (Central Park's drives, the
    # reservoir track, one-way bike paths, and every one-way street). This is
    # a pedestrian graph, and a person walks along a one-way street in either
    # direction, so give each edge that came through in one direction its
    # reverse. Counted per kind: streets are most of them.
    walk_rows = {"sidewalk": sidewalk_rows, "crossing": crossing_rows,
                 "footway": footway_rows, "street": street_rows}
    have = {(r["_u_id"], r["_v_id"]) for rows in walk_rows.values() for r in rows}
    for kind, rows in walk_rows.items():
        one_way = [r for r in rows if (r["_v_id"], r["_u_id"]) not in have]
        for r in one_way:
            back = list(r["geometry"].coords)[::-1]
            rows.append({
                **r,
                "_id": edge_id(*back[0], *back[-1], f"reverse_of_{r['_id']}", "osm_walk"),
                "_u_id": r["_v_id"],
                "_v_id": r["_u_id"],
                "geometry": LineString(back),
            })
        if one_way:
            click.echo(f"    Added the reverse of {len(one_way)} one-way {kind} edges")

    def _to_gdf(rows, geom_type_label):
        if not rows:
            click.echo(f"    Warning: no {geom_type_label} edges found")
            return gpd.GeoDataFrame(columns=["_id", "_u_id", "_v_id", "geometry"],
                                    geometry="geometry", crs="EPSG:4326")
        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
        click.echo(f"    OSM → {geom_type_label}: {len(gdf)} edges")
        return gdf

    return (
        _to_gdf(sidewalk_rows, "sidewalks"),
        _to_gdf(crossing_rows, "crossings"),
        _to_gdf(footway_rows, "footways"),
        _to_gdf(street_rows, "streets"),
    )


# ---------------------------------------------------------------------------
# DOT ramps → CurbRamp Point Nodes
# ---------------------------------------------------------------------------

def _ramps_to_curb_nodes(ramps_gdf: gpd.GeoDataFrame, pipeline_version: str,
                          manifest: dict) -> gpd.GeoDataFrame:
    """Convert DOT ramp points to OSW CurbRamp Point Nodes.

    Schema: barrier=kerb, kerb=lowered (CurbRamp type in OSW v0.3).
    Each ramp becomes a Point Node at its survey location.
    """
    prov = provenance_fields("nyc_dot_ramps", manifest, pipeline_version)
    rows = []

    for _, ramp in ramps_gdf.iterrows():
        geom = ramp.geometry
        if geom is None or geom.is_empty:
            continue
        lon, lat = geom.x, geom.y
        nid = node_id(lon, lat)

        props = {
            "_id":     nid,
            "barrier": "kerb",
            "kerb":    "lowered",
            **prov,
        }

        if "borough" in ramp:
            props["ext:borough"] = str(ramp["borough"])

        # Tactile paving from the survey's detectable warning surface status.
        # "Missing" is the most common value, so a non-empty field does not
        # mean a surface exists. "Not Applicable" and blanks stay untagged.
        dws = str(ramp.get("dws_conditions") or "").strip().lower()
        if dws in _DWS_PRESENT:
            props["tactile_paving"] = "yes"
        elif dws == "missing":
            props["tactile_paving"] = "no"

        # Preserve key ramp identifiers as ext: fields for auditability.
        for orig_key, ext_key in [("rampid", "ext:ramp_id"),
                                   ("cornerid", "ext:corner_id"),
                                   ("stname1", "ext:street_1"),
                                   ("stname2", "ext:street_2")]:
            val = ramp.get(orig_key)
            if val and str(val) not in ("nan", "None", ""):
                props[ext_key] = str(val)

        # DOT survey slope measurements, in percent, signed by direction. The
        # survey codes "no measurement" as 555, 777, 888 or 999 (see SCHEMA.md);
        # omit those rather than carry them.
        for orig_key, ext_key in [("ramp_running_slope_total", "ext:running_slope_pct"),
                                   ("ramp_cross_slope", "ext:cross_slope_pct"),
                                   ("counter_slope", "ext:counter_slope_pct")]:
            try:
                slope = float(ramp.get(orig_key))
            except (TypeError, ValueError):
                continue
            if slope not in _DOT_SENTINELS:
                props[ext_key] = slope

        rows.append({**props, "geometry": geom})

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    click.echo(f"    DOT ramps → CurbRamp nodes: {len(gdf)}")
    return gdf


# ---------------------------------------------------------------------------
# Planimetric sidewalk polygons → gap-fill Sidewalk Edges
# ---------------------------------------------------------------------------

def _planimetric_to_sidewalk_edges(
    planimetric_gdf: gpd.GeoDataFrame,
    existing_sidewalks_gdf: gpd.GeoDataFrame,
    build_cfg: dict,
    pipeline_version: str,
    manifest: dict,
    osm_pedestrian_gdf: gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame:
    """Derive sidewalk edges from planimetric polygons where OSM coverage is sparse.

    Coverage check: if a planimetric polygon has an OSM sidewalk edge within
    `planimetric_coverage_threshold_meters`, it's already covered. Skip it.

    For uncovered polygons, extract a centerline via the minimum rotated
    rectangle and emit it as a Sidewalk Edge, unless OSM already has a
    crossing or footway along it (osm_pedestrian_gdf: every OSM pedestrian
    edge, of any kind).
    """
    prov = provenance_fields("nyc_planimetric_sidewalks", manifest, pipeline_version)
    coverage_threshold = build_cfg.get("planimetric_coverage_threshold_meters", 10.0)
    min_area_m2        = build_cfg.get("planimetric_min_area_m2", 20.0)

    # Work in NYC projected CRS (EPSG:32618, UTM Zone 18N) for metric operations.
    plan_proj = planimetric_gdf.to_crs("EPSG:32618")
    sw_proj   = existing_sidewalks_gdf.to_crs("EPSG:32618") if len(existing_sidewalks_gdf) > 0 else None

    # Filter out slivers smaller than min_area_m2.
    area_mask = plan_proj.geometry.area >= min_area_m2
    plan_proj = plan_proj[area_mask].copy()
    click.echo(f"    Planimetric: {len(plan_proj)} polygons above {min_area_m2} m² threshold")

    # Build spatial index over existing OSM sidewalk edges.
    if sw_proj is not None and len(sw_proj) > 0:
        sw_sindex = sw_proj.sindex
        have_existing = True
    else:
        have_existing = False
        click.echo("    No existing OSM sidewalks. Deriving centerlines for all planimetric polygons")

    from tqdm import tqdm

    ped_geoms = (osm_pedestrian_gdf.to_crs("EPSG:32618").geometry.values
                 if osm_pedestrian_gdf is not None and len(osm_pedestrian_gdf) > 0 else None)
    ped_tree = shapely.STRtree(ped_geoms) if ped_geoms is not None else None

    rows = []
    n_skipped_covered = 0
    n_skipped_duplicate = 0
    n_centerline_ok   = 0
    n_centerline_fail = 0

    for _, poly_row in tqdm(plan_proj.iterrows(), total=len(plan_proj),
                            desc="    Centerlines", unit=" poly", leave=False):
        poly = poly_row.geometry
        if poly is None or poly.is_empty:
            continue

        # Coverage check: any OSM sidewalk within threshold of this polygon's boundary?
        if have_existing:
            poly_buffered = poly.buffer(coverage_threshold)
            candidates = list(sw_sindex.intersection(poly_buffered.bounds))
            covered = any(
                sw_proj.iloc[c].geometry.intersects(poly_buffered)
                for c in candidates
            )
            if covered:
                n_skipped_covered += 1
                continue

        # Derive centerline from polygon (in projected coordinates).
        centerline = _polygon_centerline(poly)
        if centerline is None or centerline.is_empty or centerline.length < 2.0:
            n_centerline_fail += 1
            continue

        # The rectangle axis only stands for the sidewalk when the polygon is
        # a strip. For a ring around a block, or any irregular shape, it cuts
        # straight through the block interior, so require the line to stay
        # inside its own polygon.
        if centerline.intersection(poly).length < 0.9 * centerline.length:
            n_centerline_fail += 1
            continue

        # OSM may already have this strip as a crossing or a plain footway: a
        # median refuge inside a crossing, a path mapped without
        # footway=sidewalk. A centerline on top of that is a duplicate (in a
        # Midtown window 39 of 51 gap-fill edges were).
        if ped_tree is not None:
            near = ped_tree.query(centerline, predicate="dwithin", distance=1.5)
            if len(near) and centerline.intersection(shapely.union_all(
                    shapely.buffer(ped_geoms[near], 1.5))).length >= 0.5 * centerline.length:
                n_skipped_duplicate += 1
                continue

        # Reproject centerline back to WGS-84.
        from pyproj import Transformer
        transformer = Transformer.from_crs("EPSG:32618", "EPSG:4326", always_xy=True)
        projected_coords = list(centerline.coords)
        wgs84_coords = [transformer.transform(x, y) for x, y in projected_coords]
        centerline_wgs84 = LineString(wgs84_coords)

        coords = list(centerline_wgs84.coords)
        u_lon, u_lat = coords[0]
        v_lon, v_lat = coords[-1]
        uid = node_id(u_lon, u_lat)
        vid = node_id(v_lon, v_lat)
        # Include polygon centroid in the ID to disambiguate edges whose
        # MRR-derived endpoints happen to be identical across different polygons.
        c = poly.centroid
        eid = edge_id(u_lon, u_lat, v_lon, v_lat,
                      f"sidewalk|{c.x:.6f},{c.y:.6f}", "nyc_planimetric_sidewalks")

        # Sidewalk width from the polygon: 2*area/perimeter (Cauchy mean width).
        # Works well for elongated strips; polygon is in EPSG:32618 (metres).
        # .length counts interior rings too: many planimetric sidewalks are
        # rings around a block, and the outer ring alone doubles the width.
        perimeter = poly.length
        width_m = round(2.0 * poly.area / perimeter, 2) if perimeter > 0 else None

        props = {
            "_id":     eid,
            "_u_id":   uid,
            "_v_id":   vid,
            "highway": "footway",
            "footway": "sidewalk",
            **prov,
        }
        if width_m is not None:
            props["width"] = width_m

        rows.append({**props, "geometry": centerline_wgs84})
        # Edges are directed, one per travel direction (as the OSM edges are),
        # so emit the reverse too or the sidewalk is one-way to a router.
        rows.append({
            **props,
            "_id": edge_id(v_lon, v_lat, u_lon, u_lat,
                           f"sidewalk|{c.x:.6f},{c.y:.6f}", "nyc_planimetric_sidewalks"),
            "_u_id": vid,
            "_v_id": uid,
            "geometry": LineString(coords[::-1]),
        })
        n_centerline_ok += 1

    click.echo(f"    Planimetric gap-fill: {n_centerline_ok} new sidewalk edges "
               f"(skipped {n_skipped_covered} covered, {n_skipped_duplicate} on an OSM "
               f"crossing or footway, {n_centerline_fail} centerline failures)")

    if not rows:
        return gpd.GeoDataFrame(columns=["_id", "_u_id", "_v_id", "geometry"],
                                geometry="geometry", crs="EPSG:4326")

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


# ---------------------------------------------------------------------------
# Sidewalk width from planimetric polygons
# ---------------------------------------------------------------------------

def _join_widths_from_planimetric(sidewalks: gpd.GeoDataFrame,
                                   plan_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assign width (metres) to OSM sidewalk edges via planimetric polygon lookup.

    For each sidewalk edge centroid, find the containing planimetric polygon and
    compute its width using the Cauchy mean width formula: 2*area/perimeter.
    Only assigns width when the centroid falls inside a polygon.
    """
    if len(plan_gdf) == 0 or len(sidewalks) == 0:
        return sidewalks

    plan_proj = plan_gdf.to_crs("EPSG:32618").copy()
    def _poly_width(p) -> float | None:
        if p is None or p.is_empty:
            return None
        if p.geom_type not in ("Polygon", "MultiPolygon"):
            return None
        # .length counts interior rings too: many planimetric sidewalks are
        # rings around a block, and the outer ring alone doubles the width.
        perim = p.length
        return round(2.0 * p.area / perim, 2) if perim > 0 else None

    plan_proj["_width_m"] = plan_proj.geometry.apply(_poly_width)

    sw_proj = sidewalks.to_crs("EPSG:32618").copy()
    sw_proj["_orig_index"] = sw_proj.index
    sw_centroids = sw_proj.copy()
    sw_centroids["geometry"] = sw_proj.geometry.centroid

    joined = gpd.sjoin(
        sw_centroids[["_orig_index", "geometry"]],
        plan_proj[["geometry", "_width_m"]],
        how="left",
        predicate="within",
    )
    # Take first planimetric match per edge (edges can span polygon boundaries).
    width_map = joined.groupby("_orig_index")["_width_m"].first()

    sidewalks = sidewalks.copy()
    plan_widths = pd.Series(sidewalks.index.map(width_map), index=sidewalks.index)
    if "width" in sidewalks.columns:
        # OSM-surveyed widths win; planimetric only fills the gaps.
        sidewalks["width"] = sidewalks["width"].fillna(plan_widths)
    else:
        sidewalks["width"] = plan_widths
    n_with_width = sidewalks["width"].notna().sum()
    click.echo(f"    Width assigned to {n_with_width:,}/{len(sidewalks):,} OSM sidewalk edges")
    return sidewalks


# ---------------------------------------------------------------------------
# Borough boundaries → region MultiPolygon + per-feature ext:borough
# ---------------------------------------------------------------------------

def _build_region_polygon(boroughs_gdf: gpd.GeoDataFrame) -> dict:
    """Build the OSW root metadata region as a GeoJSON MultiPolygon."""
    union = unary_union(boroughs_gdf.geometry)
    if union.geom_type == "Polygon":
        union = MultiPolygon([union])
    return mapping(union)


def _tag_borough(gdf: gpd.GeoDataFrame,
                 boroughs_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Spatial join to add ext:borough to features that lack it."""
    if "ext:borough" in gdf.columns:
        # Already tagged (e.g., from OSMnx per-borough extraction).
        return gdf

    boroughs_proj = boroughs_gdf[["boro_name", "geometry"]].copy()
    boroughs_proj = boroughs_proj.rename(columns={"boro_name": "ext:borough"})

    # Use centroid for join to handle edge cases where geometry spans boroughs.
    # Project to a metric CRS for accurate centroid computation.
    gdf_centroids = gdf.copy()
    gdf_centroids["geometry"] = gdf.to_crs("EPSG:32618").geometry.centroid.to_crs("EPSG:4326")

    joined = gpd.sjoin(
        gdf_centroids[["geometry"]].reset_index(),
        boroughs_proj,
        how="left",
        predicate="within",
    )
    # A centroid inside two overlapping borough polygons matches twice; keep
    # the first so the result lines up with gdf.
    joined = joined[~joined.index.duplicated(keep="first")]
    gdf["ext:borough"] = joined["ext:borough"].values
    return gdf


# ---------------------------------------------------------------------------
# MTA ADA station → ext:ada_accessible annotation
# ---------------------------------------------------------------------------

def _build_ada_index(mta_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Return the MTA ADA station GDF in WGS-84 for downstream annotation."""
    return mta_gdf[mta_gdf.geometry.notna()].copy()


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def run(sources: dict, build_cfg: dict, repo_root: Path) -> None:
    """Stage 3: map cleaned source data to OSW-conformant features."""
    clean_dir  = repo_root / build_cfg["dirs"]["clean"]
    staged_dir = repo_root / build_cfg["dirs"]["staged"]
    staged_dir.mkdir(parents=True, exist_ok=True)

    raw_dir   = repo_root / build_cfg["dirs"]["raw"]
    manifest  = load_manifest(raw_dir)
    pipeline_version = build_cfg.get("pipeline_version", "unknown")

    # Load cleaned sources.
    click.echo("  Loading cleaned sources...")
    boroughs_gdf = gpd.read_file(clean_dir / "nyc_boroughs.geojson")
    osm_edges    = gpd.read_file(clean_dir / "osm_walk.geojson")
    ramps_gdf    = gpd.read_file(clean_dir / "nyc_dot_ramps.geojson")
    plan_gdf     = gpd.read_file(clean_dir / "nyc_planimetric_sidewalks.geojson")

    mta_file = clean_dir / "mta_ada_stations.geojson"
    mta_gdf  = gpd.read_file(mta_file) if mta_file.exists() else None

    click.echo(f"    OSM edges: {len(osm_edges)}")
    click.echo(f"    DOT ramps: {len(ramps_gdf)}")
    click.echo(f"    Planimetric polygons: {len(plan_gdf)}")
    click.echo(f"    MTA stations: {len(mta_gdf) if mta_gdf is not None else 'N/A'}")

    # --- Transform 1: OSM edges → OSW edge types ---
    click.echo("\n  Mapping OSM edges to OSW schema...")
    sidewalks, crossings, footways, streets = _osm_edges_to_osw(
        osm_edges, pipeline_version, manifest
    )

    # --- Transform 1b: Assign width to OSM sidewalks from planimetric polygons ---
    click.echo("\n  Assigning sidewalk widths from planimetric polygons...")
    sidewalks = _join_widths_from_planimetric(sidewalks, plan_gdf)

    # --- Transform 2: DOT ramps → CurbRamp nodes ---
    click.echo("\n  Mapping DOT ramps to CurbRamp nodes...")
    curb_nodes = _ramps_to_curb_nodes(ramps_gdf, pipeline_version, manifest)

    # --- Transform 3: Planimetric → gap-fill sidewalk edges ---
    click.echo("\n  Deriving gap-fill sidewalk edges from planimetric polygons...")
    plan_sidewalks = _planimetric_to_sidewalk_edges(
        plan_gdf, sidewalks, build_cfg, pipeline_version, manifest,
        osm_pedestrian_gdf=pd.concat(
            [g[["geometry"]] for g in (sidewalks, crossings, footways) if len(g) > 0]),
    )

    # Gap-fill edges have no OSMnx borough tag.
    if len(plan_sidewalks) > 0:
        _tag_borough(plan_sidewalks, boroughs_gdf)

    # The gap-fill centrelines stay out of the graph. In a sample checked over
    # orthoimagery half of them lay on a sidewalk and the rest on driveways,
    # lots and yards, and almost none touches the network. Stage 6 ships them
    # as a sidecar file for whoever wants to check them.
    all_sidewalks = sidewalks
    click.echo(f"\n  Sidewalk edges: {len(sidewalks)} from OSM; "
               f"{len(plan_sidewalks)} planimetric gap-fill edges kept apart")

    # --- Transform 4: Borough tags ---
    click.echo("\n  Tagging features with ext:borough...")
    for gdf in [crossings, footways, streets, curb_nodes]:
        _tag_borough(gdf, boroughs_gdf)

    # --- Save staged feature files ---
    outputs = {
        "sidewalks":   all_sidewalks,
        "crossings":   crossings,
        "footways":    footways,
        "streets":     streets,
        "curb_nodes":  curb_nodes,
        "gapfill_sidewalks": plan_sidewalks,
    }

    # Sources tag boroughs three ways (OSMnx region slugs, DOT display names,
    # boro_name from the boundaries file); normalize all of them to the
    # two-letter codes SCHEMA.md documents.
    for gdf in outputs.values():
        if "ext:borough" in gdf.columns:
            gdf["ext:borough"] = gdf["ext:borough"].map(borough_code)

    click.echo()
    for name, gdf in outputs.items():
        out_path = staged_dir / f"{name}.geojson"
        if len(gdf) == 0 and name == "gapfill_sidewalks":
            out_path.unlink(missing_ok=True)   # or Stage 6 ships a stale one
            continue
        gdf.to_file(out_path, driver="GeoJSON")
        click.echo(f"  Staged {name}: {len(gdf)} features → {out_path.name}")

    # Save the region MultiPolygon for use in assemble + export.
    region = _build_region_polygon(boroughs_gdf)
    region_path = staged_dir / "region.json"
    region_path.write_text(json.dumps(region, indent=2))
    click.echo(f"  Region MultiPolygon → {region_path.name}")

    # Save MTA ADA index if available.
    if mta_gdf is not None:
        ada_index = _build_ada_index(mta_gdf)
        ada_path  = staged_dir / "mta_ada_stations.geojson"
        ada_index.to_file(ada_path, driver="GeoJSON")
        click.echo(f"  MTA ADA index: {len(ada_index)} stations → {ada_path.name}")
