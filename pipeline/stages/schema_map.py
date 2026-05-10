"""Stage 3: Map cleaned source data to OpenSidewalks v0.3 schema features.

Input:  data/clean/{source_id}.geojson
Output: data/staged/{feature_type}.geojson
        METHODOLOGY.md (updated in-place)

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

CROSSING_MARKINGS_ENUM = frozenset([
    "zebra", "zebra:double", "zebra:paired", "zebra:bicolour",
    "lines", "lines:paired", "lines:rainbow",
    "dashes", "dots",
    "ladder", "ladder:paired", "ladder:skewed",
    "pictograms", "rainbow", "surface", "yes", "no",
])

# OSM highway tags that map to OSW footway/sidewalk edges.
FOOTWAY_TYPES = frozenset(["footway", "path", "pedestrian", "steps"])

# OSM highway tags that map to OSW street edges (not sidewalk-class).
# cycleway and track are excluded: cycleway has no OSW schema definition and is
# not pedestrian infrastructure; track is rural/unpaved and not relevant to NYC.
STREET_TYPES = frozenset([
    "residential", "service", "tertiary", "secondary", "primary",
    "living_street", "unclassified",
])


# ---------------------------------------------------------------------------
# Helper: polygon centerline via Voronoi skeleton
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
    default useful_tags_way. When those columns are absent, all highway=footway
    edges are classified as generic 'footway' rather than 'sidewalk'/'crossing'.
    The acquire stage now configures OSMnx to preserve these tags on future runs.
    For the current dataset, planimetric gap-fill provides proper sidewalk edges.
    """
    highway = str(row.get("highway", "")).lower().split("|")[0].strip()

    # footway sub-tag: may be absent in older downloads (see note above).
    footway_raw = row.get("footway")
    footway = "" if footway_raw is None or str(footway_raw) in ("nan", "None", "") else str(footway_raw).lower().strip()

    # crossing tag: alternate indicator that an edge is a road crossing.
    crossing_raw = row.get("crossing")
    has_crossing = crossing_raw is not None and str(crossing_raw) not in ("nan", "None", "no", "")

    if highway in FOOTWAY_TYPES:
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

        # Borough annotation.
        if "borough" in ramp:
            props["ext:borough"] = str(ramp["borough"])

        # Tactile paving: DOT dataset doesn't have a direct flag, but we can
        # check dws_conditions (detectable warning strip condition) as a proxy.
        dws = ramp.get("dws_conditions")
        if dws and str(dws).strip().lower() not in ("nan", "none", ""):
            # Any non-empty dws_conditions means a DWS was surveyed.
            props["tactile_paving"] = "yes"

        # Preserve key ramp identifiers as ext: fields for auditability.
        for orig_key, ext_key in [("rampid", "ext:ramp_id"),
                                   ("cornerid", "ext:corner_id"),
                                   ("stname1", "ext:street_1"),
                                   ("stname2", "ext:street_2")]:
            val = ramp.get(orig_key)
            if val and str(val) not in ("nan", "None", ""):
                props[ext_key] = str(val)

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
) -> gpd.GeoDataFrame:
    """Derive sidewalk edges from planimetric polygons where OSM coverage is sparse.

    Coverage check: if a planimetric polygon has an OSM sidewalk edge within
    `planimetric_coverage_threshold_meters`, it's already covered. Skip it.

    For uncovered polygons, extract a centerline using the Voronoi skeleton
    approach and emit it as a Sidewalk Edge.
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

    rows = []
    n_skipped_covered = 0
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
        perimeter = poly.exterior.length if poly.geom_type == "Polygon" else sum(
            part.exterior.length for part in poly.geoms
        )
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
        n_centerline_ok += 1

    click.echo(f"    Planimetric gap-fill: {n_centerline_ok} new sidewalk edges "
               f"(skipped {n_skipped_covered} covered, {n_centerline_fail} centerline failures)")

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
    if "width" in sidewalks.columns:
        # Already populated (e.g., gap-fill edges already have width).
        pass

    plan_proj = plan_gdf.to_crs("EPSG:32618").copy()
    def _poly_width(p) -> float | None:
        if p is None or p.is_empty:
            return None
        if p.geom_type == "Polygon":
            perim = p.exterior.length
        elif p.geom_type == "MultiPolygon":
            perim = sum(part.exterior.length for part in p.geoms)
        else:
            return None
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
    sidewalks["width"] = sidewalks.index.map(width_map)
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
        plan_gdf, sidewalks, build_cfg, pipeline_version, manifest
    )

    # Merge planimetric gap-fills into sidewalks layer.
    all_sidewalks = gpd.GeoDataFrame(
        pd.concat([sidewalks, plan_sidewalks], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )
    click.echo(f"\n  Total sidewalk edges: {len(all_sidewalks)} "
               f"(OSM: {len(sidewalks)}, planimetric gap-fill: {len(plan_sidewalks)})")

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
    }

    click.echo()
    for name, gdf in outputs.items():
        out_path = staged_dir / f"{name}.geojson"
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
