"""Stage 4: Assemble the final OSW-conformant FeatureCollection.

Input:  data/staged/{feature_type}.geojson
Output: data/staged/nyc-osw-unvalidated.geojson
        data/staged/topology_report.md

Operations:
  1. Snap CurbRamp nodes to the nearest edge endpoint within snap_tolerance_meters.
     Per OSW spec: sidewalks and crossings connect via Curb nodes, never directly.
  2. Assign _u_id/_v_id to every Edge referencing a Node _id.
  3. Ensure every Node that is referenced as _u_id or _v_id actually exists in the
     nodes collection (inject bare nodes at dangling coordinates if needed).
  4. Compute connected components, report fragmentation.
  5. Write a single canonical FeatureCollection.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import click
import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from shapely.geometry import Point, mapping
from shapely.ops import nearest_points

from pipeline.utils.ids import node_id
from pipeline.utils.provenance import get_git_sha, load_manifest


# ---------------------------------------------------------------------------
# Snapping helpers
# ---------------------------------------------------------------------------

def _endpoint_coords(gdf: gpd.GeoDataFrame) -> list[tuple[float, float, str, str]]:
    """Return (lon, lat, edge_id, endpoint_role) for all edge endpoints.

    Uses vectorized shapely operations for speed on city-scale datasets.
    """
    valid = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    if valid.empty:
        return []

    from shapely import get_coordinates
    pts = []
    for _, row in valid.iterrows():
        coords = list(row.geometry.coords)
        eid    = row["_id"]
        pts.append((coords[0][0],  coords[0][1],  eid, "u"))
        pts.append((coords[-1][0], coords[-1][1], eid, "v"))
    return pts


def _snap_curb_nodes(curb_nodes: gpd.GeoDataFrame,
                     edge_endpoints: list[tuple],
                     snap_tolerance_m: float) -> gpd.GeoDataFrame:
    """Snap curb nodes to the nearest edge endpoint within snap_tolerance_m.

    Uses a scipy cKDTree for O(n log m) nearest-neighbor lookup instead of
    the O(n×m) brute-force approach, making this tractable for city-scale data.

    CRS note: distances are computed in EPSG:32618 (UTM Zone 18N, metres).
    """
    if curb_nodes.empty or not edge_endpoints:
        return curb_nodes

    from pyproj import Transformer
    from scipy.spatial import cKDTree

    to_proj  = Transformer.from_crs("EPSG:4326", "EPSG:32618", always_xy=True)
    to_wgs84 = Transformer.from_crs("EPSG:32618", "EPSG:4326", always_xy=True)

    click.echo(f"    Building KD-tree over {len(edge_endpoints):,} edge endpoints...")
    ep_proj = np.array([
        to_proj.transform(lon, lat)
        for lon, lat, _, _ in edge_endpoints
    ])
    tree = cKDTree(ep_proj)

    # Project curb node coordinates.
    curb_coords = np.array([
        to_proj.transform(row.geometry.x, row.geometry.y)
        for _, row in curb_nodes.iterrows()
    ])

    # Query KD-tree: nearest endpoint for every curb node.
    dists, indices = tree.query(curb_coords, k=1, workers=-1)

    snapped_geometries = []
    snapped_ids        = []
    snap_count         = 0

    for i, (_, curb) in enumerate(curb_nodes.iterrows()):
        min_dist = float(dists[i])
        if min_dist <= snap_tolerance_m:
            ex, ey = ep_proj[indices[i]]
            new_lon, new_lat = to_wgs84.transform(ex, ey)
            snapped_geometries.append(Point(new_lon, new_lat))
            snapped_ids.append(node_id(new_lon, new_lat))
            snap_count += 1
        else:
            snapped_geometries.append(curb.geometry)
            snapped_ids.append(curb["_id"])

    result = curb_nodes.copy()
    result.geometry = snapped_geometries
    result["_id"]   = snapped_ids

    click.echo(f"    Snapped {snap_count}/{len(curb_nodes)} curb nodes "
               f"(tolerance {snap_tolerance_m} m)")
    return result


# ---------------------------------------------------------------------------
# Near-coincident cross-source endpoint merge
# ---------------------------------------------------------------------------

def _merge_near_endpoints(all_edges: gpd.GeoDataFrame,
                           tolerance_m: float = 2.0) -> gpd.GeoDataFrame:
    """Unify _u_id/_v_id for edge endpoints that are geographically near but
    not identical across sources.

    Even with source-independent node IDs, OSM and planimetric-derived edges
    that share a geographic endpoint may differ by 1-3 m due to survey precision.
    This pass clusters endpoints within tolerance_m and remaps all references to
    a single canonical ID per cluster, ensuring the two subgraphs are joined.
    """
    from pyproj import Transformer
    from scipy.spatial import cKDTree

    if all_edges.empty or "_u_id" not in all_edges.columns:
        return all_edges

    # Build id -> (lon, lat) from edge endpoint geometry. Deduplicated.
    u_frame = all_edges[["_u_id"]].copy()
    u_frame["lon"] = [g.coords[0][0] if g and not g.is_empty else None
                      for g in all_edges.geometry]
    u_frame["lat"] = [g.coords[0][1] if g and not g.is_empty else None
                      for g in all_edges.geometry]
    u_frame = u_frame.rename(columns={"_u_id": "nid"})

    v_frame = all_edges[["_v_id"]].copy()
    v_frame["lon"] = [g.coords[-1][0] if g and not g.is_empty else None
                      for g in all_edges.geometry]
    v_frame["lat"] = [g.coords[-1][1] if g and not g.is_empty else None
                      for g in all_edges.geometry]
    v_frame = v_frame.rename(columns={"_v_id": "nid"})

    endpoints = (
        pd.concat([u_frame, v_frame], ignore_index=True)
        .dropna(subset=["nid", "lon", "lat"])
        .drop_duplicates(subset="nid")
        .reset_index(drop=True)
    )

    if len(endpoints) == 0:
        return all_edges

    to_proj = Transformer.from_crs("EPSG:4326", "EPSG:32618", always_xy=True)
    xy = np.array([
        to_proj.transform(row.lon, row.lat)
        for row in endpoints.itertuples()
    ])

    tree  = cKDTree(xy)
    pairs = tree.query_pairs(tolerance_m)

    if not pairs:
        click.echo("    No near-coincident cross-source endpoints found")
        return all_edges

    ids_list = endpoints["nid"].tolist()
    parent   = list(range(len(ids_list)))

    def _find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for i, j in pairs:
        ri, rj = _find(i), _find(j)
        if ri != rj:
            if ids_list[ri] <= ids_list[rj]:
                parent[rj] = ri
            else:
                parent[ri] = rj

    remap = {
        ids_list[i]: ids_list[_find(i)]
        for i in range(len(ids_list))
        if _find(i) != i
    }

    if not remap:
        return all_edges

    click.echo(f"    Merged {len(remap):,} near-coincident cross-source endpoints "
               f"(tolerance {tolerance_m} m)")
    result = all_edges.copy()
    result["_u_id"] = result["_u_id"].map(lambda x: remap.get(x, x) if pd.notna(x) else x)
    result["_v_id"] = result["_v_id"].map(lambda x: remap.get(x, x) if pd.notna(x) else x)
    return result


# ---------------------------------------------------------------------------
# Node injection for dangling edge endpoints
# ---------------------------------------------------------------------------

def _inject_missing_nodes(all_edges: gpd.GeoDataFrame,
                           existing_nodes: gpd.GeoDataFrame,
                           pipeline_version: str) -> gpd.GeoDataFrame:
    """Inject bare Point Nodes for any edge endpoint not yet in the nodes set.

    OSW requires every _u_id and _v_id to reference a Node feature with that _id.
    OSM nodes cover OSM edge endpoints; planimetric-derived edges may reference
    positions with no existing node. Uses vectorized operations for speed.
    """
    existing_ids = set(existing_nodes["_id"].values) if len(existing_nodes) > 0 else set()

    # Vectorized extraction: get all unique _u_id / _v_id not in existing nodes.
    u_ids = all_edges["_u_id"].dropna().unique() if "_u_id" in all_edges.columns else []
    v_ids = all_edges["_v_id"].dropna().unique() if "_v_id" in all_edges.columns else []
    all_ref_ids = set(u_ids) | set(v_ids)
    missing_ids = all_ref_ids - existing_ids

    if not missing_ids:
        return existing_nodes

    # Build id → endpoint coordinate map. We only iterate edges where at least
    # one endpoint is missing. Much smaller than all_edges for OSM-dominated data.
    needs_u = all_edges["_u_id"].isin(missing_ids) if "_u_id" in all_edges.columns else pd.Series(False, index=all_edges.index)
    needs_v = all_edges["_v_id"].isin(missing_ids) if "_v_id" in all_edges.columns else pd.Series(False, index=all_edges.index)
    candidate_edges = all_edges[needs_u | needs_v]

    coord_map = {}
    for _, edge in candidate_edges.iterrows():
        geom   = edge.geometry
        if geom is None or geom.is_empty:
            continue
        coords = list(geom.coords)
        src    = edge.get("ext:source", "unknown")

        uid = edge.get("_u_id")
        if uid in missing_ids and uid not in coord_map:
            coord_map[uid] = (coords[0][0], coords[0][1], src)

        vid = edge.get("_v_id")
        if vid in missing_ids and vid not in coord_map:
            coord_map[vid] = (coords[-1][0], coords[-1][1], src)

    now_iso = datetime.now(timezone.utc).isoformat()
    new_rows = [
        {
            "_id":                  nid,
            "ext:source":           src,
            "ext:source_timestamp": now_iso,
            "ext:pipeline_version": pipeline_version,
            "geometry":             Point(lon, lat),
        }
        for nid, (lon, lat, src) in coord_map.items()
    ]

    injected_gdf = gpd.GeoDataFrame(new_rows, geometry="geometry", crs="EPSG:4326")
    click.echo(f"    Injected {len(injected_gdf):,} bare nodes for dangling edge endpoints")

    return gpd.GeoDataFrame(
        pd.concat([existing_nodes, injected_gdf], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )


# ---------------------------------------------------------------------------
# Connected components analysis
# ---------------------------------------------------------------------------

def _topology_report(all_edges: gpd.GeoDataFrame, all_nodes: gpd.GeoDataFrame,
                     staged_dir: Path, min_component_size: int) -> None:
    """Build a NetworkX graph and report connectivity statistics."""
    G = nx.Graph()

    for _, edge in all_edges.iterrows():
        u = edge.get("_u_id")
        v = edge.get("_v_id")
        if u and v:
            G.add_edge(u, v, edge_id=edge["_id"],
                       feature_type=edge.get("footway", edge.get("highway", "unknown")))

    n_nodes     = G.number_of_nodes()
    n_edges     = G.number_of_edges()
    components  = list(nx.connected_components(G))
    n_components = len(components)

    component_sizes = sorted([len(c) for c in components], reverse=True)
    large = [s for s in component_sizes if s >= min_component_size]
    small = [s for s in component_sizes if s < min_component_size]

    largest_pct = (component_sizes[0] / n_nodes * 100) if n_nodes else 0

    lines = [
        "# Topology Report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Graph Statistics",
        "",
        f"- Nodes in graph: {n_nodes:,}",
        f"- Edges in graph: {n_edges:,}",
        f"- Connected components: {n_components:,}",
        f"- Nodes in largest component: {component_sizes[0] if component_sizes else 0:,} "
        f"({largest_pct:.1f}%)",
        f"- Components ≥ {min_component_size} nodes: {len(large)}",
        f"- Isolated/tiny components (< {min_component_size} nodes): {len(small)}",
        "",
        "## Component Size Distribution",
        "",
        "| Rank | Component size |",
        "|------|---------------|",
    ]
    for i, sz in enumerate(component_sizes[:20], 1):
        lines.append(f"| {i} | {sz:,} |")
    if len(component_sizes) > 20:
        lines.append(f"| … | ({len(component_sizes) - 20} more) |")

    report_path = staged_dir / "topology_report.md"
    report_path.write_text("\n".join(lines))
    click.echo(f"    Topology: {n_nodes:,} nodes, {n_edges:,} edges, "
               f"{n_components:,} components")
    click.echo(f"    Topology report → {report_path.name}")


# ---------------------------------------------------------------------------
# Incline computation from DEM
# ---------------------------------------------------------------------------

def _compute_edge_inclines(all_edges: gpd.GeoDataFrame,
                            all_nodes: gpd.GeoDataFrame,
                            dem_tiles: list[Path]) -> gpd.GeoDataFrame:
    """Sample USGS 3DEP DEM at node coordinates and add incline to each edge.

    incline = (v_elevation - u_elevation) / edge_length_m
    Positive values indicate uphill travel from _u_id to _v_id.
    Only street-type edges (highway != footway/steps) are excluded. Pedestrian
    edges all get incline so routing can model grade penalties.
    """
    import math
    import numpy as np
    import rasterio
    from pyproj import Transformer

    try:
        node_coords: dict[str, tuple[float, float]] = {}
        for _, row in all_nodes.iterrows():
            nid = row.get("_id")
            if nid and row.geometry is not None:
                node_coords[nid] = (row.geometry.x, row.geometry.y)

        if not node_coords:
            return all_edges

        node_elevs: dict[str, float] = {}
        from pyproj import Transformer as _T

        # Sample across all tiles; first valid (non-nodata) value wins.
        # Tiles may be a single study-area tile or per-borough city-wide tiles.
        for tile in dem_tiles:
            remaining = {nid: node_coords[nid]
                         for nid in node_coords if nid not in node_elevs}
            if not remaining:
                break
            with rasterio.open(tile) as src:
                raster_epsg = src.crs.to_epsg() or 4326
                if raster_epsg != 4326:
                    tr = _T.from_crs(4326, raster_epsg, always_xy=True)
                    project = lambda lon, lat: tr.transform(lon, lat)
                else:
                    project = lambda lon, lat: (lon, lat)

                ids_rem = list(remaining)
                pts = [project(*remaining[nid]) for nid in ids_rem]
                nodata = src.nodata
                for nid, elev_arr in zip(ids_rem, src.sample(pts)):
                    elev = float(elev_arr[0])
                    is_nodata = np.isnan(elev) or (
                        nodata is not None and not np.isnan(nodata) and elev == nodata
                    )
                    if not is_nodata:
                        node_elevs[nid] = elev

        click.echo(f"    Elevations sampled: {len(node_elevs):,}/{len(node_coords):,} nodes")

        def _length_m(geom) -> float:
            coords = list(geom.coords)
            total = 0.0
            for i in range(len(coords) - 1):
                lon1, lat1 = coords[i]
                lon2, lat2 = coords[i + 1]
                dx = (lon2 - lon1) * 111319 * math.cos(math.radians((lat1 + lat2) / 2))
                dy = (lat2 - lat1) * 111319
                total += math.sqrt(dx * dx + dy * dy)
            return total

        inclines = []
        for _, row in all_edges.iterrows():
            uid = row.get("_u_id")
            vid = row.get("_v_id")
            if uid in node_elevs and vid in node_elevs:
                dz = node_elevs[vid] - node_elevs[uid]
                length = _length_m(row.geometry)
                raw = round(dz / length, 4) if length > 0 else None
                # Clamp to OSW schema range [-1.0, 1.0]; values outside are DEM
                # noise on very short edges (steep apparent grade from sub-metre
                # elevation uncertainty), not real walkable grade.
                if raw is not None and abs(raw) > 1.0:
                    raw = None
                inclines.append(raw)
            else:
                inclines.append(None)

        all_edges = all_edges.copy()
        all_edges["incline"] = inclines
        n_with = sum(1 for v in inclines if v is not None)
        click.echo(f"    Incline set on {n_with:,}/{len(all_edges):,} edges")
        return all_edges

    except Exception as exc:
        click.echo(f"  Warning: incline computation failed ({exc}). Skipping.")
        return all_edges


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def run(sources: dict, build_cfg: dict, repo_root: Path) -> None:
    """Stage 4: snap nodes, assign IDs, build canonical FeatureCollection."""
    staged_dir    = repo_root / build_cfg["dirs"]["staged"]
    raw_dir       = repo_root / build_cfg["dirs"]["raw"]
    manifest      = load_manifest(raw_dir)
    pipeline_version = build_cfg.get("pipeline_version", "unknown")
    git_sha       = get_git_sha()

    snap_tolerance = build_cfg.get("snap_tolerance_meters", 5.0)
    min_comp_size  = build_cfg.get("min_component_size_nodes", 3)

    # Load staged feature files.
    click.echo("  Loading staged features...")
    sidewalks  = gpd.read_file(staged_dir / "sidewalks.geojson")
    crossings  = gpd.read_file(staged_dir / "crossings.geojson")
    footways   = gpd.read_file(staged_dir / "footways.geojson")
    streets    = gpd.read_file(staged_dir / "streets.geojson")
    curb_nodes = gpd.read_file(staged_dir / "curb_nodes.geojson")

    osm_nodes_path = repo_root / build_cfg["dirs"]["clean"] / "osm_nodes.geojson"
    osm_nodes = gpd.read_file(osm_nodes_path) if osm_nodes_path.exists() else gpd.GeoDataFrame()

    region_raw = json.loads((staged_dir / "region.json").read_text())

    click.echo(f"    Sidewalks:  {len(sidewalks):,}")
    click.echo(f"    Crossings:  {len(crossings):,}")
    click.echo(f"    Footways:   {len(footways):,}")
    click.echo(f"    Streets:    {len(streets):,}")
    click.echo(f"    Curb nodes: {len(curb_nodes):,}")

    # Combine all edges for the full graph.
    all_edges = gpd.GeoDataFrame(
        pd.concat([sidewalks, crossings, footways, streets], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )

    # Drop degenerate edges (u == v). Zero-length self-loops from OSM or planimetric gaps.
    if "_u_id" in all_edges.columns and "_v_id" in all_edges.columns:
        degen = (all_edges["_u_id"] == all_edges["_v_id"]) & all_edges["_u_id"].notna()
        if degen.any():
            all_edges = all_edges[~degen].copy()
            click.echo(f"    Dropped {degen.sum()} degenerate edges (_u_id == _v_id)")

    # Deduplicate edges by _id. OSMnx downloads borough-boundary edges twice
    # (once per adjacent borough query), producing identical features with same _id.
    if "_id" in all_edges.columns:
        before = len(all_edges)
        all_edges = all_edges.drop_duplicates(subset="_id", keep="first").copy()
        dropped = before - len(all_edges)
        if dropped:
            click.echo(f"    Deduplicated {dropped} duplicate edges (borough boundary overlap)")

    # Snap curb nodes to edge endpoints.
    # Use only pedestrian edges (sidewalks + footways) for snapping. Curb ramps
    # sit where sidewalks meet road crossings, not on street centerlines.
    click.echo("\n  Snapping curb nodes to edge endpoints...")
    pedestrian_edges = gpd.GeoDataFrame(
        pd.concat([sidewalks, crossings, footways], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )
    click.echo(f"    {len(pedestrian_edges):,} pedestrian edge endpoints to index")
    endpoints  = _endpoint_coords(pedestrian_edges)
    curb_nodes = _snap_curb_nodes(curb_nodes, endpoints, snap_tolerance)

    # Merge near-coincident endpoints across sources. OSM and planimetric edges
    # that share a geographic endpoint within survey tolerance get the same node ID,
    # ensuring the two subgraphs are topologically joined.
    click.echo("\n  Merging near-coincident cross-source endpoints...")
    merge_tolerance = build_cfg.get("endpoint_merge_tolerance_meters", 2.0)
    all_edges = _merge_near_endpoints(all_edges, tolerance_m=merge_tolerance)

    # Prepare OSM nodes with _id field.
    if len(osm_nodes) > 0:
        # OSMnx node IDs are in the osmid column or index.
        if "_id" not in osm_nodes.columns:
            osm_nodes["_id"] = osm_nodes.apply(
                lambda r: node_id(r.geometry.x, r.geometry.y),
                axis=1
            )

        if "ext:source" not in osm_nodes.columns:
            osm_nodes["ext:source"] = "osm_walk"
        if "ext:pipeline_version" not in osm_nodes.columns:
            osm_nodes["ext:pipeline_version"] = pipeline_version

        # Strip all non-OSW properties. OSMnx attaches osmid, oneway, reversed,
        # length, junction, ref, etc.. These fail the OSW additionalProperties:false
        # constraint. Node features only carry _id, barrier/kerb/tactile_paving if
        # relevant, and ext:* provenance fields.
        _osw_node_fields = {"_id", "barrier", "kerb", "tactile_paving"}
        cols_to_keep = [
            c for c in osm_nodes.columns
            if c in _osw_node_fields or str(c).startswith("ext:") or c == osm_nodes.geometry.name
        ]
        osm_nodes = osm_nodes[cols_to_keep].copy()

    # Combine all nodes.
    all_node_gdfs = [g for g in [osm_nodes, curb_nodes] if len(g) > 0]
    if all_node_gdfs:
        all_nodes = gpd.GeoDataFrame(
            pd.concat(all_node_gdfs, ignore_index=True),
            geometry="geometry", crs="EPSG:4326"
        )
    else:
        all_nodes = gpd.GeoDataFrame(geometry=gpd.GeoSeries([], crs="EPSG:4326"))

    # Inject bare nodes for any dangling edge endpoints.
    click.echo("\n  Ensuring all edge endpoints have corresponding Node features...")
    all_nodes = _inject_missing_nodes(all_edges, all_nodes, pipeline_version)

    # Deduplicate nodes by _id, merging properties so CurbRamp annotations
    # (barrier, kerb, tactile_paving) survive even when the same location also
    # appears as a generic OSM node. Without this, drop_duplicates(keep="first")
    # silently discards DOT ramp data whenever an OSM node lands at the same point.
    if "_id" in all_nodes.columns:
        before = len(all_nodes)
        curb_fields = {"barrier", "kerb", "tactile_paving"}

        def _merge_node_group(group: pd.DataFrame) -> pd.Series:
            # Start from the first row, then fill in curb fields from any row
            # that has them (curb_nodes rows carry barrier/kerb/tactile_paving).
            merged = group.iloc[0].copy()
            for field in curb_fields:
                if field in group.columns:
                    filled = group[field].dropna()
                    if not filled.empty:
                        merged[field] = filled.iloc[0]
            return merged

        all_nodes = (
            all_nodes
            .groupby("_id", sort=False)
            .apply(_merge_node_group)
            .reset_index(drop=True)
        )
        # Restore GeoDataFrame with correct geometry column.
        all_nodes = gpd.GeoDataFrame(all_nodes, geometry="geometry", crs="EPSG:4326")
        dropped = before - len(all_nodes)
        n_curb = (all_nodes["kerb"].notna().sum()
                  if "kerb" in all_nodes.columns else 0)
        click.echo(f"    Deduplicated {dropped} duplicate nodes "
                   f"({n_curb} CurbRamp nodes preserved)")

    click.echo(f"\n  Final counts: {len(all_nodes):,} nodes, {len(all_edges):,} edges")

    # Compute incline from LiDAR DEM tiles if available.
    # Study area: single dem.tif; city-wide: per-borough dem_{boro}.tif tiles.
    dem_dir = raw_dir / "dem_nyc"
    dem_tiles = sorted(dem_dir.glob("dem*.tif")) if dem_dir.exists() else []
    # Filter out any corrupt tiles (< 1 KB = error response saved as file).
    dem_tiles = [p for p in dem_tiles if p.stat().st_size > 1024]
    if dem_tiles:
        click.echo(f"\n  Computing edge inclines from {len(dem_tiles)} LiDAR tile(s)...")
        all_edges = _compute_edge_inclines(all_edges, all_nodes, dem_tiles)
    else:
        click.echo("\n  LiDAR DEM not found. Skipping incline (run Stage 1 to acquire)")

    # Topology report: analyze pedestrian graph connectivity (exclude street edges
    # which are not part of the pedestrian routing graph).
    click.echo("\n  Computing connected components (pedestrian edges only)...")
    pedestrian_for_topo = gpd.GeoDataFrame(
        pd.concat([sidewalks, crossings, footways], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )
    _topology_report(pedestrian_for_topo, all_nodes, staged_dir, min_comp_size)

    # Build the canonical OSW FeatureCollection.
    # Features: all nodes first, then all edges (OSW convention).
    click.echo("\n  Assembling canonical FeatureCollection...")

    def _gdf_to_features(gdf: gpd.GeoDataFrame) -> list[dict]:
        """Convert GeoDataFrame to GeoJSON Feature dicts using vectorized geometry mapping."""
        valid = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
        if valid.empty:
            return []
        geom_col = valid.geometry.name
        prop_cols = [c for c in valid.columns if c != geom_col]
        features = []
        for i in range(len(valid)):
            row   = valid.iloc[i]
            props = {}
            for col in prop_cols:
                # OSW schema has additionalProperties:false on every feature type
                # with no extension mechanism. Ext:* provenance fields are captured
                # at the root FeatureCollection level (dataSource, pipelineVersion).
                if str(col).startswith("ext:"):
                    continue
                v = row[col]
                if v is None:
                    continue
                # Skip NaN, NaT, and string representations of missing values.
                try:
                    import pandas as _pd
                    if _pd.isna(v):
                        continue
                except (TypeError, ValueError):
                    pass
                sv = str(v)
                if sv not in ("nan", "NaN", "None", "<NA>", "NaT"):
                    # Convert numpy scalar types to Python native for JSON compat.
                    import numpy as _np
                    import pandas as _pd2
                    if isinstance(v, _np.integer):
                        v = int(v)
                    elif isinstance(v, _np.floating):
                        v = float(v)
                    elif isinstance(v, _np.bool_):
                        v = bool(v)
                    elif isinstance(v, (_pd2.Timestamp,)):
                        v = v.isoformat()
                    props[col] = v
            if props.get("_id") is None:
                continue
            features.append({
                "type": "Feature",
                "geometry": mapping(row.geometry),
                "properties": props,
            })
        return features

    node_features = _gdf_to_features(all_nodes)
    edge_features = _gdf_to_features(all_edges)
    all_features  = node_features + edge_features

    # Root OSW metadata.
    now_iso = datetime.now(timezone.utc).isoformat()
    fc = {
        # Must match the CompatibleSchemaURI enum in the OSW schema exactly.
        # The GitHub raw URL is used to *fetch* the schema; this field must use
        # the canonical sidewalks.washington.edu URI the enum validates against.
        "$schema": (
            f"https://sidewalks.washington.edu/opensidewalks/"
            f"{build_cfg.get('osw_schema_version', '0.3')}/schema.json"
        ),
        "type": "FeatureCollection",
        "dataSource": {
            "name": "opensidewalks-nyc pipeline",
            "url": "https://github.com/msradam/opensidewalks-nyc",
        },
        "dataTimestamp": now_iso,
        "pipelineVersion": {
            "version": pipeline_version,
            "gitSHA": git_sha,
            "builtAt": now_iso,
        },
        "region": region_raw,
        "features": all_features,
    }

    out_path = staged_dir / "nyc-osw-unvalidated.geojson"

    class _SafeEncoder(json.JSONEncoder):
        """Encode numpy/pandas types that json.dumps can't handle natively."""
        def default(self, obj):
            import numpy as _np
            import pandas as _pd
            if isinstance(obj, _np.integer):
                return int(obj)
            if isinstance(obj, _np.floating):
                return float(obj)
            if isinstance(obj, _np.bool_):
                return bool(obj)
            if isinstance(obj, _pd.Timestamp):
                return obj.isoformat()
            if hasattr(obj, 'item'):
                return obj.item()
            return super().default(obj)

    out_path.write_text(json.dumps(fc, indent=2, cls=_SafeEncoder))

    size_mb = out_path.stat().st_size / 1_048_576
    click.echo(f"\n  Canonical FeatureCollection: {len(all_features):,} features "
               f"({len(node_features):,} nodes, {len(edge_features):,} edges)")
    click.echo(f"  Written to {out_path} ({size_mb:.1f} MB)")
