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
from shapely.geometry import LineString, Point, mapping

from pipeline.stages.schema_map import borough_code
from pipeline.utils.deck import grade
from pipeline.utils.ids import node_id
from pipeline.utils.provenance import get_git_sha, load_manifest


# ---------------------------------------------------------------------------
# Snapping helpers
# ---------------------------------------------------------------------------

def _endpoint_coords(gdf: gpd.GeoDataFrame) -> list[tuple[float, float, str, str]]:
    """Return (lon, lat, edge_id, endpoint_role) for all edge endpoints."""
    valid = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    if valid.empty:
        return []

    pts = []
    for _, row in valid.iterrows():
        coords = list(row.geometry.coords)
        eid    = row["_id"]
        pts.append((coords[0][0],  coords[0][1],  eid, "u"))
        pts.append((coords[-1][0], coords[-1][1], eid, "v"))
    return pts


def _crossing_ends(crossings: gpd.GeoDataFrame,
                   others: gpd.GeoDataFrame) -> list[tuple[float, float]]:
    """The coordinates of the nodes where a crossing meets the rest of the
    pedestrian network: an end of a crossing edge that is also an end of a
    sidewalk, footway or zone ring edge. A crossing's inner vertices (the
    kerb line, the lanes, a median) are not among them."""
    walk = set(others["_u_id"]) | set(others["_v_id"]) if len(others) else set()
    ends = {}
    for lon, lat, _, _ in _endpoint_coords(crossings):
        nid = node_id(lon, lat)
        if nid in walk:
            ends[nid] = (lon, lat)
    return list(ends.values())


def _snap_curb_nodes(curb_nodes: gpd.GeoDataFrame,
                     edge_endpoints: list[tuple],
                     snap_tolerance_m: float,
                     preferred: list[tuple[float, float]] | None = None) -> gpd.GeoDataFrame:
    """Snap curb nodes to the nearest edge endpoint within snap_tolerance_m.

    A ramp serves a crossing, so where one of the `preferred` points (the
    crossing ends, see _crossing_ends) is within the tolerance it goes
    there, nearest ramp first and one ramp per end, even when a sidewalk
    vertex beside the crossing is nearer. The rest take the nearest endpoint
    of any kind, as before v0.3.7, when three in ten attached ramps landed
    on a sidewalk vertex beside the crossing they serve.

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
    target = {i: ep_proj[indices[i]] for i in range(len(curb_nodes)) if dists[i] <= snap_tolerance_m}

    n_at_end = 0
    if preferred:
        pref_proj = np.array([to_proj.transform(lon, lat) for lon, lat in preferred])
        pd_, pi = cKDTree(pref_proj).query(curb_coords, k=1, workers=-1)
        taken: set[int] = set()
        for i in np.argsort(pd_, kind="stable"):
            if pd_[i] > snap_tolerance_m:
                break
            if int(pi[i]) not in taken:
                taken.add(int(pi[i]))
                target[int(i)] = pref_proj[pi[i]]
                n_at_end += 1

    snapped_geometries = []
    snapped_ids        = []
    snap_count         = 0

    for i, (_, curb) in enumerate(curb_nodes.iterrows()):
        if i in target:
            ex, ey = target[i]
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
               f"(tolerance {snap_tolerance_m} m), {n_at_end} of them to a crossing end")
    return result


# ---------------------------------------------------------------------------
# Near-miss endpoint merge
# ---------------------------------------------------------------------------

def _merge_near_endpoints(all_edges: gpd.GeoDataFrame,
                           tolerance_m: float = 2.0,
                           near_factor: float = 5.0) -> tuple[gpd.GeoDataFrame, dict]:
    """Close gaps between endpoints that nearly touch, without chaining.

    A node moves onto another node within tolerance_m only if that closes a
    gap: one of the two is a dead end, or the two are in different connected
    components (of the whole graph, or of the pedestrian graph). A dead end is
    not moved onto a neighbour, or onto a node it already reaches within
    near_factor * tolerance_m. Pairs are taken nearest first; a node that has
    moved is never a target and a target never moves, so no endpoint moves
    more than tolerance_m.

    The earlier version united every pair within tolerance with union-find.
    That chains along closely spaced vertices: on Staten Island it moved
    endpoints up to 33 m and collapsed a quarter of all edges. This version
    joins the same components and leaves the geometry alone.

    Returns the edges and the {merged ID: canonical ID} remap, so callers can
    carry anything else keyed on an endpoint ID (curb nodes) through the merge.
    """
    import heapq

    import shapely
    from pyproj import Transformer
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    from scipy.spatial import cKDTree

    if all_edges.empty or "_u_id" not in all_edges.columns:
        return all_edges, {}

    # One row per endpoint node ID, from the edge geometry.
    geoms = all_edges.geometry.values
    first, last = shapely.get_point(geoms, 0), shapely.get_point(geoms, -1)
    endpoints = pd.DataFrame({
        "nid": np.r_[all_edges["_u_id"].values, all_edges["_v_id"].values],
        "lon": np.r_[shapely.get_x(first), shapely.get_x(last)],
        "lat": np.r_[shapely.get_y(first), shapely.get_y(last)],
    }).dropna().drop_duplicates(subset="nid").reset_index(drop=True)
    if len(endpoints) == 0:
        return all_edges, {}

    ids = endpoints["nid"].values
    xy = np.c_[Transformer.from_crs("EPSG:4326", "EPSG:32618", always_xy=True)
               .transform(endpoints["lon"].values, endpoints["lat"].values)]
    index = {nid: k for k, nid in enumerate(ids)}
    u = all_edges["_u_id"].map(index).values
    v = all_edges["_v_id"].map(index).values
    n = len(ids)

    adj: list[dict[int, float]] = [{} for _ in range(n)]
    for a, b in zip(u, v):
        if a != b:
            d = float(np.hypot(*(xy[a] - xy[b])))
            adj[a][b] = d
            adj[b][a] = d

    def _labels(mask):
        g = coo_matrix((np.ones(int(mask.sum())), (u[mask], v[mask])), shape=(n, n))
        return connected_components(g, directed=False)[1]

    ped = all_edges["highway"].isin(["footway", "pedestrian", "steps"]).values
    whole = _labels(np.ones(len(all_edges), dtype=bool))
    pedc = _labels(ped)
    in_ped = np.zeros(n, dtype=bool)
    in_ped[u[ped]] = True
    in_ped[v[ped]] = True
    parents: dict[str, dict[int, int]] = {"whole": {}, "ped": {}}

    def _find(kind, x):
        par = parents[kind]
        while par.get(x, x) != x:
            par[x] = par.get(par[x], par[x])
            x = par[x]
        return x

    def _near_connected(a, b, cutoff):
        dist = {a: 0.0}
        heap = [(0.0, a)]
        while heap:
            d, x = heapq.heappop(heap)
            if x == b:
                return True
            if d > dist.get(x, np.inf):
                continue
            for y, w in adj[x].items():
                nd = d + w
                if nd <= cutoff and nd < dist.get(y, np.inf):
                    dist[y] = nd
                    heapq.heappush(heap, (nd, y))
        return False

    pairs = cKDTree(xy).query_pairs(tolerance_m, output_type="ndarray")
    if len(pairs) == 0:
        click.echo("    No near-miss endpoints found")
        return all_edges, {}
    gap = np.hypot(*(xy[pairs[:, 0]] - xy[pairs[:, 1]]).T)

    moved: set[int] = set()
    targets: set[int] = set()
    remap: dict[str, str] = {}
    largest = 0.0
    for k in np.argsort(gap, kind="stable"):
        i, j = int(pairs[k, 0]), int(pairs[k, 1])
        diff_whole = _find("whole", whole[i]) != _find("whole", whole[j])
        diff_ped = (in_ped[i] and in_ped[j]
                    and _find("ped", pedc[i]) != _find("ped", pedc[j]))
        if not (diff_whole or diff_ped):
            # Same component both ways: only a dead end may close the gap,
            # and only if the two are not already joined close by.
            if len(adj[i]) != 1 and len(adj[j]) != 1:
                continue
            if j in adj[i] or _near_connected(i, j, near_factor * tolerance_m):
                continue
        # The dead end moves; otherwise the node with fewer neighbours.
        for a, b in sorted([(i, j), (j, i)], key=lambda p: (len(adj[p[0]]), ids[p[0]])):
            if a in moved or a in targets or b in moved:
                continue
            remap[ids[a]] = ids[b]
            largest = max(largest, float(gap[k]))
            moved.add(a)
            targets.add(b)
            for y, w in adj[a].items():
                del adj[y][a]
                if y != b:
                    adj[b][y] = w
                    adj[y][b] = w
            adj[a] = {}
            parents["whole"][_find("whole", whole[a])] = _find("whole", whole[b])
            if in_ped[a] and in_ped[b]:
                parents["ped"][_find("ped", pedc[a])] = _find("ped", pedc[b])
            elif in_ped[a]:
                in_ped[b] = True
                pedc[b] = pedc[a]
            break

    if not remap:
        return all_edges, {}

    click.echo(f"    Merged {len(remap):,} near-miss endpoints "
               f"(tolerance {tolerance_m} m, largest move {largest:.2f} m)")
    result = all_edges.copy()
    result["_u_id"] = result["_u_id"].map(lambda x: remap.get(x, x) if pd.notna(x) else x)
    result["_v_id"] = result["_v_id"].map(lambda x: remap.get(x, x) if pd.notna(x) else x)

    # A street segment shorter than the tolerance, whose two ends were in
    # different pedestrian components, collapses into a 2-point self-loop.
    # Its geometry becomes zero-length (SFA-invalid) once endpoints snap to
    # the node coordinate and it carries no connectivity, so drop it.
    # Self-loops with interior vertices stay valid.
    collapsed = (result["_u_id"] == result["_v_id"]) & result.geometry.apply(
        lambda g: g is not None and not g.is_empty and len(g.coords) == 2
    )
    if collapsed.any():
        click.echo(f"    Dropped {int(collapsed.sum()):,} edges collapsed "
                   f"to zero length by the merge")
        result = result[~collapsed].copy()
    return result, remap


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

# The OSW curb entities: barrier=kerb with one of these kerb values, or with
# none (a generic curb). OSM's other kerb values (yes, no, unknown) say
# nothing the schema can hold and are dropped.
_KERB_VALUES = frozenset(["lowered", "raised", "flush", "rolled"])
_TACTILE_VALUES = frozenset(["yes", "no", "primitive", "contrasted"])


def _osm_node_tags(osm_nodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """The OSM node tags Stage 1 keeps, as the schema's node fields.

    A node tagged barrier=kerb, or kerb=lowered, raised, flush or rolled, is
    a curb node: barrier=kerb and the kerb value (kerb=* alone means a kerb
    is there, so barrier=kerb is written for it). tactile_paving is kept on
    a curb node when its value is one the schema lists. An elevator
    (highway=elevator) is a bare node with ext:osm_highway=elevator; Stage 4
    writes no incline on the edges at it. Every other value of these tags,
    and every other highway value, is dropped.
    """
    nodes = osm_nodes.copy()
    kerb = nodes["kerb"].map(lambda v: str(v).split(";")[0].strip().lower()) if "kerb" in nodes else pd.Series("", index=nodes.index)
    kerb = kerb.where(kerb.isin(_KERB_VALUES))
    barrier = nodes["barrier"].astype(str).str.lower() if "barrier" in nodes else pd.Series("", index=nodes.index)
    is_curb = (barrier == "kerb") | kerb.notna()
    nodes["barrier"] = np.where(is_curb, "kerb", None)
    nodes["kerb"] = kerb
    tactile = nodes["tactile_paving"].astype(str).str.lower() if "tactile_paving" in nodes else pd.Series("", index=nodes.index)
    nodes["tactile_paving"] = tactile.where(is_curb & tactile.isin(_TACTILE_VALUES))
    if "highway" in nodes:
        nodes["ext:osm_highway"] = nodes["highway"].where(nodes["highway"].astype(str).str.lower() == "elevator")
    click.echo(f"    OSM node tags: {int(is_curb.sum()):,} kerbs "
               f"({kerb.value_counts().to_dict()}), "
               f"{int(nodes['tactile_paving'].notna().sum()):,} with tactile_paving, "
               f"{int(nodes['ext:osm_highway'].notna().sum()) if 'ext:osm_highway' in nodes else 0:,} elevators")
    return nodes


def _merge_node_group(group: pd.DataFrame, curb_fields: set) -> pd.Series:
    """One node from the rows that share an id: an OSM vertex and the ramp on it.

    The first row keeps its position (the edge endpoint). The ramp fields
    come from the ramp's row, and a node that carries a ramp says so in its
    provenance: `ext:source` and `ext:source_timestamp` are the survey's,
    because every value on it apart from its position comes from the survey.
    Where the OSM vertex is itself a kerb node, the node carries the
    survey's kerb and tactile_paving (they are measured, and the node's
    provenance says every value is the survey's), and OSM's own values are
    kept beside them as ext:osm_kerb and ext:osm_tactile_paving, whether or
    not they agree. Where the survey has no value, none is written.
    """
    merged = group.iloc[0].copy()
    ramp = group[group["ext:source"] == "nyc_dot_ramps"] if "ext:source" in group.columns else group.iloc[0:0]
    if not ramp.empty:
        for field in ("kerb", "tactile_paving"):
            if field in group.columns and pd.notna(merged.get(field)):
                merged[f"ext:osm_{field}"] = merged[field]
        for field in curb_fields | {"ext:source", "ext:source_timestamp"}:
            if field in ramp.columns:
                merged[field] = ramp.iloc[0][field]
        return merged
    for field in curb_fields:
        if field in group.columns:
            filled = group[field].dropna()
            if not filled.empty:
                merged[field] = filled.iloc[0]
    return merged


CURB_FIELDS = {"barrier", "kerb", "tactile_paving",
               "ext:ramp_id", "ext:corner_id", "ext:street_1", "ext:street_2",
               "ext:running_slope_pct", "ext:cross_slope_pct",
               "ext:counter_slope_pct", "ext:dws_condition"}


def _dedup_nodes(all_nodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """One node per `_id`, merged by _merge_node_group.

    Every merged row must come back with the same keys: pandas builds a
    DataFrame from groupby().apply() only when every Series it returns has
    the same index, and otherwise stacks them into one Series, which loses
    the geometry. The two fields the merge may add are created first.
    """
    for col in ("ext:osm_kerb", "ext:osm_tactile_paving"):
        if col not in all_nodes.columns:
            all_nodes[col] = None
    merged = (
        all_nodes
        .groupby("_id", sort=False)
        .apply(_merge_node_group, CURB_FIELDS)
        # pandas 3 excludes the grouping column from apply() groups, so _id
        # only survives as the group index; a plain reset_index restores it.
        .reset_index()
    )
    return gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")


def _load_zones(path: Path) -> gpd.GeoDataFrame:
    """The staged Pedestrian Zones, with `_w_id` parsed back into a list."""
    if not path.exists():
        return gpd.GeoDataFrame(columns=["_id", "_w_id", "geometry"], geometry="geometry", crs="EPSG:4326")
    zones = gpd.read_file(path)
    zones["_w_id"] = zones["_w_id"].map(lambda w: json.loads(w) if isinstance(w, str) else list(w))
    before = len(zones)
    zones = zones.drop_duplicates(subset="_id", keep="first").copy()
    if len(zones) < before:
        click.echo(f"    Deduplicated {before - len(zones)} duplicate zones")
    return zones


def _ring_rows(zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """One edge row per pair of consecutive ring nodes of every zone.

    They carry `_ring_of`, the zone id, so the caller can drop them again.
    """
    rows = []
    for _, z in zones.iterrows():
        w = z["_w_id"]
        ring = list(z.geometry.exterior.coords)
        for i in range(len(w)):
            a, b = w[i], w[(i + 1) % len(w)]
            row = {"_id": f"ring:{z['_id']}:{i}", "_u_id": a, "_v_id": b, "highway": "pedestrian",
                   "_ring_of": z["_id"], "geometry": LineString([ring[i], ring[(i + 1) % len(w)]])}
            for k in ("ext:structure", "ext:source", "ext:borough", "ext:pipeline_version"):
                if k in z and pd.notna(z[k]):
                    row[k] = z[k]
            rows.append(row)
    if not rows:
        return gpd.GeoDataFrame(columns=["_id", "_u_id", "_v_id", "highway", "_ring_of", "geometry"],
                                geometry="geometry", crs="EPSG:4326")
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


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

# Edges shorter than this share their end heights before incline is taken.
_SMOOTH_M = 5.0
# A height step this large between two nodes that close is a change of level
# (a wall, untagged steps, a deck beside the ground), not noise.
_LEVEL_BREAK_M = 0.5

# LiDAR returns within this distance of a node describe the surface it is on.
# Paths are about 3 m wide and OSM lines sit within a metre or two of them.
_LIDAR_RADIUS_M = 2.0


def _structure_elevations(all_edges: gpd.GeoDataFrame,
                          node_coords: dict[str, tuple[float, float]],
                          node_elevs: dict[str, float],
                          surveys: list[dict], cache_dir: Path
                          ) -> tuple[dict[str, float], dict[str, str], set[str]]:
    """Deck heights for nodes on bridges and elevated ways, from LiDAR returns.

    The terrain model has the ground or water under a deck. Starting from the
    edges OSM tags as a structure, read the returns around each node from the
    first survey that has any (the 2017 one misses spans over open water, the
    2014 one has them), and let pipeline.utils.deck pick the walking surface
    by continuity along the path. A deck often runs on past the tagged part
    (an approach viaduct), so the region grows wherever an untagged node comes
    out on a deck, until every deck has met the ground.

    Returns ({node ID: height} for nodes taken off the terrain model,
    {node ID: where the height came from}, IDs of structure nodes with no
    height at all).
    """
    from pyproj import Transformer
    from scipy.sparse import coo_matrix

    from pipeline.utils.deck import label_surfaces, surface_levels
    from pipeline.utils.ept import EptCloud

    if "ext:structure" not in all_edges.columns:
        return {}, {}, set()
    struct = all_edges["ext:structure"].isin(["bridge", "elevated"]).values
    if not struct.any():
        return {}, {}, set()

    m = len(all_edges)
    inv, ids = pd.factorize(np.r_[all_edges["_u_id"].values, all_edges["_v_id"].values])
    u, v, n = inv[:m], inv[m:], len(ids)
    lonlat = np.array([node_coords.get(i, (np.nan, np.nan)) for i in ids])
    dtm = np.array([node_elevs.get(i, np.nan) for i in ids])
    steps = (all_edges["highway"] == "steps").values
    seed = np.zeros(n, dtype=bool)
    seed[u[struct]] = seed[v[struct]] = True
    nbr = coo_matrix((np.ones(2 * m), (np.r_[u, v], np.r_[v, u])), shape=(n, n)).tocsr()

    def neighbours(nodes):
        return np.unique(np.concatenate(
            [nbr.indices[nbr.indptr[i]:nbr.indptr[i + 1]] for i in nodes] or [[]]).astype(int))

    clouds = [(s["name"], EptCloud(s["url"], cache_dir / s["name"]),
               s.get("surface_classes", [])) for s in surveys]
    xy = np.c_[Transformer.from_crs(4326, int(clouds[0][1].srs["horizontal"]), always_xy=True)
               .transform(lonlat[:, 0], lonlat[:, 1])]
    length = np.hypot(*(xy[u] - xy[v]).T)

    # Two hops beyond the tagged edges gives each structure ground to start from.
    region = seed.copy()
    for _ in range(2):
        region[neighbours(np.flatnonzero(region))] = True
    levels: dict[int, list] = {}
    survey_of: dict[int, str] = {}
    for _ in range(40):
        new = np.array([i for i in np.flatnonzero(region)
                        if i not in levels and not np.isnan(xy[i, 0])], dtype=int)
        for name, cloud, classes in clouds:
            if len(new) == 0:
                break
            found = [surface_levels(r, classes)
                     for r in cloud.returns_near(xy[new], _LIDAR_RADIUS_M)]
            for i, lv in zip(new, found):
                if lv:
                    levels[i], survey_of[i] = lv, name
            new = np.array([i for i, lv in zip(new, found) if not lv], dtype=int)
        for i in new:
            levels[i] = []

        idx = np.flatnonzero(region)
        local = np.full(n, -1)
        local[idx] = np.arange(len(idx))
        inside = region[u] & region[v] & (u != v)
        pairs = pd.DataFrame({"a": local[np.minimum(u, v)[inside]], "b": local[np.maximum(u, v)[inside]],
                              "len": length[inside], "steps": steps[inside]}).drop_duplicates(["a", "b"])
        z, kind = label_surfaces(len(idx), list(pairs.itertuples(index=False, name=None)),
                                 seed[idx], dtm[idx], [levels.get(i, []) for i in idx])
        # An untagged node on a deck: its neighbours may be on the deck too.
        grow = neighbours(idx[(kind == 1) & ~seed[idx]])
        grow = grow[~region[grow]]
        if len(grow) == 0:
            break
        region[grow] = True

    off_dtm = kind > 0
    elevs = {ids[i]: float(h) for i, h in zip(idx[off_dtm], z[off_dtm])}
    source = {ids[i]: (f"lidar_{survey_of[i]}" if k == 1 else "interpolated")
              for i, k in zip(idx[off_dtm], kind[off_dtm])}
    unknown = {ids[i] for i in idx[kind < 0]}
    click.echo(f"    Structures: {int(seed.sum()):,} nodes on tagged edges, "
               f"{int((kind == 1).sum()):,} heights from LiDAR "
               f"({int(((kind == 1) & ~seed[idx]).sum()):,} on untagged approaches), "
               f"{int((kind == 2).sum()):,} interpolated, {len(unknown):,} unknown")
    return elevs, source, unknown


def _smoothed_for_incline(all_edges: gpd.GeoDataFrame,
                          node_coords: dict[str, tuple[float, float]],
                          node_elevs: dict[str, float]) -> dict[str, float]:
    """Node heights with the survey noise taken out of short edges.

    The graph keeps every OSM vertex as a node, so half its edges are shorter
    than 6 m, and a few decimetres of height error (or a kerb) between two
    nodes a metre apart reads as a 30% grade. Before the difference is taken,
    each node's height is averaged with its neighbours', weighted
    1 - length / _SMOOTH_M: a node 1 m away counts almost as much as the node
    itself, one 5 m away not at all. A steady slope comes through unchanged
    (the neighbours up and down the path cancel); a jump between two close
    nodes is smoothed out unless it is a real change of level, which is left
    alone. The heights written on the nodes are not smoothed.
    """
    from scipy.sparse import coo_matrix

    m = len(all_edges)
    if m == 0:
        return dict(node_elevs)
    inv, ids = pd.factorize(np.r_[all_edges["_u_id"].values, all_edges["_v_id"].values])
    u, v, n = inv[:m], inv[m:], len(ids)
    z = np.array([node_elevs.get(i, np.nan) for i in ids])
    lonlat = np.array([node_coords.get(i, (np.nan, np.nan)) for i in ids])
    east = 111319 * np.cos(np.radians(40.7))
    length = np.hypot((lonlat[u, 0] - lonlat[v, 0]) * east, (lonlat[u, 1] - lonlat[v, 1]) * 111319)

    w = np.clip(1 - length / _SMOOTH_M, 0, None)
    step = np.abs(z[u] - z[v])
    w[np.isnan(step) | (step > _LEVEL_BREAK_M) | (u == v)] = 0
    if "highway" in all_edges.columns:
        w[(all_edges["highway"] == "steps").values] = 0
    if "ext:structure" in all_edges.columns:
        w[(all_edges["ext:structure"] == "tunnel").values] = 0
    # One weight per pair of nodes, whatever number of edges join them.
    pair = pd.DataFrame({"a": np.minimum(u, v), "b": np.maximum(u, v), "w": w})
    pair = pair[pair.w > 0].groupby(["a", "b"], as_index=False).w.max()
    W = coo_matrix((np.r_[pair.w, pair.w], (np.r_[pair.a, pair.b], np.r_[pair.b, pair.a])),
                   shape=(n, n)).tocsr()
    total = 1 + np.asarray(W.sum(axis=1)).ravel()
    known = ~np.isnan(z)
    for _ in range(2):
        filled = np.where(known, z, 0.0)
        z = np.where(known, (filled + W @ filled) / total, np.nan)
    return {ids[i]: float(z[i]) for i in np.flatnonzero(known)}


def _sample_terrain(band: np.ndarray, rows: np.ndarray, cols: np.ndarray) -> np.ndarray:
    """Terrain heights at fractional pixel positions, NaN where there is no data.

    Heights are interpolated between pixel centres: the nearest pixel gives
    two neighbouring nodes either the same elevation or a whole pixel's step.
    The terrain service writes "no data" (open water, mostly) as exactly 0.0
    and the tiles carry no nodata tag, so a pixel of exactly 0.0 is no height.
    A node whose four pixels all hold data is interpolated as usual. On a
    shoreline, where some do not, the height is taken from the pixels that
    do if they carry at least half the weight; otherwise the node has none.
    """
    from scipy.ndimage import map_coordinates

    at = [np.asarray(rows) - 0.5, np.asarray(cols) - 0.5]
    valid = (band != 0.0) & ~np.isnan(band)
    weight = map_coordinates(valid.astype("float64"), at, order=1, mode="nearest")
    z = map_coordinates(np.where(valid, band, 0.0), at, order=1, mode="nearest")
    partial = weight < 1 - 1e-9
    z[partial] = np.where(weight[partial] >= 0.5, z[partial] / np.maximum(weight[partial], 0.5), np.nan)
    return z


def _tunnel_only_nodes(all_edges: gpd.GeoDataFrame) -> set[str]:
    """Nodes every one of whose edges is a tunnel edge.

    Such a node is underground, and the terrain model there is the ground
    above it. A node where a tunnel edge meets any other edge is the mouth of
    the tunnel, in the open, and keeps its terrain height.
    """
    if "ext:structure" not in all_edges.columns:
        return set()
    tunnel = (all_edges["ext:structure"] == "tunnel").values
    ends = all_edges[["_u_id", "_v_id"]]
    return set(ends[tunnel].values.ravel()) - set(ends[~tunnel].values.ravel())


def _compute_edge_inclines(all_edges: gpd.GeoDataFrame,
                            all_nodes: gpd.GeoDataFrame,
                            dem_tiles: list[Path],
                            lidar_surveys: list[dict] | None = None,
                            lidar_cache: Path | None = None) -> gpd.GeoDataFrame:
    """Sample the NYC 2017 LiDAR DEM tiles at node coordinates and add incline to each edge.

    incline = (v_elevation - u_elevation) / edge_length_m
    Positive values indicate uphill travel from _u_id to _v_id.
    Nodes on bridges and elevated ways take their height from LiDAR returns
    (_structure_elevations); tunnel edges get no incline, and nodes inside a
    tunnel no height. An edge whose two heights cannot be a slope
    (pipeline.utils.deck.grade) gets no incline and `ext:incline_unknown`.
    """
    try:
        import math
        import numpy as np
        import rasterio

        node_coords: dict[str, tuple[float, float]] = {}
        for _, row in all_nodes.iterrows():
            nid = row.get("_id")
            if nid and row.geometry is not None:
                node_coords[nid] = (row.geometry.x, row.geometry.y)

        if not node_coords:
            return all_edges

        from pyproj import Transformer as _T

        # Sample across all tiles; the first tile that covers a node wins.
        # Tiles may be a single study-area tile or a grid over each borough.
        ids = list(node_coords)
        lonlat = np.array([node_coords[i] for i in ids])
        sampled = np.full(len(ids), np.nan)
        water = np.zeros(len(ids), dtype=bool)
        for tile in dem_tiles:
            todo = np.flatnonzero(np.isnan(sampled))
            if len(todo) == 0:
                break
            try:
                src = rasterio.open(tile)
            except Exception as exc:
                click.echo(f"  Warning: cannot read {tile.name} ({exc}); nodes it covers "
                           "get no height from it")
                continue
            with src:
                raster_epsg = src.crs.to_epsg() or 4326
                x, y = lonlat[todo, 0], lonlat[todo, 1]
                if raster_epsg != 4326:
                    x, y = _T.from_crs(4326, raster_epsg, always_xy=True).transform(x, y)

                # The tiles carry no nodata value, so a point outside a tile's
                # extent would read as 0.0. Sample only the nodes a tile
                # covers, or every node off the first tile is pinned at 0 m.
                # Half-open, as rasterio indexes: the right and bottom edges
                # belong to the neighbouring tile.
                b = src.bounds
                inside = (b.left <= x) & (x < b.right) & (b.bottom < y) & (y <= b.top)
                if not inside.any():
                    continue
                band = src.read(1).astype("float64")
                if src.nodata is not None and not np.isnan(src.nodata):
                    band[band == src.nodata] = np.nan
                cols, rows = ~src.transform * (x[inside], y[inside])
                # A node on the seam of two tiles that has no data in this
                # one is left for the next.
                z = _sample_terrain(band, rows, cols)
                sampled[todo[inside]] = z
                water[todo[inside]] = np.isnan(z)
        node_elevs: dict[str, float] = {ids[i]: float(sampled[i])
                                        for i in np.flatnonzero(~np.isnan(sampled))}

        click.echo(f"    Elevations sampled: {len(node_elevs):,}/{len(node_coords):,} nodes")

        # On a bridge the terrain model is not the walking surface. Without
        # the LiDAR surveys (not configured, or unreachable) a node on a
        # structure gets no height written and a structure edge no incline.
        deck_source: dict[str, str] = {}
        no_height: set[str] = set()
        if "ext:structure" in all_edges.columns:
            on = all_edges["ext:structure"].isin(["bridge", "elevated"])
            no_height = set(all_edges.loc[on, "_u_id"]) | set(all_edges.loc[on, "_v_id"])
        if lidar_surveys and lidar_cache is not None and no_height:
            try:
                # Where the terrain model has no data there is open water
                # under the node, and the deck rules need that floor: with
                # none, the water's own LiDAR returns pass for a deck. Sea
                # level stands in for it here and is not written as a height.
                floor = {**node_elevs, **{ids[i]: 0.0 for i in np.flatnonzero(water & np.isnan(sampled))}}
                deck_elevs, deck_source, _ = _structure_elevations(
                    all_edges, node_coords, floor, lidar_surveys, lidar_cache)
                node_elevs.update(deck_elevs)
                no_height -= set(deck_source)
            except Exception as exc:
                click.echo(f"  Warning: structure elevations failed ({exc}). "
                           "Bridges get no incline.")
        # Underground, neither the terrain model nor a deck above is the
        # walking surface.
        for nid in _tunnel_only_nodes(all_edges):
            node_elevs.pop(nid, None)
            deck_source.pop(nid, None)

        # Mutates the caller's all_nodes: sampled elevations ship on the nodes
        # as ext:elevation_m alongside the per-edge incline.
        if node_elevs and "_id" in all_nodes.columns:
            all_nodes["ext:elevation_m"] = all_nodes["_id"].map(
                lambda nid: round(node_elevs[nid], 1)
                if nid in node_elevs and nid not in no_height else None
            )
            if deck_source:
                all_nodes["ext:elevation_source"] = all_nodes["_id"].map(deck_source)

        slope_elevs = _smoothed_for_incline(all_edges, node_coords, node_elevs)

        # An elevator joins two levels by machine. The edges at an elevator
        # node run between its levels, so the difference of their end heights
        # is not a slope anyone rolls up; they get no incline and no mark.
        lifts: set[str] = set()
        if "ext:osm_highway" in all_nodes.columns:
            lifts = set(all_nodes.loc[all_nodes["ext:osm_highway"] == "elevator", "_id"])

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

        inclines, unknown = [], []
        for _, row in all_edges.iterrows():
            uid = row.get("_u_id")
            vid = row.get("_v_id")
            # In a tunnel the terrain model is not the walking surface and
            # no survey sees it; no incline is better than a wrong one. The
            # same goes for a bridge or elevated edge with an end that has
            # no deck height.
            structure = row.get("ext:structure")
            incline, cannot = None, False
            if (structure == "tunnel" or (structure in ("bridge", "elevated")
                                          and (uid in no_height or vid in no_height))
                    or uid in lifts or vid in lifts):
                pass
            elif uid in slope_elevs and vid in slope_elevs:
                # Heights that cannot be a slope along the edge (the ground
                # at one end and a deck at the other, or noise on a very
                # short edge) are not written as one; the edge says so.
                incline, cannot = grade(slope_elevs[vid] - slope_elevs[uid],
                                        _length_m(row.geometry), row.get("highway") == "steps")
            inclines.append(incline)
            unknown.append("yes" if cannot else None)

        all_edges = all_edges.copy()
        all_edges["incline"] = inclines
        all_edges["ext:incline_unknown"] = unknown
        n_with = sum(1 for v in inclines if v is not None)
        click.echo(f"    Incline set on {n_with:,}/{len(all_edges):,} edges; "
                   f"{sum(1 for v in unknown if v):,} with heights that are not a slope")
        return all_edges

    except Exception as exc:
        click.echo(f"  Warning: incline computation failed ({exc}). Skipping.")
        return all_edges


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

ATTRIBUTION = (
    "Pedestrian network from OpenSidewalks NYC, an independent dataset in the "
    "OpenSidewalks Schema, ODbL-1.0. Map data \u00a9 OpenStreetMap contributors "
    "(openstreetmap.org/copyright). Also from NYC DOT and NYC OTI data on NYC "
    "Open Data, and LiDAR from NYS GIS and NOAA."
)
REPO_URL = "https://github.com/msradam/opensidewalks-nyc"


def root_metadata(build_cfg: dict, osm_extract: dict, pipeline_version: str, git_sha: str) -> dict:
    """The root members of the FeatureCollection, as the schema defines them.

    `dataTimestamp` is how current the data is: the OSM extract's own data
    timestamp, since the network is OSM's. The curb ramp survey is older
    (mostly 2018) and each ramp's `ext:source_timestamp` says when it was
    read. The build time is `pipelineVersion.builtAt`. `dataSource` names
    the sources; `pipelineVersion` names the software and the commit.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        # Must match the CompatibleSchemaURI enum in the OSW schema exactly.
        # The GitHub raw URL is used to *fetch* the schema; this field must use
        # the canonical sidewalks.washington.edu URI the enum validates against.
        "$schema": (
            f"https://sidewalks.washington.edu/opensidewalks/"
            f"{build_cfg.get('osw_schema_version', '0.3')}/schema.json"
        ),
        "type": "FeatureCollection",
        "dataSource": {
            "name": ("OpenStreetMap, NYC DOT Pedestrian Ramp Locations, NYC Planimetric "
                     "Sidewalks, NYC 2017 LiDAR (terrain model and point clouds)"),
            "url": REPO_URL,
            "license": "ODbL-1.0",
            "licenseUrl": "https://opendatacommons.org/licenses/odbl/1-0/",
            "attribution": ATTRIBUTION,
            # Which OSM snapshot this build read: extract URL, SHA-256 and
            # the extract's own data timestamp.
            "osmExtract": {
                "url": osm_extract.get("url"),
                "sha256": osm_extract.get("content_hash"),
                "dataTimestamp": osm_extract.get("osm_data_timestamp"),
            },
        },
        "dataTimestamp": osm_extract.get("osm_data_timestamp") or now_iso,
        "pipelineVersion": {
            "name": "opensidewalks-nyc",
            "version": pipeline_version,
            "url": f"{REPO_URL}/tree/{git_sha}" if git_sha and git_sha != "unknown" else REPO_URL,
            "gitSHA": git_sha,
            "builtAt": now_iso,
        },
    }


def run(sources: dict, build_cfg: dict, repo_root: Path) -> None:
    """Stage 4: snap nodes, assign IDs, build canonical FeatureCollection."""
    staged_dir    = repo_root / build_cfg["dirs"]["staged"]
    raw_dir       = repo_root / build_cfg["dirs"]["raw"]
    pipeline_version = build_cfg.get("pipeline_version", "unknown")
    git_sha       = get_git_sha()
    osm_extract   = load_manifest(raw_dir).get("osm_extract", {})

    snap_tolerance = build_cfg.get("snap_tolerance_meters", 5.0)
    min_comp_size  = build_cfg.get("min_component_size_nodes", 3)

    # Load staged feature files.
    click.echo("  Loading staged features...")
    sidewalks  = gpd.read_file(staged_dir / "sidewalks.geojson")
    crossings  = gpd.read_file(staged_dir / "crossings.geojson")
    footways   = gpd.read_file(staged_dir / "footways.geojson")
    streets    = gpd.read_file(staged_dir / "streets.geojson")
    curb_nodes = gpd.read_file(staged_dir / "curb_nodes.geojson")
    zones      = _load_zones(staged_dir / "zones.geojson")
    # A zone takes part in the graph through its ring: one row per pair of
    # consecutive ring nodes, so that the ramp snap, the endpoint merge, the
    # orphan check, the heights and the topology report see a plaza as the
    # edges it replaced. The rows are dropped before the file is written.
    rings      = _ring_rows(zones)

    osm_nodes_path = repo_root / build_cfg["dirs"]["clean"] / "osm_nodes.geojson"
    osm_nodes = gpd.read_file(osm_nodes_path) if osm_nodes_path.exists() else gpd.GeoDataFrame()

    region_raw = json.loads((staged_dir / "region.json").read_text())

    click.echo(f"    Sidewalks:  {len(sidewalks):,}")
    click.echo(f"    Crossings:  {len(crossings):,}")
    click.echo(f"    Footways:   {len(footways):,}")
    click.echo(f"    Streets:    {len(streets):,}")
    click.echo(f"    Curb nodes: {len(curb_nodes):,}")
    click.echo(f"    Zones:      {len(zones):,}")

    # Combine all edges for the full graph.
    all_edges = gpd.GeoDataFrame(
        pd.concat([sidewalks, crossings, footways, streets, rings], ignore_index=True),
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
    # Use only pedestrian edges (sidewalks + crossings + footways) for snapping.
    # Curb ramps sit where sidewalks meet road crossings, not on street
    # centerlines.
    click.echo("\n  Snapping curb nodes to edge endpoints...")
    pedestrian_edges = gpd.GeoDataFrame(
        pd.concat([sidewalks, crossings, footways, rings], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )
    click.echo(f"    {len(pedestrian_edges):,} pedestrian edge endpoints to index")
    endpoints  = _endpoint_coords(pedestrian_edges)
    ends       = _crossing_ends(crossings, gpd.GeoDataFrame(
        pd.concat([sidewalks, footways, rings], ignore_index=True), geometry="geometry", crs="EPSG:4326"))
    click.echo(f"    {len(ends):,} crossing ends")
    surveyed   = curb_nodes[["_id", "geometry"]].copy()
    curb_nodes = _snap_curb_nodes(curb_nodes, endpoints, snap_tolerance, preferred=ends)

    # Close near-miss gaps: dead ends and separate components whose endpoints
    # lie within the tolerance get the same node ID.
    click.echo("\n  Merging near-miss endpoints...")
    merge_tolerance = build_cfg.get("endpoint_merge_tolerance_meters", 2.0)
    all_edges, endpoint_remap = _merge_near_endpoints(all_edges, tolerance_m=merge_tolerance)

    # A curb node took the ID of the endpoint it snapped to. If the merge then
    # folded that endpoint into another node, follow it, or the ramp is left on
    # an ID no edge references.
    if endpoint_remap and len(curb_nodes) > 0:
        curb_nodes["_id"] = curb_nodes["_id"].map(lambda i: endpoint_remap.get(i, i))
    if endpoint_remap and len(zones) > 0:
        zones["_w_id"] = zones["_w_id"].map(lambda w: [endpoint_remap.get(i, i) for i in w])

    # Several ramps at one corner can land on the same node, and a node holds
    # one ramp's fields. The first ramp keeps the node; the others go back to
    # their surveyed position as unattached nodes, so the dedup below does not
    # silently discard their survey record.
    extra = curb_nodes["_id"].duplicated(keep="first")
    if extra.any():
        curb_nodes.loc[extra, "_id"] = surveyed.loc[extra, "_id"]
        curb_nodes.loc[extra, "geometry"] = surveyed.loc[extra, "geometry"]
        click.echo(f"    {int(extra.sum()):,} ramps share a node with another ramp; "
                   f"kept at their surveyed position")

    # Prepare OSM nodes with _id field.
    if len(osm_nodes) > 0:
        # OSMnx node IDs are in the osmid column or index.
        if "_id" not in osm_nodes.columns:
            osm_nodes["_id"] = osm_nodes.apply(
                lambda r: node_id(r.geometry.x, r.geometry.y),
                axis=1
            )

        # Stage 1 saves the nodes of every way that passed the tag filter.
        # Stage 3 then drops some of those ways (a cycleway or track with no
        # foot tag), and their nodes would ride along as points no edge
        # touches. A surveyed ramp off the graph is a record worth keeping;
        # a bare OSM vertex is not.
        referenced = set(all_edges["_u_id"]) | set(all_edges["_v_id"])
        orphan = ~osm_nodes["_id"].isin(referenced)
        if orphan.any():
            click.echo(f"    Dropped {int(orphan.sum()):,} OSM nodes that no edge references")
            osm_nodes = osm_nodes[~orphan].copy()

        if "ext:source" not in osm_nodes.columns:
            osm_nodes["ext:source"] = "osm_walk"
        if "ext:pipeline_version" not in osm_nodes.columns:
            osm_nodes["ext:pipeline_version"] = pipeline_version
        # The nodes come straight from Stage 2 with the OSMnx region name
        # ("queens", "bronx_county"); the edges got their codes in Stage 3.
        if "ext:borough" in osm_nodes.columns:
            osm_nodes["ext:borough"] = osm_nodes["ext:borough"].map(borough_code)

        osm_nodes = _osm_node_tags(osm_nodes)

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

    # Inject bare nodes for any dangling edge endpoints. This runs before the
    # curb nodes join, so the first row for every referenced ID sits at the
    # edge's own endpoint coordinate; the dedup below keeps that row's geometry
    # and folds the ramp fields into it.
    click.echo("\n  Ensuring all edge endpoints have corresponding Node features...")
    all_nodes = _inject_missing_nodes(all_edges, osm_nodes, pipeline_version)

    # Combine all nodes.
    all_node_gdfs = [g for g in [all_nodes, curb_nodes] if len(g) > 0]
    if all_node_gdfs:
        all_nodes = gpd.GeoDataFrame(
            pd.concat(all_node_gdfs, ignore_index=True),
            geometry="geometry", crs="EPSG:4326"
        )
    else:
        all_nodes = gpd.GeoDataFrame(geometry=gpd.GeoSeries([], crs="EPSG:4326"))

    # Deduplicate nodes by _id, merging properties so CurbRamp annotations
    # (barrier, kerb, tactile_paving) survive even when the same location also
    # appears as a generic OSM node. Without this, drop_duplicates(keep="first")
    # silently discards DOT ramp data whenever an OSM node lands at the same point.
    if "_id" in all_nodes.columns:
        before = len(all_nodes)
        all_nodes = _dedup_nodes(all_nodes)
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
        lidar = sources.get("sources", {}).get("lidar_points", {}).get("retrieval", {})
        all_edges = _compute_edge_inclines(all_edges, all_nodes, dem_tiles,
                                           lidar_surveys=lidar.get("surveys"),
                                           lidar_cache=raw_dir / "lidar_points")
    else:
        click.echo("\n  LiDAR DEM not found. Skipping incline (run Stage 1 to acquire)")

    # Topology report: analyze pedestrian graph connectivity (exclude street edges
    # which are not part of the pedestrian routing graph).
    click.echo("\n  Computing connected components (pedestrian edges only)...")
    pedestrian_for_topo = gpd.GeoDataFrame(
        pd.concat([sidewalks, crossings, footways, rings], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )
    _topology_report(pedestrian_for_topo, all_nodes, staged_dir, min_comp_size)

    # The ring rows have done their work; the zones ship as Polygons.
    if "_ring_of" in all_edges.columns:
        all_edges = all_edges[all_edges["_ring_of"].isna()].drop(columns=["_ring_of"]).copy()

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
    zone_features = _gdf_to_features(zones) if len(zones) > 0 else []
    all_features  = node_features + edge_features + zone_features

    # Root OSW metadata.
    fc = {**root_metadata(build_cfg, osm_extract, pipeline_version, git_sha),
          "region": region_raw, "features": all_features}

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
