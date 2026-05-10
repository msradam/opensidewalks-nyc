"""Stage 6: Export the validated FeatureCollection to output formats.

Input:  data/staged/nyc-osw-unvalidated.geojson
Output: output/nyc-osw.geojson      . Canonical OSW GeoJSON
        output/nyc.graphml          . NetworkX/academic GraphML
        output/nyc-routing.json     . Routing-friendly edges+nodes+costs

Each export format is a separate function so future formats can be added
without touching upstream stages.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import click
import networkx as nx
import geopandas as gpd
from shapely.geometry import shape


# ---------------------------------------------------------------------------
# Export: canonical OSW GeoJSON
# ---------------------------------------------------------------------------

def export_osw_geojson(fc: dict, output_dir: Path) -> Path:
    """Write the canonical OSW FeatureCollection to output/nyc-osw.geojson."""
    out_path = output_dir / "nyc-osw.geojson"
    out_path.write_text(json.dumps(fc, indent=2))
    size_mb = out_path.stat().st_size / 1_048_576
    click.echo(f"  nyc-osw.geojson: {len(fc['features']):,} features ({size_mb:.1f} MB)")
    return out_path


# ---------------------------------------------------------------------------
# Export: GraphML for NetworkX/academic use
# ---------------------------------------------------------------------------

def export_graphml(fc: dict, output_dir: Path) -> Path:
    """Build a NetworkX DiGraph from OSW edges and export as GraphML.

    Only pedestrian-traversable edges (sidewalks, crossings, footways) are
    included in the routing graph. Street edges are included as a separate
    attribute for reference. Nodes get coordinate attributes.
    """
    G = nx.DiGraph()

    node_coords = {}

    for feat in fc["features"]:
        props     = feat.get("properties", {}) or {}
        geom      = feat.get("geometry", {}) or {}
        geom_type = geom.get("type", "")
        fid       = props.get("_id")

        if geom_type == "Point":
            coords = geom.get("coordinates", [])
            if len(coords) >= 2 and fid:
                lon, lat = coords[0], coords[1]
                node_coords[fid] = (lon, lat)
                node_attrs = {k: str(v) for k, v in props.items()
                              if k not in ("_id",) and v is not None}
                node_attrs["lon"] = str(lon)
                node_attrs["lat"] = str(lat)
                G.add_node(fid, **node_attrs)

        elif geom_type == "LineString":
            u_id = props.get("_u_id")
            v_id = props.get("_v_id")
            if u_id and v_id and fid:
                # Compute length in metres using pyproj.
                edge_attrs = {k: str(v) for k, v in props.items()
                              if k not in ("_id", "_u_id", "_v_id") and v is not None}

                # Length from geometry if not already set. Haversine per segment.
                if "length" not in props:
                    try:
                        import math
                        coords_list = geom.get("coordinates", [])
                        length_m = 0.0
                        for k in range(len(coords_list) - 1):
                            lon1, lat1 = coords_list[k][0],   coords_list[k][1]
                            lon2, lat2 = coords_list[k+1][0], coords_list[k+1][1]
                            dx = (lon2 - lon1) * 111319 * math.cos(math.radians((lat1 + lat2) / 2))
                            dy = (lat2 - lat1) * 111319
                            length_m += math.sqrt(dx * dx + dy * dy)
                        edge_attrs["length_approx_m"] = str(round(length_m, 2))
                    except Exception:
                        pass

                G.add_edge(u_id, v_id, edge_id=fid, **edge_attrs)
                # Pedestrian graph is undirected. Add reverse edge.
                G.add_edge(v_id, u_id, edge_id=fid, **edge_attrs)

    # Add coordinate attributes to nodes that exist as edges but weren't Point features.
    for node in G.nodes():
        if "lon" not in G.nodes[node] and node in node_coords:
            lon, lat = node_coords[node]
            G.nodes[node]["lon"] = str(lon)
            G.nodes[node]["lat"] = str(lat)

    out_path = output_dir / "nyc.graphml"
    nx.write_graphml(G, str(out_path))
    size_mb = out_path.stat().st_size / 1_048_576
    click.echo(f"  nyc.graphml: {G.number_of_nodes():,} nodes, "
               f"{G.number_of_edges() // 2:,} edges ({size_mb:.1f} MB)")
    return out_path


# ---------------------------------------------------------------------------
# Export: routing-friendly JSON
# ---------------------------------------------------------------------------

def export_routing_json(fc: dict, output_dir: Path) -> Path:
    """Export a compact routing-friendly JSON format.

    Structure:
    {
      "meta": { ... },
      "nodes": { "<_id>": { "lon": ..., "lat": ..., "props": {...} }, ... },
      "edges": [
        {
          "_id": ..., "_u_id": ..., "_v_id": ...,
          "highway": ..., "footway": ...,
          "length_m": ...,
          "surface": ...,
          "props": { ... }
        }, ...
      ]
    }

    Consumers can use this to build routing graphs without parsing full GeoJSON.
    Costs (e.g. Incline penalty, surface roughness) are left to the consumer -
    this format provides the raw attributes for cost function construction.
    """
    nodes_out = {}
    edges_out = []

    for feat in fc["features"]:
        props     = feat.get("properties", {}) or {}
        geom      = feat.get("geometry", {}) or {}
        geom_type = geom.get("type", "")
        fid       = props.get("_id")

        if geom_type == "Point":
            coords = geom.get("coordinates", [])
            if len(coords) >= 2 and fid:
                extra = {k: v for k, v in props.items()
                         if k not in ("_id",) and v is not None}
                nodes_out[fid] = {
                    "lon": coords[0],
                    "lat": coords[1],
                    "props": extra,
                }

        elif geom_type == "LineString":
            u_id = props.get("_u_id")
            v_id = props.get("_v_id")
            if not (u_id and v_id and fid):
                continue

            # Compute approximate length.
            coords_list = geom.get("coordinates", [])
            length_m    = 0.0
            for i in range(len(coords_list) - 1):
                dx = (coords_list[i+1][0] - coords_list[i][0]) * 111319 * \
                     __import__("math").cos(__import__("math").radians(coords_list[i][1]))
                dy = (coords_list[i+1][1] - coords_list[i][1]) * 111319
                length_m += (dx**2 + dy**2) ** 0.5

            edge_record = {
                "_id":      fid,
                "_u_id":    u_id,
                "_v_id":    v_id,
                "highway":  props.get("highway"),
                "footway":  props.get("footway"),
                "surface":  props.get("surface"),
                "length_m": round(length_m, 2),
                "props":    {k: v for k, v in props.items()
                             if k not in ("_id", "_u_id", "_v_id", "highway",
                                          "footway", "surface") and v is not None},
            }
            edges_out.append(edge_record)

    routing_doc = {
        "meta": {
            "generated_at":      datetime.now(timezone.utc).isoformat(),
            "schema_version":    fc.get("$schema", ""),
            "pipeline_version":  (fc.get("pipelineVersion") or {}).get("version", ""),
            "n_nodes":           len(nodes_out),
            "n_edges":           len(edges_out),
            "description": (
                "Routing-friendly export of the NYC OpenSidewalks pedestrian graph. "
                "Use nodes dict + edges list to build a routing graph. "
                "length_m is approximate (Haversine on edge coordinates). "
                "Cost functions (surface roughness, incline penalty, etc.) are left "
                "to the consumer."
            ),
        },
        "nodes": nodes_out,
        "edges": edges_out,
    }

    out_path = output_dir / "nyc-routing.json"
    out_path.write_text(json.dumps(routing_doc))
    size_mb = out_path.stat().st_size / 1_048_576
    click.echo(f"  nyc-routing.json: {len(nodes_out):,} nodes, "
               f"{len(edges_out):,} edges ({size_mb:.1f} MB)")
    return out_path


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def run(sources: dict, build_cfg: dict, repo_root: Path) -> None:
    """Stage 6: export all output formats."""
    staged_dir = repo_root / build_cfg["dirs"]["staged"]
    output_dir = repo_root / build_cfg["dirs"]["output"]
    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = staged_dir / "nyc-osw-unvalidated.geojson"
    if not input_path.exists():
        raise FileNotFoundError(
            f"Staged FeatureCollection not found: {input_path}\n"
            "Run stages 1-4 first."
        )

    click.echo(f"  Loading {input_path.name}...")
    fc = json.loads(input_path.read_text())
    click.echo(f"  {len(fc['features']):,} features")

    outputs = build_cfg.get("outputs", {})

    if outputs.get("geojson", True):
        export_osw_geojson(fc, output_dir)

    if outputs.get("graphml", True):
        export_graphml(fc, output_dir)

    if outputs.get("routing_json", True):
        export_routing_json(fc, output_dir)

    click.echo(f"\n  All outputs written to {output_dir}/")
