"""Convert the canonical OSW GeoJSON to a NetworkX GraphML file.

The graph is built from the LineString edges:
  - each edge contributes one networkx edge from _u_id to _v_id (its OSW ID
    is the `_id` attribute; NetworkX numbers parallel edges itself)
  - each Point feature contributes a node keyed by _id, with x/y coords
  - edge attributes: all OSW properties (flattened to strings/numbers), and
    `length_m`, the length of the edge's line in metres
  - node attributes: x, y (lon, lat), plus any OSW point properties
  - graph attributes: licence, attribution and the OSM snapshot
  - each Pedestrian Zone (a Polygon) contributes the edges a person can walk
    across it: its ring and the chords between its entrances, with `ext:zone`

The graph is a directed multigraph, as the OSW file is: a walkable segment
is one edge per travel direction and `incline` is signed for that direction.
An undirected graph would keep one of the two and lose which way is uphill.

With --undirected the graph is a simple undirected one, for code written
against an undirected graph: one edge per pair of nodes, the first
the file holds for that pair. Its `_u_id` and `_v_id` attributes say which
way its `incline` reads; parallel edges and the reverse direction are gone.

Usage:
    python scripts/to_graphml.py INPUT.geojson OUTPUT.graphml [--undirected]
"""

from __future__ import annotations

import itertools
import json
import sys
from decimal import Decimal
from pathlib import Path

import ijson
import networkx as nx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.utils.zones import _haversine_m, zone_edges


PRIMITIVE = (str, int, float, bool)


def _coerce(v):
    """GraphML only accepts scalar primitives. Coerce or stringify."""
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, PRIMITIVE):
        return v
    return str(v)


def _flatten(props: dict) -> dict:
    return {k: _coerce(v) for k, v in props.items() if v is not None}


def main(in_path: Path, out_path: Path, undirected: bool = False) -> None:
    print(f"streaming {in_path.name}...", flush=True)
    G = nx.Graph() if undirected else nx.MultiDiGraph()
    with in_path.open("rb") as f:
        source = next(ijson.items(f, "dataSource"), None) or {}
    for k, v in source.items():
        # GraphML holds primitives only; osmExtract (a dict) goes in as JSON.
        G.graph[k] = v if isinstance(v, PRIMITIVE) else json.dumps(v)

    n_edges = 0
    n_nodes = 0
    n_skipped = 0
    zones, referenced, node_xy, node_z = [], set(), {}, {}

    with in_path.open("rb") as f:
        for feat in ijson.items(f, "features.item"):
            geom = feat.get("geometry") or {}
            props = feat.get("properties") or {}
            gtype = geom.get("type")

            if gtype == "Point":
                fid = props.get("_id")
                if not fid:
                    n_skipped += 1
                    continue
                coords = geom.get("coordinates") or [None, None]
                attrs = _flatten(props)
                attrs["x"] = float(coords[0]) if coords[0] is not None else 0.0
                attrs["y"] = float(coords[1]) if coords[1] is not None else 0.0
                G.add_node(fid, **attrs)
                n_nodes += 1
                node_xy[fid] = (attrs["x"], attrs["y"])
                if props.get("ext:elevation_m") is not None:
                    node_z[fid] = float(props["ext:elevation_m"])

            elif gtype == "LineString":
                u = props.get("_u_id")
                v = props.get("_v_id")
                if not u or not v:
                    n_skipped += 1
                    continue
                referenced.update((u, v))
                if undirected and G.has_edge(u, v):
                    continue
                attrs = _flatten(props)
                line = [(float(c[0]), float(c[1])) for c in geom.get("coordinates") or []]
                attrs["length_m"] = round(sum(_haversine_m(a, b) for a, b in itertools.pairwise(line)), 3)
                G.add_edge(u, v, **attrs)
                n_edges += 1

            elif gtype == "Polygon" and props.get("_w_id"):
                zones.append({"properties": props})

    # A Pedestrian Zone (a plaza) is a Polygon in the file. In the graph it
    # is the edges a person can walk across it: its ring and the chords
    # between its entrances, each named by ext:zone.
    n_zone_edges = 0
    for z in zones:
        for e in zone_edges(z, node_xy, referenced, node_z):
            u, v = e["_u_id"], e["_v_id"]
            if undirected and G.has_edge(u, v):
                continue
            G.add_edge(u, v, **_flatten({k: val for k, val in e.items() if k != "coordinates"}))
            n_zone_edges += 1

    print(f"  nodes: {n_nodes:,} | edges: {n_edges:,} | zone edges: {n_zone_edges:,} "
          f"from {len(zones):,} zones | skipped: {n_skipped:,}")
    # Edges referencing a node with no Point feature in the file leave a bare
    # auto-created node behind. Backfill x/y so GraphML attributes stay uniform.
    for nid, attrs in G.nodes(data=True):
        attrs.setdefault("x", 0.0)
        attrs.setdefault("y", 0.0)

    print(f"writing {out_path.name}...", flush=True)
    nx.write_graphml(G, out_path)
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"  wrote {size_mb:.1f} MB")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--undirected"]
    if len(args) != 2:
        print("usage: to_graphml.py INPUT.geojson OUTPUT.graphml [--undirected]",
              file=sys.stderr)
        sys.exit(2)
    main(Path(args[0]), Path(args[1]), undirected="--undirected" in sys.argv)
