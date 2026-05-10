"""Convert the canonical OSW GeoJSON to a NetworkX GraphML file.

The graph is built from the LineString edges:
  - each edge contributes one networkx edge keyed by (_u_id, _v_id)
  - each Point feature contributes a node keyed by _id, with x/y coords
  - edge attributes: all OSW properties (flattened to strings/numbers)
  - node attributes: x, y (lon, lat), plus any OSW point properties

The graph is undirected (pedestrian edges are bidirectional by default).

Usage:
    python scripts/to_graphml.py INPUT.geojson OUTPUT.graphml
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import ijson
import networkx as nx


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


def main(in_path: Path, out_path: Path) -> None:
    print(f"streaming {in_path.name}...", flush=True)
    G = nx.Graph()

    n_edges = 0
    n_nodes = 0
    n_skipped = 0

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

            elif gtype == "LineString":
                u = props.get("_u_id")
                v = props.get("_v_id")
                if not u or not v:
                    n_skipped += 1
                    continue
                attrs = _flatten(props)
                G.add_edge(u, v, **attrs)
                n_edges += 1

    print(f"  nodes: {n_nodes:,} | edges: {n_edges:,} | skipped: {n_skipped:,}")
    # Some edges may reference nodes that arrived later or weren't in the file;
    # NetworkX auto-creates them as bare nodes. Backfill x/y where missing.
    for nid, attrs in G.nodes(data=True):
        attrs.setdefault("x", 0.0)
        attrs.setdefault("y", 0.0)

    print(f"writing {out_path.name}...", flush=True)
    nx.write_graphml(G, out_path)
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"  wrote {size_mb:.1f} MB")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: to_graphml.py INPUT.geojson OUTPUT.graphml", file=sys.stderr)
        sys.exit(2)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
