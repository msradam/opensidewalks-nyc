"""Split the canonical OSW GeoJSON into five per-borough GeoJSONs.

Two-pass streaming, bounded memory:

    Pass 1: scan all LineStrings, bucket their _id by `ext:borough`,
            and collect the set of node _ids referenced via _u_id/_v_id.
    Pass 2: stream all features again, write each LineString into its
            borough file and each Point into every borough file whose
            edge-endpoint set contains its _id.

Per-borough output is a valid GeoJSON FeatureCollection, with the same
top-level metadata as the source minus the `region` field (each borough
gets only its own subset).

Usage:
    python scripts/split_by_borough.py INPUT.geojson OUTDIR/
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import ijson

BOROUGHS = ("MN", "BK", "QN", "BX", "SI")


def _default(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"unserialisable: {type(o)}")


def main(in_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"pass 1: scanning edges in {in_path.name}...", flush=True)
    edge_ids_by_boro: dict[str, set[str]] = defaultdict(set)
    node_ids_by_boro: dict[str, set[str]] = defaultdict(set)
    edge_count = 0
    node_count = 0

    with in_path.open("rb") as f:
        for feat in ijson.items(f, "features.item"):
            geom = feat.get("geometry") or {}
            props = feat.get("properties") or {}
            if geom.get("type") == "LineString":
                edge_count += 1
                boro = props.get("ext:borough")
                if boro not in BOROUGHS:
                    continue
                fid = props.get("_id")
                if fid:
                    edge_ids_by_boro[boro].add(fid)
                u, v = props.get("_u_id"), props.get("_v_id")
                if u:
                    node_ids_by_boro[boro].add(u)
                if v:
                    node_ids_by_boro[boro].add(v)
            elif geom.get("type") == "Point":
                node_count += 1

    print(f"  edges scanned: {edge_count:,}, points: {node_count:,}")
    for b in BOROUGHS:
        print(f"  {b}: {len(edge_ids_by_boro[b]):,} edges, "
              f"{len(node_ids_by_boro[b]):,} referenced nodes")

    print("\npass 2: writing per-borough files...", flush=True)
    handles = {b: (out_dir / f"nyc-osw-{b}.geojson").open("w", encoding="utf-8")
               for b in BOROUGHS}
    counts = {b: {"edges": 0, "nodes": 0} for b in BOROUGHS}
    first = {b: True for b in BOROUGHS}

    # write FeatureCollection prelude
    for b, h in handles.items():
        h.write('{"type":"FeatureCollection",')
        h.write(f'"name":"nyc-osw-{b}",')
        h.write('"features":[')

    with in_path.open("rb") as f:
        for feat in ijson.items(f, "features.item"):
            geom = feat.get("geometry") or {}
            props = feat.get("properties") or {}
            gtype = geom.get("type")
            if gtype == "LineString":
                boro = props.get("ext:borough")
                if boro in BOROUGHS:
                    h = handles[boro]
                    if not first[boro]:
                        h.write(",")
                    h.write(json.dumps(feat, default=_default, separators=(",", ":")))
                    first[boro] = False
                    counts[boro]["edges"] += 1
            elif gtype == "Point":
                fid = props.get("_id")
                if not fid:
                    continue
                serialised = json.dumps(feat, default=_default, separators=(",", ":"))
                for b in BOROUGHS:
                    if fid in node_ids_by_boro[b]:
                        h = handles[b]
                        if not first[b]:
                            h.write(",")
                        h.write(serialised)
                        first[b] = False
                        counts[b]["nodes"] += 1

    for b, h in handles.items():
        h.write("]}\n")
        h.close()

    print()
    for b in BOROUGHS:
        path = out_dir / f"nyc-osw-{b}.geojson"
        size_mb = path.stat().st_size / 1024 / 1024
        print(f"  {path.name}: {counts[b]['edges']:,} edges, "
              f"{counts[b]['nodes']:,} nodes, {size_mb:.1f} MB")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: split_by_borough.py INPUT.geojson OUTDIR/", file=sys.stderr)
        sys.exit(2)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
