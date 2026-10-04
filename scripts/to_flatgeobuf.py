"""Convert the canonical OSW GeoJSON to FlatGeobuf (.fgb).

FlatGeobuf is a streamable, spatially indexed binary geo format. It's
typically 5-10x smaller than the equivalent GeoJSON and supports bbox
queries without a full read. Produced via pyogrio (GDAL).

Usage:
    python scripts/to_flatgeobuf.py INPUT.geojson OUTPUT.fgb
"""

from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import ijson


def main(in_path: Path, out_path: Path) -> None:
    print(f"reading {in_path.name}...", flush=True)
    gdf = gpd.read_file(in_path)
    print(f"  features: {len(gdf):,}, columns: {len(gdf.columns)}")

    # FlatGeobuf has no free-form root object; the layer title and
    # description carry the licence and credit instead.
    with in_path.open("rb") as f:
        source = next(ijson.items(f, "dataSource"), None) or {}
    extract = source.get("osmExtract") or {}
    description = " ".join(filter(None, [
        source.get("attribution"),
        f"Licence: {source['license']} ({source.get('licenseUrl')})." if source.get("license") else None,
        f"OSM data as of {extract['dataTimestamp']}." if extract.get("dataTimestamp") else None,
    ]))

    print(f"writing {out_path.name} (FlatGeobuf, spatial index)...", flush=True)
    gdf.to_file(out_path, driver="FlatGeobuf", spatial_index=True,
                TITLE="OpenSidewalks NYC", DESCRIPTION=description)
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"  wrote {size_mb:.1f} MB")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: to_flatgeobuf.py INPUT.geojson OUTPUT.fgb", file=sys.stderr)
        sys.exit(2)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
