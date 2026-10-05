"""Cut one community district out of the built OSW file.

Keeps every edge and Pedestrian Zone with a vertex inside the district polygon
grown by MARGIN_M, the nodes they reference, and the unattached nodes (surveyed ramps)
inside the same area. Writes an OSW FeatureCollection and the split ZIP the
validator and the converters read.

usage: python compare/clip_osw.py OSW_GEOJSON CD_GEOJSON BORO_CD OUT_PREFIX
"""
import json
import sys
import zipfile
from pathlib import Path

import ijson
from shapely import contains_xy
from shapely.geometry import shape

MARGIN_M = 150


def clip(osw, cd_geojson, boro_cd, prefix):
    with open(cd_geojson) as f:
        cd = next(shape(ft["geometry"]) for ft in json.load(f)["features"] if ft["properties"]["boro_cd"] == boro_cd)
    area = cd.buffer(MARGIN_M / 111320)
    nodes, edges, zones, loose, need = {}, [], [], [], set()
    with open(osw, "rb") as f:
        for ft in ijson.items(f, "features.item", use_float=True):
            c = ft["geometry"]["coordinates"]
            if ft["geometry"]["type"] == "Point":
                nodes[ft["properties"]["_id"]] = ft
            elif ft["geometry"]["type"] == "Polygon":      # a Pedestrian Zone
                if any(contains_xy(area, x, y) for x, y, *_ in c[0]):
                    zones.append(ft)
                    need.update(ft["properties"].get("_w_id") or [])
            elif any(contains_xy(area, x, y) for x, y, *_ in c):
                edges.append(ft)
                need.update((ft["properties"]["_u_id"], ft["properties"]["_v_id"]))
    for nid, ft in nodes.items():
        if nid not in need and contains_xy(area, *ft["geometry"]["coordinates"][:2]):
            loose.append(ft)
    kept = [nodes[n] for n in sorted(need)] + loose
    with open(osw, "rb") as f:
        root = {k: v for k, v in ijson.kvitems(f, "", use_float=True) if k != "features"}
    # The schema wants a MultiPolygon, and the validator at most 7 decimals.
    ring = [[round(x, 7), round(y, 7)] for x, y in area.exterior.coords]
    root["region"] = {"type": "MultiPolygon", "coordinates": [[ring]]}
    prefix = Path(prefix)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    with open(f"{prefix}.geojson", "w") as f:
        json.dump({**root, "features": kept + edges + zones}, f)
    with zipfile.ZipFile(f"{prefix}-osw-split.zip", "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{prefix.name}.nodes.geojson", json.dumps({**root, "features": kept}))
        z.writestr(f"{prefix.name}.edges.geojson", json.dumps({**root, "features": edges}))
        if zones:
            z.writestr(f"{prefix.name}.zones.geojson", json.dumps({**root, "features": zones}))
    print(f"{len(kept):,} nodes ({len(loose):,} on no kept edge), {len(edges):,} edges, {len(zones):,} zones")


if __name__ == "__main__":
    clip(*sys.argv[1:5])
