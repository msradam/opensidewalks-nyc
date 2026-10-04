"""Route every pair with Valhalla (pyvalhalla, in process, no service).

Run in its own environment, since the project does not depend on Valhalla:
  uv run --no-project --isolated --python 3.12 --with pyvalhalla==3.9.0 \
      python compare/run_valhalla.py build PBF TILE_DIR
  uv run ... python compare/run_valhalla.py route TILE_DIR PAIRS_JSON OUT_JSONL_GZ

Configurations:
  foot         pedestrian costing as shipped: the engine's plain foot route
  wheelchair   pedestrian costing, type wheelchair, use_hills 0, with the
               10 km wheelchair distance cap raised. The tiles are built with
               no elevation tiles, so every edge has zero grade and use_hills
               acts on nothing. sidewalk_factor is left at its default of 1.0,
               so a sidewalk is not preferred over a centreline. No kerb option
               exists, the grade limit is not enforced, and steps are
               penalised (600 s), not forbidden, so this is a stair-avoiding
               foot baseline, not a wheelchair router.
"""
import gzip
import json
import math
import subprocess
import sys
import time
from pathlib import Path

EAST, NORTH = 111320 * math.cos(math.radians(40.7)), 111320.0
CONFIGS = {
    "foot": {"pedestrian": {}},
    "wheelchair": {"pedestrian": {"type": "wheelchair", "use_hills": 0, "max_distance": 200000}},
}


def build(pbf, tile_dir):
    from valhalla import get_config
    tile_dir = Path(tile_dir)
    (tile_dir / "tiles").mkdir(parents=True, exist_ok=True)     # get_config wants it to exist
    cfg = get_config(tile_extract="", tile_dir=str(tile_dir / "tiles"))
    cfg["service_limits"]["pedestrian"]["max_distance"] = 250000
    (tile_dir / "valhalla.json").write_text(json.dumps(cfg, indent=1))
    t = time.time()
    subprocess.run([sys.executable, "-m", "valhalla", "valhalla_build_tiles", "-c", str(tile_dir / "valhalla.json"), str(pbf)], check=True)
    (tile_dir / "build.json").write_text(json.dumps({"seconds": round(time.time() - t), "pbf": str(pbf)}))


def decode(shape, precision=6):
    out, i, lat, lon = [], 0, 0, 0
    while i < len(shape):
        for k in (0, 1):
            shift = result = 0
            while True:
                b = ord(shape[i]) - 63
                i += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            d = ~(result >> 1) if result & 1 else result >> 1
            if k == 0:
                lat += d
            else:
                lon += d
        out.append([round(lon / 10 ** precision, 6), round(lat / 10 ** precision, 6)])
    return out


def route(tile_dir, pairs_json, out):
    from valhalla import Actor
    actor = Actor(str(Path(tile_dir) / "valhalla.json"))
    with open(pairs_json) as f:
        pairs = json.load(f)["pairs"]
    snap = lambda a, b: round(math.hypot((a[0] - b[0]) * EAST, (a[1] - b[1]) * NORTH), 1)
    with gzip.open(out, "wt") as f:
        for n, p in enumerate(pairs):
            for name, opts in CONFIGS.items():
                q = {"locations": [{"lon": p["o"][0], "lat": p["o"][1]}, {"lon": p["d"][0], "lat": p["d"][1]}],
                     "costing": "pedestrian", "costing_options": opts, "units": "kilometers", "directions_type": "none"}
                try:
                    leg = actor.route(q)["trip"]["legs"][0]
                    c = decode(leg["shape"])
                    r = {"found": True, "length_m": round(leg["summary"]["length"] * 1000, 1),
                         "snap_m": [snap(c[0], p["o"]), snap(c[-1], p["d"])], "coords": c}
                except RuntimeError as e:      # Valhalla raises on "no path" and "no edge near location"
                    r = {"found": False, "error": str(e)[:160]}
                f.write(json.dumps({"id": p["id"], "config": name, **r}) + "\n")
            if n % 2000 == 0:
                print(n, len(pairs), flush=True)


if __name__ == "__main__":
    {"build": build, "route": route}[sys.argv[1]](*sys.argv[2:])
