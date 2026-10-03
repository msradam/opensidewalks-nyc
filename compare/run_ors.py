"""Ask a local OpenRouteService for every pair under a set of configurations.

Never a public API: the base URL must be on this machine.

Configurations (all with preference "shortest", to match this graph's
shortest-passable-path search, except "default"):
  foot                      foot-walking, the engine's plain foot route
  default                   wheelchair as ORS ships it (recommended, 6%, 0.06 m)
  i{6,10}_k{3,6}[_w90]      wheelchair with maximum_incline, maximum_sloped_kerb
                            (cm) and, with _w90, minimum_width 0.9 m

usage: python compare/run_ors.py BASE_URL PAIRS_JSON OUT_JSONL_GZ [--full-matrix-per-area N]
"""
import gzip
import json
import math
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare.graph import EAST, NORTH, read_json

PRIMARY = ("foot", "default", "i10_k6", "i6_k3")


def configs():
    out = {"foot": ("foot-walking", {"preference": "shortest"}),
           "default": ("wheelchair", {})}
    for inc in (6, 10):
        for kerb in (3, 6):
            for width in (None, 0.9):
                r = {"maximum_incline": inc, "maximum_sloped_kerb": kerb / 100}
                if width:
                    r["minimum_width"] = width
                name = f"i{inc}_k{kerb}" + ("_w90" if width else "")
                out[name] = ("wheelchair", {"preference": "shortest", "options": {"profile_params": {"restrictions": r}}})
    return out


def ask(session, base, profile, body, o, d):
    body = {"coordinates": [o, d], "instructions": False, "extra_info": ["osmid"], **body}
    try:
        r = session.post(f"{base}/ors/v2/directions/{profile}/geojson", json=body, timeout=120)
        j = r.json()
    except (requests.RequestException, ValueError) as e:      # a dropped connection is recorded, not retried silently
        return {"found": False, "error": f"client: {str(e)[:120]}"}
    if "features" not in j:
        err = j.get("error", {})
        return {"found": False, "error": err.get("code") if isinstance(err, dict) else str(err)[:120],
                "message": (err.get("message") if isinstance(err, dict) else "")[:160]}
    f = j["features"][0]
    c = f["geometry"]["coordinates"]
    snap = lambda a, b: round(math.hypot((a[0] - b[0]) * EAST, (a[1] - b[1]) * NORTH), 1)
    return {"found": True, "length_m": f["properties"]["summary"].get("distance", 0.0),
            "snap_m": [snap(c[0], o), snap(c[-1], d)],
            "coords": [[round(x, 6), round(y, 6)] for x, y, *_ in c],
            "osmid": f["properties"].get("extras", {}).get("osmid", {}).get("values")}


def main(base, pairs_json, out, full_per_area=None, threads=6):
    assert base.startswith(("http://localhost", "http://127.0.0.1")), "local engines only"
    pairs = read_json(pairs_json)["pairs"]
    cfg = configs()
    seen = {}
    jobs = []
    for p in pairs:
        n = seen[p["area"]] = seen.get(p["area"], 0) + 1
        full = p["set"] != "random" or full_per_area is None or n <= full_per_area
        jobs += [(p, name) for name in cfg if full or name in PRIMARY]
    session = requests.Session()

    def run(job):
        p, name = job
        profile, body = cfg[name]
        return {"id": p["id"], "config": name, **ask(session, base, profile, body, p["o"], p["d"])}

    with ThreadPoolExecutor(threads) as ex, gzip.open(out, "wt") as f:
        for n, r in enumerate(ex.map(run, jobs)):
            f.write(json.dumps(r) + "\n")
            if n % 5000 == 0:
                print(n, len(jobs), flush=True)


if __name__ == "__main__":
    a = sys.argv[1:]
    full = int(a[a.index("--full-matrix-per-area") + 1]) if "--full-matrix-per-area" in a else None
    main(a[0], a[1], a[2], full)
