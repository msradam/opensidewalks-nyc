"""Ask a local OpenRouteService for every pair under a set of configurations.

Never a public API: the base URL must be on this machine.

Configurations:
  foot                      foot-walking, shortest
  foot_rec                  foot-walking as ORS ships it (recommended weighting)
  default                   wheelchair with no restrictions given. ORS then
                            applies no kerb, incline or width limit at all
                            (ors_tag_probe.json); only its weighting differs
                            from foot.
  rec_i{6,10}_k6            wheelchair, recommended weighting, with
                            maximum_incline and maximum_sloped_kerb 0.06 m.
                            6% and 0.06 m are the defaults ORS documents;
                            these are the routes a user of ORS would see.
  i{6,10}_k{3,6}[_w90]      the same limits with preference "shortest", which
                            matches this graph's shortest-passable-path
                            search, and with _w90 minimum_width 0.9 m. Whether
                            a route is found does not depend on the weighting.

usage: python compare/run_ors.py BASE_URL PAIRS_JSON OUT_JSONL_GZ [--only CONFIG,CONFIG]
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


def configs():
    out = {"foot": ("foot-walking", {"preference": "shortest"}),
           "foot_rec": ("foot-walking", {}),
           "default": ("wheelchair", {})}
    for inc in (6, 10):
        out[f"rec_i{inc}_k6"] = ("wheelchair", {"options": {"profile_params": {"restrictions": {"maximum_incline": inc, "maximum_sloped_kerb": 0.06}}}})
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
            # The OSM ways the route uses (the wheelchair profile stores them; foot-walking does not).
            "osmid": sorted({int(v[2]) for v in f["properties"].get("extras", {}).get("osmId", {}).get("values", [])}) or None}


def main(base, pairs_json, out, only=None, threads=6):
    assert base.startswith(("http://localhost", "http://127.0.0.1")), "local engines only"
    pairs = read_json(pairs_json)["pairs"]
    cfg = configs()
    jobs = [(p, name) for p in pairs for name in cfg if only is None or name in only]
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
    main(a[0], a[1], a[2], a[a.index("--only") + 1].split(",") if "--only" in a else None)
