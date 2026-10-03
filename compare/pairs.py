"""The origin and destination pairs every engine is asked for.

Three sets, written to one JSON:
  random       the seeded pairs behind reach.json (same pools, same draws)
  landmark     the 17 pairs of scripts/route_test.py
  brownsville  origins at NYCHA developments, senior centres and subway
               entrances in Community District 316; destinations at cooling
               sites, clinics, public schools, libraries and subway elevators

usage: python compare/pairs.py GRAPH_NPZ BROWNSVILLE_RAW_DIR CD_GEOJSON OUT_JSON [PAIRS_PER_BOROUGH]
"""
import math
import sys
from pathlib import Path

import numpy as np
from shapely.geometry import Point, shape

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare.graph import EAST, NORTH, Graph, read_json, write_json
from scripts.route_test import LANDMARKS, ROUTES

SEED_RANDOM = 20261002      # research_notes/next/routing/reach.py
SEED_BROWNSVILLE = 20261003
ELEVATOR_REACH_M = 1000     # elevators this close to the district count as destinations


def random_pairs(g, per_borough):
    """Same pools and the same generator calls as reach.py, so pair i here is pair i there."""
    walkable = (g.kind != "street") & (g.kind != "steps")
    rng = np.random.default_rng(SEED_RANDOM)
    pools = {}
    for b in sorted(set(g.borough[walkable].tolist()) - {""}):
        e = walkable & (g.borough == b)
        pools[b] = np.unique(np.r_[g.u[e], g.v[e]])
    pools["citywide"] = np.unique(np.r_[g.u[walkable], g.v[walkable]])
    out = []
    for b, p in pools.items():
        ss, tt = rng.choice(p, 2000), rng.choice(p, 2000)
        for i, (s, t) in enumerate(zip(ss[:per_borough], tt[:per_borough])):
            out.append({"id": f"random-{b}-{i:04d}", "set": "random", "area": b, "o_node": int(s), "d_node": int(t),
                        "o": g.xy[s].round(7).tolist(), "d": g.xy[t].round(7).tolist()})
    return out


def landmark_pairs():
    out = []
    for i, (a, b, label) in enumerate(ROUTES):
        la, lb = LANDMARKS[a], LANDMARKS[b]
        out.append({"id": f"landmark-{i:02d}", "set": "landmark", "area": "structure" if i >= 10 or i in (3, 5) else "hand-checked",
                    "label": label, "o_name": la[0], "d_name": lb[0], "o": [la[2], la[1]], "d": [lb[2], lb[1]]})
    return out


def _metres(a, b):
    return math.hypot((a[0] - b[0]) * EAST, (a[1] - b[1]) * NORTH)


def brownsville_places(raw, cd_geojson):
    cd = next(shape(f["geometry"]) for f in read_json(cd_geojson)["features"] if f["properties"]["boro_cd"] == "316")
    inside = lambda xy: cd.contains(Point(xy))
    places = []

    def add(role, kind, name, xy, source):
        places.append({"role": role, "kind": kind, "name": " ".join(str(name).split()).title(), "xy": [round(xy[0], 7), round(xy[1], 7)], "source": source})

    for f in read_json(raw / "nycha_phvi-damg.geojson")["features"]:
        poly = shape(f["geometry"])
        if poly.intersects(cd) and inside(poly.representative_point().coords[0]):
            add("origin", "NYCHA development", f["properties"]["developmen"], poly.representative_point().coords[0], "phvi-damg")
    fac = read_json(raw / "facdb_cd316.json")
    for r in fac:
        xy = (float(r["longitude"]), float(r["latitude"]))
        if r["factype"] == "SENIOR CENTER":
            add("origin", "senior centre", r["facname"], xy, "ji82-xba5")
        elif r["facsubgrp"] == "HOSPITALS AND CLINICS":
            add("destination", "clinic", r["facname"], xy, "ji82-xba5")
        elif r["facsubgrp"] == "PUBLIC K-12 SCHOOLS":
            add("destination", "public school", r["facname"], xy, "ji82-xba5")
        elif r["factype"] == "PUBLIC LIBRARY":
            add("destination", "library", r["facname"], xy, "ji82-xba5")
    for r in read_json(raw / "cooling_h2bn-gu9k.json"):
        xy = (float(r["x"]), float(r["y"]))
        if inside(xy):
            add("destination", "cooling site", f'{r["propertyname"]} ({r["featuretype"]})', xy, "h2bn-gu9k")
    for r in read_json(raw / "entrances_i9wp-a4ja.json"):
        xy = (float(r["entrance_longitude"]), float(r["entrance_latitude"]))
        label = f'{r["stop_name"]} ({r["daytime_routes"]}) {r["entrance_type"]}'
        if inside(xy):
            add("origin", "subway entrance", label, xy, "i9wp-a4ja")
        if "Elevator" in r["entrance_type"] and cd.distance(Point(xy)) * NORTH <= ELEVATOR_REACH_M:
            add("destination", "subway elevator", label, xy, "i9wp-a4ja")
    # One place per name and kind: FacDB lists a school building once per programme.
    seen, uniq = set(), []
    for p in places:
        key = (p["role"], p["kind"], p["xy"][0], p["xy"][1])
        if key not in seen:
            seen.add(key)
            uniq.append(p)
    return uniq


def brownsville_pairs(places):
    """Each origin to the nearest destination of each kind, and to one other drawn at random."""
    rng = np.random.default_rng(SEED_BROWNSVILLE)
    origins = [p for p in places if p["role"] == "origin"]
    out = []
    for kind in sorted({p["kind"] for p in places if p["role"] == "destination"}):
        dests = [p for p in places if p["role"] == "destination" and p["kind"] == kind]
        for o in origins:
            order = sorted(range(len(dests)), key=lambda i: _metres(o["xy"], dests[i]["xy"]))
            order = [i for i in order if _metres(o["xy"], dests[i]["xy"]) > 50]
            picks = order[:1] + ([order[1 + int(rng.integers(len(order) - 1))]] if len(order) > 1 else [])
            for how, i in zip(("nearest", "random"), picks):
                d = dests[i]
                out.append({"id": f"brownsville-{len(out):04d}", "set": "brownsville", "area": "BK16", "pick": how,
                            "o_kind": o["kind"], "o_name": o["name"], "d_kind": d["kind"], "d_name": d["name"],
                            "o": o["xy"], "d": d["xy"]})
    return out


if __name__ == "__main__":
    npz, raw, cd, out = sys.argv[1], Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4])
    per = int(sys.argv[5]) if len(sys.argv) > 5 else 2000
    places = brownsville_places(raw, cd)
    pairs = random_pairs(Graph(npz), per) + landmark_pairs() + brownsville_pairs(places)
    write_json({"places": places, "pairs": pairs}, out)
    from collections import Counter
    print(Counter((p["set"], p["area"]) for p in pairs))
    print(Counter((p["role"], p["kind"]) for p in places))
