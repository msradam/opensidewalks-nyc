"""End-to-end Unweaver routing tests against the OSW NYC graph.

Picks endpoints near real NYC landmarks and snaps each to its nearest node
in the giant connected component before querying. Without this, queries can
fall outside Unweaver's 30 m candidate-edge search radius.

Usage:
    python scripts/route_test.py --base http://127.0.0.1:5000 \\
        --osw output/nyc-osw.geojson \\
        --md  validators/route_test_results.md
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

import networkx as nx
import requests


# (name, lat, lon)
LANDMARKS = [
    ("Penn Station",          40.7506, -73.9935),
    ("Grand Central",         40.7527, -73.9772),
    ("Times Square",          40.7580, -73.9855),
    ("Empire State Building", 40.7484, -73.9857),
    ("Union Square",          40.7359, -73.9911),
    ("Washington Sq Park",    40.7308, -73.9973),
    ("Brooklyn Bridge MN",    40.7115, -74.0028),
    ("DUMBO",                 40.7033, -73.9888),
    ("Atlantic Av-Barclays",  40.6840, -73.9778),
    ("Prospect Park",         40.6602, -73.9690),
    ("Williamsburg Bridge MN",40.7155, -73.9810),
    ("Williamsburg Bridge BK",40.7128, -73.9650),
    ("Court Sq Queens",       40.7470, -73.9445),
    ("LIC Hunters Pt",        40.7424, -73.9534),
    ("Yankee Stadium",        40.8296, -73.9262),
    ("161 St-Yankee Stadium", 40.8275, -73.9282),
]

# (origin_idx, destination_idx, label)
ROUTES = [
    (0, 1, "Penn Station -> Grand Central"),
    (2, 3, "Times Square -> Empire State Building"),
    (4, 5, "Union Square -> Washington Sq Park"),
    (6, 7, "Brooklyn Bridge MN -> DUMBO"),
    (8, 9, "Atlantic Av-Barclays -> Prospect Park"),
    (10, 11, "Williamsburg Bridge MN -> Williamsburg Bridge BK"),
    (12, 13, "Court Sq Queens -> LIC Hunters Point"),
    (14, 15, "Yankee Stadium -> 161 St"),
    (3, 7,  "Empire State Building -> DUMBO (cross-borough)"),
    (1, 5,  "Grand Central -> Washington Sq Park (long Manhattan)"),
]


def hav(a, b):
    R = 6371000
    la1, la2 = math.radians(a[1]), math.radians(b[1])
    dla = la2 - la1; dlo = math.radians(b[0] - a[0])
    h = (math.sin(dla / 2) ** 2 +
         math.cos(la1) * math.cos(la2) * math.sin(dlo / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(h))


def snap_landmarks(osw_path: Path):
    """Return per-landmark dict with snapped coords and snap distance."""
    print(f"snapping landmarks via {osw_path.name}...")
    fc = json.loads(osw_path.read_text())
    coords_by_id = {}
    for f in fc["features"]:
        if (f.get("geometry") or {}).get("type") != "Point":
            continue
        p = f.get("properties") or {}
        nid = p.get("_id")
        c = (f.get("geometry") or {}).get("coordinates")
        if nid and c:
            coords_by_id[nid] = (float(c[0]), float(c[1]))

    G = nx.Graph()
    for f in fc["features"]:
        if (f.get("geometry") or {}).get("type") != "LineString":
            continue
        p = f.get("properties") or {}
        u, v = p.get("_u_id"), p.get("_v_id")
        if u and v and u != v:
            G.add_edge(u, v)
    giant = max(nx.connected_components(G), key=len)
    print(f"  giant component: {len(giant):,} nodes")

    giant_coords = [(nid, coords_by_id[nid]) for nid in giant
                    if nid in coords_by_id]

    snapped = []
    for name, lat, lon in LANDMARKS:
        target = (lon, lat)
        best = None
        for nid, npos in giant_coords:
            d = hav(target, npos)
            if best is None or d < best[1]:
                best = (nid, d, npos)
        snapped.append({
            "name": name,
            "query_lat": lat, "query_lon": lon,
            "snap_node_id": best[0],
            "snap_dist_m": round(best[1], 1),
            "lat": best[2][1], "lon": best[2][0],
        })
        print(f"  {name:<28} -> {best[0]} ({best[1]:.0f}m)")
    return snapped


def query(base, profile, lat1, lon1, lat2, lon2, **extras):
    url = f"{base}/shortest_path/{profile}.json"
    params = {"lon1": lon1, "lat1": lat1, "lon2": lon2, "lat2": lat2,
              **{k: v for k, v in extras.items() if v is not None}}
    t0 = time.time()
    try:
        r = requests.get(url, params=params, timeout=120)
        return r.status_code, r.json(), round(time.time() - t0, 2)
    except Exception as e:
        return -1, {"err": str(e)[:200]}, round(time.time() - t0, 2)


def summarize(body):
    if not isinstance(body, dict):
        return {"status": "error", "raw": str(body)[:120]}
    if "code" in body:
        return {"status": body["code"]}
    edges = body.get("edges") or []
    path  = body.get("path")  or []
    out = {"status": "Ok"}
    if edges:
        out["edges"] = len(edges)
        out["total_length_m"] = round(sum((e or {}).get("length", 0) or 0 for e in edges), 1)
    elif path:
        # The example wheelchair shortest_path returns just {"status":"Ok","path":[node ids]}.
        # Report node count.
        out["path_nodes"] = len(path) if isinstance(path, list) else "?"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:5000")
    ap.add_argument("--osw",  type=Path,
                    default=Path("output/nyc-osw.geojson"))
    ap.add_argument("--out",  type=Path,
                    default=Path("validators/route_test_results.json"))
    ap.add_argument("--md",   type=Path,
                    default=Path("validators/route_test_results.md"))
    args = ap.parse_args()

    snapped = snap_landmarks(args.osw)

    md = ["# Unweaver routing test results", "",
          f"Base URL: `{args.base}`",
          f"Routes tested: {len(ROUTES)} pairs × 2 profiles = {len(ROUTES)*2}",
          "",
          "## Snapped landmarks (giant component)", "",
          "| Landmark | Snap node | Snap distance |",
          "|---|---|---|"]
    for s in snapped:
        md.append(f"| {s['name']} | `{s['snap_node_id']}` | "
                  f"{s['snap_dist_m']} m |")

    md += ["", "## Per-route results", "",
           "| Route | Profile | Status | Elapsed | Edges | Length (m) |",
           "|---|---|---|---|---|---|"]

    results = []
    n_ok_per_profile = {"distance": 0, "wheelchair": 0}

    for i, j, label in ROUTES:
        a = snapped[i]; b = snapped[j]
        for profile, extras in [
            ("distance",   {}),
            ("wheelchair", {"avoidCurbs": "true",
                            "uphill": "0.083",
                            "downhill": "-0.1"}),
        ]:
            sc, body, elapsed = query(args.base, profile,
                                       a["lat"], a["lon"],
                                       b["lat"], b["lon"], **extras)
            s = summarize(body)
            if s.get("status") == "Ok":
                n_ok_per_profile[profile] += 1
            results.append({"route": label, "profile": profile,
                            "from": a["name"], "to": b["name"],
                            "status": s.get("status"),
                            "edges": s.get("edges"),
                            "total_length_m": s.get("total_length_m"),
                            "elapsed_s": elapsed,
                            "snap_dist_origin_m": a["snap_dist_m"],
                            "snap_dist_dest_m":  b["snap_dist_m"]})
            md.append(f"| {label} | {profile} | {s.get('status')} | "
                      f"{elapsed}s | {s.get('edges','—')} | "
                      f"{s.get('total_length_m','—')} |")
            print(f"  [{profile:>10}] {label}  ->  {s}")

    md += ["",
           f"## Summary",
           "",
           f"- distance profile: **{n_ok_per_profile['distance']} / {len(ROUTES)}** OK",
           f"- wheelchair profile: **{n_ok_per_profile['wheelchair']} / {len(ROUTES)}** OK"]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(results, indent=2))
    args.md.write_text("\n".join(md))
    print()
    print(f"distance: {n_ok_per_profile['distance']}/{len(ROUTES)}")
    print(f"wheelchair: {n_ok_per_profile['wheelchair']}/{len(ROUTES)}")


if __name__ == "__main__":
    main()
