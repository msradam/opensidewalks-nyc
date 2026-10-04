"""Ferry use in the router comparison, and the pairs no walking route can serve.

This graph has no ferry edges. A pair with one end on Governors, Liberty or
Ellis Island, or with one end on Staten Island and the other elsewhere in the
city, has no route here, while ORS and Valhalla on plain OSM take a
`route=ferry` way. Such pairs are called "across water" below: the two ends
lie on different ones of Governors Island, Liberty Island, Ellis Island,
Staten Island and the rest of the city. From the archived routes this gives:

  pairs_across_water   per area, how many pairs, by which lands the ends are on
  ferry_use            per router that returns OSM way ids and per area: routes
                       found, routes using a `route=ferry` way, and how many of
                       those are on pairs that are not across water
  found                per table row and area: routes found over all pairs and
                       with the pairs across water set apart
  agreement            the two by two table against this graph's wheelchair
                       profile on measured pairs, all and with the pairs across
                       water set apart
  arm_a_only           per ORS setting: pairs ORS finds on plain OSM (arm A) and
                       not on this graph converted to OSM (arm B strict), how
                       many of those arm A routes used a ferry, and how many
                       used some other way this graph does not have

usage: python ferries.py --routes-dir DIR --pbf CLIP.osm.pbf --pairs pairs.json
           --detail pairs_detail.jsonl.gz --graph nyc.npz --out ferries.json [--tables ferries_tables.md]
"""

import argparse
import gzip
import json
import shlex
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import osmium

REST = "rest of the city"
# lon_min, lon_max, lat_min, lat_max. Pair ends are graph nodes inside the city, so a box need only tell the lands apart.
LANDS = {
    "Governors Island": (-74.03, -74.008, 40.684, 40.6965),
    "Liberty Island": (-74.05, -74.03, 40.685, 40.694),
    "Ellis Island": (-74.05, -74.03, 40.694, 40.703),
    "Staten Island": (-74.30, -74.048, 40.45, 40.652),
}
# routers whose routes carry OSM way ids: (route file, config)
WAY_IDS = {
    "ors_a_rec_i10_k6": ("ors_armA.with_way_ids.jsonl.gz", "rec_i10_k6"),
    "ors_a_no_limits": ("ors_armA.with_way_ids.jsonl.gz", "default"),
    "valhalla_wheelchair": ("valhalla.with_way_ids.jsonl.gz", "wheelchair"),
    "valhalla_foot": ("valhalla.with_way_ids.jsonl.gz", "foot"),
}
# arm A config: (arm A file, arm B strict file)
ARM_PAIRS = {
    "default": ("ors_armA.with_way_ids.jsonl.gz", "ors_armB_strict.jsonl.gz"),
    "rec_i10_k6": ("ors_armA.with_way_ids.jsonl.gz", "ors_armB_strict.rec.jsonl.gz"),
    "foot_rec": ("ors_armA.rec.jsonl.gz", "ors_armB_strict.rec.jsonl.gz"),
}
# the rows of tables.md: (label, key in pairs_detail)
ROWS = [
    ("this graph, wheelchair", "ours_wheelchair"),
    ("ORS raw OSM, incline 10 kerb 0.06", "ors_a_rec_i10_k6"),
    ("ORS raw OSM, incline 6 kerb 0.06", "ors_a_rec_i6_k6"),
    ("ORS raw OSM, no limits given", "ors_a_no_limits"),
    ("ORS on this graph, strict kerbs, 10/0.06", "ors_b_strict_rec_i10_k6"),
    ("ORS on this graph, strict kerbs, 6/0.06", "ors_b_strict_rec_i6_k6"),
    ("ORS on this graph, known kerbs only, 10/0.06", "ors_b_known_rec_i10_k6"),
    ("Valhalla wheelchair type", "valhalla_wheelchair"),
    ("this graph, walk", "ours_walk"),
    ("ORS raw OSM, foot", "ors_a_foot"),
    ("Valhalla foot", "valhalla_foot"),
]
AREAS = ("BK", "QN", "MN", "BX", "SI", "citywide", "brownsville")
SNAP_FAR_M = 25.0
# tables.md as generated on 2026-10-04, to show this script reads the same data
EXPECT_FOUND = {
    ("ours_wheelchair", "BK"): 0.912,
    ("ours_wheelchair", "MN"): 0.640,
    ("ors_a_rec_i10_k6", "MN"): 0.977,
    ("valhalla_wheelchair", "MN"): 0.942,
}
EXPECT_AGREE = {
    ("ors_a_rec_i10_k6", "MN"): [1256, 15, 633, 25],
    ("valhalla_wheelchair", "citywide"): [887, 2, 960, 39],
}


def land(lonlat):
    lon, lat = lonlat
    return next(
        (
            name
            for name, (x0, x1, y0, y1) in LANDS.items()
            if x0 < lon < x1 and y0 < lat < y1
        ),
        REST,
    )


def rows(path, configs=None):
    with gzip.open(path, "rt") as f:
        for row in f:
            r = json.loads(row)
            if configs is None or r["config"] in configs:
                yield r


class Ways(osmium.SimpleHandler):
    """Ferry way ids, and the tags of ways this graph does not have."""

    def __init__(self, in_graph):
        super().__init__()
        self.in_graph, self.ferry, self.absent = in_graph, set(), {}

    def way(self, w):
        if w.tags.get("route") == "ferry":
            self.ferry.add(w.id)
        if w.id not in self.in_graph and ("highway" in w.tags or "route" in w.tags):
            self.absent[w.id] = {
                k: w.tags[k] for k in ("highway", "route") if k in w.tags
            }


def share(num, den):
    return round(num / den, 4) if den else None


def markdown(out):
    pct = lambda x: "n/a" if x is None else f"{100 * x:.1f}%"
    head = (
        "| | "
        + " | ".join("Brownsville" if a == "brownsville" else a for a in AREAS)
        + " |\n|---|"
        + "---|" * len(AREAS)
    )
    line = lambda label, fn: f"| {label} | " + " | ".join(fn(a) for a in AREAS) + " |"
    label = {k: n for n, k in ROWS}
    t = [
        "### Pairs across water\n",
        head,
        line("pairs", lambda a: str(out["pairs_across_water"][a]["pairs"])),
        line(
            "across water", lambda a: str(out["pairs_across_water"][a]["across_water"])
        ),
        line(
            "of those, with an end on Governors, Liberty or Ellis Island",
            lambda a: str(
                out["pairs_across_water"][a]["one_end_on_governors_liberty_or_ellis"]
            ),
        ),
        line(
            "of those, with an end on Staten Island",
            lambda a: str(out["pairs_across_water"][a]["one_end_on_staten_island"]),
        ),
        "\n### Routes using a ferry: routes (share of the router's routes found; of them, on pairs not across water)\n",
        head,
    ]
    for k in WAY_IDS:
        t.append(
            line(
                label[k],
                lambda a, k=k: (
                    "{using_a_ferry} ({0}; {using_a_ferry_on_pairs_not_across_water})".format(
                        pct(out["ferry_use"][k][a]["share"]), **out["ferry_use"][k][a]
                    )
                ),
            )
        )
    t += ["\n### Route found, all pairs, with the pairs across water set apart\n", head]
    for n, k in ROWS:
        t.append(
            line(
                n,
                lambda a, k=k: pct(
                    out["found"][k][a]["share_without_pairs_across_water"]
                ),
            )
        )
    t += [
        "\n### Found by both / this graph only / the other only / neither (measured pairs, pairs across water set apart)\n",
        head,
    ]
    for n, k in ROWS[1:8]:
        t.append(
            line(
                n,
                lambda a, k=k: " / ".join(
                    str(x) for x in out["agreement"][k][a]["without_pairs_across_water"]
                ),
            )
        )
    return "\n".join(t) + "\n"


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--routes-dir", required=True, type=Path)
    ap.add_argument(
        "--pbf",
        required=True,
        type=Path,
        help="the pinned OSM clip the arm A engines were built from",
    )
    ap.add_argument("--pairs", required=True, type=Path)
    ap.add_argument(
        "--detail",
        required=True,
        type=Path,
        help="pairs_detail.jsonl.gz from compare/analyse.py",
    )
    ap.add_argument(
        "--graph",
        required=True,
        type=Path,
        help="this graph as arrays (compare/graph.py); only osm_id is read",
    )
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument(
        "--tables", type=Path, help="write the Markdown tables that tables.md carries"
    )
    a = ap.parse_args()

    pairs = [
        p for p in json.loads(a.pairs.read_text())["pairs"] if p["set"] != "landmark"
    ]
    area = {
        p["id"]: ("brownsville" if p["set"] == "brownsville" else p["area"])
        for p in pairs
    }
    ends = {p["id"]: (land(p["o"]), land(p["d"])) for p in pairs}
    across = {i for i, (o, d) in ends.items() if o != d}
    ids_of = {ar: [i for i, x in area.items() if x == ar] for ar in AREAS}
    checks = []
    # the Staten Island box must hold every end of the Staten Island set and no end of the other borough sets
    for ar in ("BK", "QN", "MN", "BX", "SI"):
        n = sum(
            (o == "Staten Island") + (d == "Staten Island")
            for o, d in (ends[i] for i in ids_of[ar])
        )
        checks.append(
            {
                "check": f"ends of the {ar} set inside the Staten Island box",
                "expected": 4000 if ar == "SI" else 0,
                "computed": n,
                "match": n == (4000 if ar == "SI" else 0),
            }
        )

    pairs_across = {}
    for ar in AREAS:
        c = Counter(
            " / ".join(sorted(ends[i])) for i in ids_of[ar] if ends[i] != (REST, REST)
        )
        x = [ends[i] for i in ids_of[ar] if i in across]
        pairs_across[ar] = {
            "pairs": len(ids_of[ar]),
            "across_water": len(x),
            "one_end_on_governors_liberty_or_ellis": sum(
                any(e not in (REST, "Staten Island") for e in p) for p in x
            ),
            "one_end_on_staten_island": sum("Staten Island" in p for p in x),
            "pairs_with_an_end_off_the_rest_of_the_city_by_ends": dict(c.most_common()),
        }

    in_graph = set(np.load(a.graph)["osm_id"].tolist()) - {-1}
    h = Ways(in_graph)
    h.apply_file(str(a.pbf))
    print(
        f"{len(h.ferry)} ferry ways, {len(h.absent)} highway/route ways absent from this graph",
        file=sys.stderr,
    )

    ferry_use = {
        k: {
            ar: Counter(
                found=0, using_a_ferry=0, using_a_ferry_on_pairs_not_across_water=0
            )
            for ar in AREAS
        }
        for k in WAY_IDS
    }
    arm_a = {}  # (config, id) -> (found, used a ferry, non-ferry way ids this graph lacks)
    for key, (fname, cfg) in WAY_IDS.items():
        for r in rows(a.routes_dir / fname, {cfg}):
            ar = area.get(r["id"])
            ids = set(r.get("osmid") or [])
            ferry = bool(ids & h.ferry)
            if fname.startswith("ors_armA"):
                arm_a[(cfg, r["id"])] = (r["found"], ferry, ids - in_graph - h.ferry)
            if ar and r["found"]:
                c = ferry_use[key][ar]
                c["found"] += 1
                c["using_a_ferry"] += ferry
                c["using_a_ferry_on_pairs_not_across_water"] += (
                    ferry and r["id"] not in across
                )
    ferry_use = {
        k: {
            ar: {**c, "share": share(c["using_a_ferry"], c["found"])}
            for ar, c in v.items()
        }
        for k, v in ferry_use.items()
    }
    for r in rows(a.routes_dir / ARM_PAIRS["foot_rec"][0], {"foot_rec"}):
        arm_a[("foot_rec", r["id"])] = (r["found"], None, set())

    got, measured = {k: {} for _, k in ROWS}, set()
    for r in rows(a.detail):
        if r["id"] not in area:
            continue
        if r["snap_apart_m"] <= SNAP_FAR_M:
            measured.add(r["id"])
        for _, k in ROWS:
            got[k][r["id"]] = bool(r["routes"].get(k, {}).get("found"))
    found, agreement = {}, {}
    for _, k in ROWS:
        found[k], agreement[k] = {}, {}
        for ar in AREAS:
            ids = ids_of[ar]
            keep = [i for i in ids if i not in across]
            found[k][ar] = {
                "pairs": len(ids),
                "found": sum(got[k][i] for i in ids),
                "share_all_pairs": share(sum(got[k][i] for i in ids), len(ids)),
                "pairs_without_pairs_across_water": len(keep),
                "found_without_pairs_across_water": sum(got[k][i] for i in keep),
                "share_without_pairs_across_water": share(
                    sum(got[k][i] for i in keep), len(keep)
                ),
                "found_on_pairs_across_water": sum(
                    got[k][i] for i in ids if i in across
                ),
            }
            two = lambda ids, k=k: [
                sum(got["ours_wheelchair"][i] == x and got[k][i] == y for i in ids)
                for x, y in ((True, True), (True, False), (False, True), (False, False))
            ]
            agreement[k][ar] = {
                "order": "both / this graph only / the other only / neither",
                "measured_pairs": two([i for i in ids if i in measured]),
                "without_pairs_across_water": two([i for i in keep if i in measured]),
            }
            if (k, ar) in EXPECT_FOUND:
                v = found[k][ar]["share_all_pairs"]
                checks.append(
                    {
                        "check": f"route found, all pairs, {k}, {ar}",
                        "expected": EXPECT_FOUND[(k, ar)],
                        "computed": v,
                        "match": abs(v - EXPECT_FOUND[(k, ar)]) < 0.0005,
                    }
                )
            if (k, ar) in EXPECT_AGREE:
                v = agreement[k][ar]["measured_pairs"]
                checks.append(
                    {
                        "check": f"found by both table, {k}, {ar}",
                        "expected": EXPECT_AGREE[(k, ar)],
                        "computed": v,
                        "match": v == EXPECT_AGREE[(k, ar)],
                    }
                )
    for c in checks:
        print("check", c, file=sys.stderr)
    assert all(c["match"] for c in checks)

    arm_b = {}
    for cfg, (_, bfile) in ARM_PAIRS.items():
        for r in rows(a.routes_dir / bfile, {cfg}):
            arm_b[(cfg, r["id"])] = r["found"]
    a_only = {}
    for cfg in ARM_PAIRS:
        a_only[cfg] = {}
        for ar in AREAS:
            ids = ids_of[ar]
            only = [i for i in ids if arm_a[(cfg, i)][0] and not arm_b[(cfg, i)]]
            c = {
                "found_arm_a": sum(arm_a[(cfg, i)][0] for i in ids),
                "found_arm_b_strict": sum(arm_b[(cfg, i)] for i in ids),
                "arm_a_only": len(only),
                "arm_a_only_on_pairs_across_water": sum(i in across for i in only),
            }
            if cfg == "foot_rec":
                c["note"] = (
                    "foot_rec routes carry no way ids, so ferry use cannot be read from them"
                )
            else:
                c["arm_a_route_used_a_ferry"] = sum(arm_a[(cfg, i)][1] for i in only)
                rest = [i for i in only if not arm_a[(cfg, i)][1]]
                absent = [i for i in rest if arm_a[(cfg, i)][2]]
                c["arm_a_route_used_no_ferry_but_a_way_this_graph_lacks"] = len(absent)
                c["arm_a_route_used_only_ways_this_graph_has"] = len(rest) - len(absent)
                tags = Counter()
                for i in absent:
                    for w in arm_a[(cfg, i)][2]:
                        t = h.absent.get(w, {})
                        tags[
                            t.get("highway")
                            or (
                                "route=" + t["route"]
                                if "route" in t
                                else "not a highway or route way"
                            )
                        ] += 1
                c["highway_tags_of_those_ways_counted_per_route_and_way"] = dict(
                    tags.most_common()
                )
            a_only[cfg][ar] = c

    out = {
        "_meta": {
            "date": "2026-10-04",
            "code": "evaluation/compare/results/code/ferries.py",
            "command": shlex.join(["python", *sys.argv]),
            "inputs": {
                "routes": {
                    k: f"{a.routes_dir / v[0]} config {v[1]}"
                    for k, v in WAY_IDS.items()
                },
                "arm_b_strict": {
                    cfg: str(a.routes_dir / v[1]) for cfg, v in ARM_PAIRS.items()
                },
                "pairs": str(a.pairs),
                "detail": str(a.detail),
                "graph": str(a.graph),
                "osm": str(a.pbf),
                "note": "the routes, pairs and graph arrays are in the project's working archive, which is not published; the OSM clip is made as engines/README.md says",
            },
            "what": {
                "pairs_across_water": "pairs whose two ends lie on different lands (boxes below). This graph has no ferry edge and no walkway joins these lands inside the city, so it can route none of them",
                "ferry_use": "per router and area: routes found, routes using at least one OSM way tagged route=ferry (by the way ids the router returned), the share, and how many of those are on pairs that are not across water (the router chose a ferry where a walking route may exist)",
                "found": "routes found over all pairs, and with the pairs across water removed from numerator and denominator; found or not is read from pairs_detail.jsonl.gz",
                "agreement": "against this graph's wheelchair profile on measured pairs (snapped ends within 25 m across routers), all and with the pairs across water removed",
                "arm_a_only": "per ORS setting: pairs found on arm A (plain OSM) and not on arm B strict (this graph as OSM); of those, arm A routes that used a ferry, routes that used no ferry but a way that is in the clip and not in this graph (an upper bound on what the pipeline's tag filter explains, since such a way may also lie in New Jersey or be a motorway), and routes using only ways this graph has",
                "checks": "this script's numbers against tables.md, and the Staten Island box against the borough sets",
            },
            "areas": "random pairs by borough and citywide (2,000 each), brownsville (420 trips); the 17 landmark pairs are left out",
            "land_boxes_lon_min_lon_max_lat_min_lat_max": LANDS,
            "ferry_ways_in_clip": len(h.ferry),
        },
        "pairs_across_water": pairs_across,
        "ferry_use": ferry_use,
        "found": found,
        "agreement": agreement,
        "arm_a_only": a_only,
        "checks": checks,
    }
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1)
    if a.tables:
        a.tables.write_text(markdown(out))
    print(f"wrote {a.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
