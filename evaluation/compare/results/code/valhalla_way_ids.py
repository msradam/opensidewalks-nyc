"""Redo Valhalla's audit rows by the OSM way ids Valhalla used (2026-10-04).

The first run matched Valhalla's routes to this graph's edges by geometry
alone. compare/run_valhalla.py now traces each route and stores its way ids.
This rereads the old pairs_detail.jsonl.gz, re-describes only the two
Valhalla routers through compare/analyse.py's own `match` and `describe`
(every other router and the verdicts depend on nothing Valhalla returns),
then reruns `tables`.

usage (from the repo root):
  uv run python evaluation/compare/results/code/valhalla_way_ids.py
"""
import gzip
import json
import shutil
import sys
from collections import Counter
from multiprocessing import get_context
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
import compare.analyse as an
from compare.graph import Graph, read_json, write_json
from compare.measures import Matcher, line

RN = ROOT / "research_notes/compare"
OUT = ROOT / "evaluation/compare/results"
KEYS = {"valhalla_foot": "foot", "valhalla_wheelchair": "wheelchair"}
FIELDS = ("with_steps", "with_over_10m_of_street_centreline", "with_an_edge_over_the_incline_limits",
          "with_a_crossing_without_a_surveyed_ramp", "with_any_of_these", "unramped_crossings_per_km", "median_share_off_this_graph")
NEW = {}


def redo(rec):
    out = {}
    for key, cfg in KEYS.items():
        r = NEW.get((rec["id"], cfg))
        if r is None or not r["found"]:
            continue
        geom = line(r["coords"])
        if geom is None or geom.length == 0:
            continue
        edges, unmatched = an.match("valhalla", r, geom)
        out[key] = {**an.describe(edges, unmatched), "length_m": round(r["length_m"], 1)}
    return rec["id"], out


def main():
    an.G = Graph(RN / "graph/nyc.npz")
    an.M = Matcher(an.G)
    an.TAGS = read_json(RN / "graph/osm_tags.json")
    old_rows, _ = an.load_engine(RN / "routes", "valhalla")       # newest file wins; read the first run's file alone below
    with gzip.open(RN / "routes/valhalla.jsonl.gz", "rt") as f:
        first = {(r["id"], r["config"]): r for r in map(json.loads, f)}
    with gzip.open(RN / "routes/valhalla.with_way_ids.jsonl.gz", "rt") as f:
        NEW.update({(r["id"], r["config"]): r for r in map(json.loads, f)})
    assert old_rows.keys() == NEW.keys() and all(old_rows[k] is NEW[k] or old_rows[k] == NEW[k] for k in NEW), "load_engine must read the way-id file"

    # Same routes as the first run?
    same = Counter((k[1], first[k]["found"] == r["found"] and first[k].get("coords") == r.get("coords")) for k, r in NEW.items())
    found = [(k, r) for k, r in NEW.items() if r["found"]]
    trace = {cfg: Counter(r.get("trace", "failed") for k, r in found if k[1] == cfg) for cfg in KEYS.values()}
    # Every found route must carry way ids, or the check below cannot find anything (lessons/a-check-that-finds-nothing...).
    with_ids = {cfg: sum(bool(r.get("osmid")) for k, r in found if k[1] == cfg) for cfg in KEYS.values()}
    gap = [r["length_m"] - r["traced_m"] for k, r in found if r.get("traced_m") is not None]
    short = [r for k, r in found if r.get("traced_m") is not None and r["length_m"] - r["traced_m"] > 50]

    # Keep the first run's outputs once, and always start from them.
    for name, kept in (("pairs_detail.jsonl.gz", "pairs_detail_geometry_matched_valhalla.jsonl.gz"), ("comparison.json", "comparison_geometry_matched_valhalla.json")):
        if not (RN / "results" / kept).exists():
            shutil.copy(RN / "results" / name, RN / "results" / kept)
    recs = [json.loads(x) for x in gzip.open(RN / "results/pairs_detail_geometry_matched_valhalla.jsonl.gz", "rt")]
    old_t = read_json(RN / "results/comparison_geometry_matched_valhalla.json")
    with get_context("fork").Pool(8) as pool:
        redone = dict(pool.imap(redo, recs, chunksize=50))
    changed = {key: Counter() for key in KEYS}
    for rec in recs:
        for key, new in redone[rec["id"]].items():
            changed[key].update(f for f in ("steps", "crossing_no_ramp", "over_incline", "street_m", "unmatched", "osm_flags") if rec["routes"][key][f] != new[f])
            changed[key]["routes"] += 1
            rec["routes"][key].update(new)
    matrix = {src: an.load_engine(RN / "routes", src)[1] for src in sorted({s for s, _, _ in an.ROUTERS.values()}) if src.startswith("ors")}
    t = an.tables(recs, matrix)

    # Nothing but the Valhalla audits may move.
    for area, row in t.items():
        for part, val in row.items():
            if part in ("audit_against_this_graph", "audit_against_raw_osm"):
                assert {k: v for k, v in val.items() if k not in KEYS} == {k: v for k, v in old_t[area][part].items() if k not in KEYS}, (area, part)
            else:
                assert val == old_t[area][part], (area, part)

    # Positive control and cross-check: Valhalla's own edge use against this graph's steps edges on the ways it named.
    by_id = {r["id"]: r for r in recs}
    cross = {}
    for cfg in KEYS.values():
        c = Counter()
        for (pid, k), r in found:
            if k != cfg or not r.get("osmid") or "valhalla_" + cfg not in by_id[pid]["routes"]:
                continue
            c[(r["edge_use_m"].get("steps", 0) > 0, by_id[pid]["routes"]["valhalla_" + cfg]["steps"] > 0)] += 1
        # highway=steps with conveying=* is steps to this graph and an escalator to Valhalla.
        esc = sum(1 for (pid, k), r in found if k == cfg and r.get("osmid") and "valhalla_" + cfg in by_id[pid]["routes"]
                  and not r["edge_use_m"].get("steps") and by_id[pid]["routes"]["valhalla_" + cfg]["steps"] > 0 and r["edge_use_m"].get("escalator"))
        cross[cfg] = {"valhalla_use_steps_and_way_id_steps": c[(True, True)], "valhalla_use_only": c[(True, False)],
                      "way_id_only": c[(False, True)], "way_id_only_on_a_valhalla_escalator": esc, "neither": c[(False, False)]}
    control = None
    for (pid, k), r in found:      # a measured pair whose foot route takes steps by both signals
        d = by_id[pid]
        if k == "foot" and d["snap_apart_m"] <= an.SNAP_FAR_M and r.get("edge_use_m", {}).get("steps", 0) > 0 and d["routes"]["valhalla_foot"]["steps"] > 0:
            w = d["routes"]["valhalla_wheelchair"]
            control = {"pair": pid, "area": d["area"], "foot": {"steps_by_way_id": d["routes"]["valhalla_foot"]["steps"], "valhalla_steps_m": r["edge_use_m"]["steps"]},
                       "wheelchair": {"steps_by_way_id": w.get("steps"), "valhalla_steps_m": NEW[(pid, "wheelchair")].get("edge_use_m", {}).get("steps", 0)}}
            break

    areas = list(t)
    summary = {
        "_meta": {"date": "2026-10-04", "what": "Valhalla audit rows redone by the OSM way ids Valhalla used, against the first run's geometry match",
                  "engine": "Valhalla 3.9.0 (pyvalhalla), tiles in research_notes/compare/engines/valhalla",
                  "way_ids": "trace_attributes on each route's own shape, shape_match edge_walk, walk_or_snap where edge_walk fails (compare/run_valhalla.py)",
                  "matching": "compare/analyse.py `match`: only edges of the named ways are matched, as for ORS wheelchair on plain OSM",
                  "code": "evaluation/compare/results/code/valhalla_way_ids.py"},
        "routes_identical_to_first_run": {f"{cfg} {'same' if s else 'differs'}": n for (cfg, s), n in sorted(same.items())},
        "found_routes": {cfg: sum(k[1] == cfg for k, _ in found) for cfg in KEYS.values()},
        "found_routes_with_way_ids": with_ids,
        "shape_match_used": {cfg: dict(c) for cfg, c in trace.items()},
        "route_minus_traced_length_m": {"median": sorted(gap)[len(gap) // 2], "max": max(gap), "over_50m": len(short),
                                        "over_50m_using_a_ferry": sum("ferry" in r["edge_use_m"] for r in short)},
        "routes_whose_audit_changed": changed,
        "steps_cross_check_all_pairs": cross,
        "positive_control_pair": control,
        "by_area": {a: {key: {f: {"geometry": old_t[a]["audit_against_this_graph"].get(key, {}).get(f), "way_ids": t[a]["audit_against_this_graph"].get(key, {}).get(f)}
                              for f in FIELDS} for key in KEYS} for a in areas},
        "raw_osm_any_by_area": {a: {key: {"geometry": old_t[a]["audit_against_raw_osm"].get(key, {}).get("with_any"), "way_ids": t[a]["audit_against_raw_osm"].get(key, {}).get("with_any")}
                                    for key in KEYS} for a in areas},
    }
    write_json(summary, OUT / "valhalla_way_ids.json", indent=1)
    write_json(t, OUT / "comparison.json", indent=1)
    write_json(t, RN / "results/comparison.json", indent=1)
    with gzip.open(RN / "results/pairs_detail.jsonl.gz", "wt") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k not in ("by_area", "raw_osm_any_by_area")}, indent=1))
    for a in areas:
        print(a, {k: (v["with_steps"], v["with_over_10m_of_street_centreline"]) for k, v in summary["by_area"][a].items()})


if __name__ == "__main__":
    main()
