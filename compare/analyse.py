"""Put every engine's routes side by side and measure them.

For each pair: which routers found a route, each route audited against this
graph's data and against raw OSM tags, overlap and parting point against this
graph's wheelchair route, and one cause for each disagreement with ORS on raw
OSM. Then tables by area.

Routers (ROUTERS below): this graph's own search, ORS on raw OSM (arm A), ORS
on this graph converted to OSM (arm B, strict and known kerbs), Valhalla.

usage: python compare/analyse.py GRAPH_NPZ OSM_TAGS_JSON PAIRS_JSON ROUTES_DIR OUT_DIR [PROCESSES]

ROUTES_DIR holds ours.pkl, the engines' *.jsonl.gz and, for each arm B source,
SOURCE.ways.json (the sidecar scripts/osw_to_osm.py writes beside its PBF).
"""
import gzip
import json
import pickle
import statistics
import sys
from collections import Counter, defaultdict
from multiprocessing import get_context
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shapely.geometry import LineString

from compare.graph import Graph, metres, read_json, write_json
from compare.measures import Matcher, audit, line, near, overlap, parting
from compare.osm_tags import barriers

# name: (source, config, role). "wheelchair" routers are compared with ours; "foot" is each engine's plain foot route.
# ORS "rec" configurations use its recommended weighting (the routes a user sees); the plain i*_k* ones use
# "shortest", like this graph's search. Whether a route is found is the same under both.
ROUTERS = {
    "ors_a_foot": ("ors_armA", "foot_rec", "foot"),
    "ors_a_no_limits": ("ors_armA", "default", "wheelchair"),
    "ors_a_rec_i10_k6": ("ors_armA", "rec_i10_k6", "wheelchair"),
    "ors_a_rec_i6_k6": ("ors_armA", "rec_i6_k6", "wheelchair"),
    "ors_a_shortest_i10_k6": ("ors_armA", "i10_k6", "wheelchair"),
    "ors_b_strict_foot": ("ors_armB_strict", "foot_rec", "foot"),
    "ors_b_strict_rec_i10_k6": ("ors_armB_strict", "rec_i10_k6", "wheelchair"),
    "ors_b_strict_rec_i6_k6": ("ors_armB_strict", "rec_i6_k6", "wheelchair"),
    "ors_b_strict_shortest_i10_k6": ("ors_armB_strict", "i10_k6", "wheelchair"),
    "ors_b_known_rec_i10_k6": ("ors_armB_known", "rec_i10_k6", "wheelchair"),
    "ors_b_known_shortest_i10_k6": ("ors_armB_known", "i10_k6", "wheelchair"),
    "valhalla_foot": ("valhalla", "foot", "foot"),
    "valhalla_wheelchair": ("valhalla", "wheelchair", "wheelchair"),
}
FOOT_OF = {"ours_wheelchair": "ours_walk", "valhalla_wheelchair": "valhalla_foot",
           **{k: "ors_a_foot" for k in ROUTERS if k.startswith("ors_a_") and k != "ors_a_foot"},
           **{k: "ors_b_strict_foot" for k in ROUTERS if k.startswith("ors_b_") and k != "ors_b_strict_foot"}}
REFERENCE = "ors_a_rec_i10_k6"      # raw OSM, ORS's own weighting, incline 10%, kerb 0.06 m
ARM_B = "ors_b_strict_shortest_i10_k6"   # same engine and limits on this graph's data, same objective as ours
SNAP_FAR_M = 25.0
SAME_ROUTE = 0.9                # both routes within 10 m of each other over this share of their length
G = M = TAGS = OURS = ENGINE = None
WAYS = {}        # arm B source -> the matcher geometry behind each OSM way id the converter wrote


def structure(tags):
    return tags.get("bridge", "no") != "no" or tags.get("tunnel", "no") != "no" or tags.get("layer", "0") not in ("0", "")


def describe(edges, unmatched=0.0):
    """One route as its audit against this graph and against raw OSM."""
    e = np.asarray(edges, dtype=int)
    a = audit(G, e)
    flags = Counter()
    for way in dict.fromkeys(G.osm_id[e].tolist()):
        flags.update(barriers(TAGS.get(str(way), {})))
    first = a.pop("first_barrier")
    if first:
        tags = TAGS.get(str(int(G.osm_id[first["edge"]])), {})
        first = {**first, "xy": G.xy[G.u[first["edge"]]].round(6).tolist(), "osm_way": int(G.osm_id[first["edge"]]), "structure": structure(tags)}
    return {**a, "first_barrier": first, "unmatched": round(unmatched, 3), "osm_flags": dict(flags)}


def snapped(lonlat):
    """Where this graph's wheelchair profile puts a coordinate, in metres."""
    e, _, along = G.snap_edge(lonlat, "wheelchair")
    return np.array(LineString(metres(G.edge_coords(e))).interpolate(along).coords[0])


def one(p):
    rec = {"id": p["id"], "set": p["set"], "area": p["area"], "routes": {}, "vs_ours": {}}
    lines = {}
    for prof, r in OURS[p["id"]].items():
        key = f"ours_{prof}"
        rec["routes"][key] = {"found": r["edges"] is not None, "snap_m": r["snap_m"]}
        if r["edges"]:
            rec["routes"][key].update(length_m=r["length_m"], **describe(r["edges"]))
            lines[key] = line(G.line(r["edges"], r["skip_first_m"], r["skip_last_m"]))
    ends = np.array([snapped(p["o"]), snapped(p["d"])])
    far = 0.0
    for key, (src, cfg, role) in ROUTERS.items():
        r = ENGINE[src].get((p["id"], cfg))
        if r is None:
            continue
        rec["routes"][key] = {"found": r["found"], "snap_m": r.get("snap_m"), "error": r.get("error")}
        geom = line(r["coords"]) if r["found"] else None
        if not r["found"]:
            continue
        if geom is None or geom.length == 0:      # both ends snapped to one point
            rec["routes"][key].update(length_m=0.0, **describe([]))
            continue
        ids = np.array(r.get("osmid") or [], dtype=np.int64)
        if len(ids) and src == "ors_armA":      # ORS names the OSM ways it used
            edges, unmatched = M.match(geom, ways=ids)
        elif len(ids) and src in WAYS:          # on this graph, its way ids are this graph's own edges
            edges, unmatched = M.match(geom, among=WAYS[src][ids - 1])
        else:                                   # foot-walking and Valhalla give no way ids
            edges, unmatched = M.match(geom)
        rec["routes"][key].update(length_m=round(r["length_m"], 1), **describe(edges, unmatched))
        lines[key] = geom
        if role == "wheelchair":
            c = metres([r["coords"][0], r["coords"][-1]])
            far = max(far, float(np.hypot(*(c - ends).T).max()))
    rec["snap_apart_m"] = round(far, 1)
    ours = lines.get("ours_wheelchair")
    if ours is not None and ours.length:
        ours_near = near(ours)
        for key, geom in lines.items():
            if key != "ours_wheelchair" and geom.length:
                rec["vs_ours"][key] = {"ours_in_theirs": round(overlap(ours, near(geom)), 3), "theirs_in_ours": round(overlap(geom, ours_near), 3),
                                       "parting_m": parting(geom, ours_near)}
    rec["verdict"] = verdict(rec)
    return rec


def verdict(rec):
    """Ours against the reference: do they agree, and if not, one cause."""
    ours, ref = rec["routes"]["ours_wheelchair"], rec["routes"].get(REFERENCE, {"found": False})
    vs = rec["vs_ours"].get(REFERENCE)
    if ours["found"] and ref["found"]:
        status = "same route" if vs and min(vs["ours_in_theirs"], vs["theirs_in_ours"]) >= SAME_ROUTE else "different route"
    else:
        status = {(True, False): "ours only", (False, True): "reference only", (False, False): "neither"}[(ours["found"], ref["found"])]
    out = {"status": status}
    if status in ("different route", "reference only"):
        first = ref.get("first_barrier")
        if first is None:
            # Nothing on the reference's route is barred here, so this graph could have taken it.
            cause = "connectivity" if ref.get("unmatched", 0) > 0.05 else "rule"
            detail = "reference uses ways this graph does not have" if cause == "connectivity" else "reference route is passable here; the two searches chose differently"
        elif first["why"] == "crossing_no_ramp":
            cause, detail = "kerb data", "reference crosses where no surveyed ramp is within reach"
        elif first["why"] == "over_incline":
            cause = "structure" if first["structure"] else "incline data"
            detail = "reference takes an edge over the incline limits" + (" on a bridge, tunnel or raised way" if first["structure"] else "")
        elif first["why"] == "street":
            cause, detail = "connectivity", "reference follows a street centreline; no sidewalk is mapped there"
        else:
            cause, detail = "rule", "reference takes steps"
        out.update(cause=cause, detail=detail, at=first and first["xy"])
    elif status == "ours only":
        flags = ours.get("osm_flags", {})
        if flags:
            out.update(cause="rule", detail="ORS refuses what OSM tags on this route: " + ", ".join(sorted(flags)))
        else:
            out.update(cause="connectivity", detail="reference finds no route though OSM tags bar nothing on ours")
    b = rec["routes"].get(ARM_B)
    if b is not None:
        vb = rec["vs_ours"].get(ARM_B)
        out["arm_b_matches_ours"] = (b["found"] == ours["found"]) and (not ours["found"] or bool(vb and min(vb["ours_in_theirs"], vb["theirs_in_ours"]) >= SAME_ROUTE))
    return out


def load_engine(routes_dir, src):
    """Rows for the configurations ROUTERS names, and found or not for every configuration run."""
    out, found = {}, defaultdict(dict)
    wanted = {cfg for s, cfg, _ in ROUTERS.values() if s == src}
    for path in sorted(routes_dir.glob(f"{src}.*jsonl.gz")):
        with gzip.open(path, "rt") as f:
            for row in f:
                r = json.loads(row)
                found[r["id"]][r["config"]] = r["found"]
                if r["config"] in wanted:
                    out[(r["id"], r["config"])] = r
    return out, found


def med(xs):
    return round(statistics.median(xs), 3) if xs else None


def tables(recs, matrix):
    """Every measure by area. Pairs whose snapped ends lie over 25 m apart between routers are counted apart."""
    out = {}
    for area in list(dict.fromkeys((r["set"], r["area"]) for r in recs)):
        rs_all = [r for r in recs if (r["set"], r["area"]) == area]
        rs = [r for r in rs_all if r["snap_apart_m"] <= SNAP_FAR_M]
        t = {"pairs": len(rs_all), "pairs_with_snaps_over_25m_apart": len(rs_all) - len(rs), "pairs_measured": len(rs),
             "found": {}, "found_all_pairs": {}, "agreement_with_ours": {}, "detour_over_own_foot_route": {}, "overlap_with_ours": {},
             "audit_against_this_graph": {}, "audit_against_raw_osm": {}}
        keys = list(dict.fromkeys(k for r in rs_all for k in r["routes"]))
        for k in keys:
            have = [r for r in rs if k in r["routes"]]
            if not have:
                continue
            found = [r for r in have if r["routes"][k]["found"]]
            t["found"][k] = round(len(found) / len(have), 4)
            t["found_all_pairs"][k] = round(sum(r["routes"][k]["found"] for r in rs_all if k in r["routes"]) / len(rs_all), 4)
            if k != "ours_wheelchair":
                c = Counter((r["routes"]["ours_wheelchair"]["found"], r["routes"][k]["found"]) for r in have)
                t["agreement_with_ours"][k] = {"both": c[(True, True)], "ours_only": c[(True, False)], "theirs_only": c[(False, True)], "neither": c[(False, False)]}
                ov = [min(r["vs_ours"][k]["ours_in_theirs"], r["vs_ours"][k]["theirs_in_ours"]) for r in have if k in r["vs_ours"]]
                t["overlap_with_ours"][k] = {"both_found": len(ov), "median": med(ov), "share_same_route": round(sum(o >= SAME_ROUTE for o in ov) / len(ov), 3) if ov else None}
            if k in FOOT_OF:
                d = [r["routes"][k]["length_m"] / r["routes"][FOOT_OF[k]]["length_m"] for r in found
                     if r["routes"].get(FOOT_OF[k], {}).get("found") and r["routes"][FOOT_OF[k]]["length_m"] > 0]
                t["detour_over_own_foot_route"][k] = {"pairs": len(d), "median": med(d), "p90": round(float(np.percentile(d, 90)), 3) if d else None,
                                                      "share_over_1.5": round(sum(x > 1.5 for x in d) / len(d), 3) if d else None}
            if found:
                n = len(found)
                km = sum(r["routes"][k]["length_m"] for r in found) / 1000
                t["audit_against_this_graph"][k] = {
                    "routes": n,
                    "with_steps": round(sum(r["routes"][k]["steps"] > 0 for r in found) / n, 4),
                    "with_a_crossing_without_a_surveyed_ramp": round(sum(r["routes"][k]["crossing_no_ramp"] > 0 for r in found) / n, 4),
                    "with_an_edge_over_the_incline_limits": round(sum(r["routes"][k]["over_incline"] > 0 for r in found) / n, 4),
                    "with_over_10m_of_street_centreline": round(sum(r["routes"][k]["street_m"] > 10 for r in found) / n, 4),
                    "with_any_of_these": round(sum(r["routes"][k]["steps"] + r["routes"][k]["crossing_no_ramp"] + r["routes"][k]["over_incline"] > 0 or r["routes"][k]["street_m"] > 10 for r in found) / n, 4),
                    "unramped_crossings_per_km": round(sum(r["routes"][k]["crossing_no_ramp"] for r in found) / km, 3) if km else None,
                    "median_share_off_this_graph": med([r["routes"][k]["unmatched"] for r in found]),
                }
                flags = Counter(f for r in found for f in r["routes"][k]["osm_flags"])
                t["audit_against_raw_osm"][k] = {"routes": n, **{f"with_{f}": round(c / n, 4) for f, c in sorted(flags.items())},
                                                 "with_any": round(sum(bool(r["routes"][k]["osm_flags"]) for r in found) / n, 4)}
        t["ors_setting_matrix_found"] = {src: {cfg: round(sum(matrix[src][r["id"]].get(cfg, False) for r in rs_all) / len(rs_all), 4)
                                               for cfg in sorted({c for r in rs_all for c in matrix[src][r["id"]]})} for src in matrix}
        v = Counter(r["verdict"]["status"] for r in rs)
        causes = Counter(r["verdict"].get("cause") for r in rs if r["verdict"].get("cause"))
        t["ours_against_reference"] = {"reference": REFERENCE, "status": dict(v), "causes_of_disagreement": dict(causes),
                                       "arm_b_strict_matches_ours": round(sum(bool(r["verdict"].get("arm_b_matches_ours")) for r in rs) / len(rs), 4) if rs else None}
        out["/".join(area)] = t
    return out


def main(npz, tags_json, pairs_json, routes_dir, out_dir, procs=8):
    global G, M, TAGS, OURS, ENGINE
    routes_dir, out_dir = Path(routes_dir), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    G = Graph(npz)
    M = Matcher(G)
    TAGS = read_json(tags_json)
    with open(routes_dir / "ours.pkl", "rb") as f:
        OURS = pickle.load(f)
    ENGINE, matrix = {}, {}
    for src in sorted({s for s, _, _ in ROUTERS.values()}):
        ENGINE[src], found = load_engine(routes_dir, src)
        if src.startswith("ors"):
            matrix[src] = found
    edge_of = {str(f): e for e, f in enumerate(G.fid.tolist())}
    for src in ENGINE:
        sidecar = routes_dir / f"{src}.ways.json"
        if sidecar.exists():
            WAYS[src] = M.base_of([edge_of[f] for f in read_json(sidecar)])
    pairs = read_json(pairs_json)["pairs"]
    G.snap_edge(pairs[0]["o"], "wheelchair")        # build the snap index before the fork
    recs = []
    with get_context("fork").Pool(procs) as pool:
        for n, rec in enumerate(pool.imap(one, pairs, chunksize=20)):
            recs.append(rec)
            if n % 1000 == 0:
                print(n, len(pairs), flush=True)
    with gzip.open(out_dir / "pairs_detail.jsonl.gz", "wt") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    t = tables(recs, matrix)
    write_json(t, out_dir / "comparison.json", indent=1)
    for area, row in t.items():
        print(area, row["pairs_measured"], {k: row["found"][k] for k in ("ours_wheelchair", REFERENCE, ARM_B, "valhalla_wheelchair") if k in row["found"]},
              row["ours_against_reference"]["status"], row["ours_against_reference"]["causes_of_disagreement"])


if __name__ == "__main__":
    main(*sys.argv[1:6], *(int(x) for x in sys.argv[6:7]))
