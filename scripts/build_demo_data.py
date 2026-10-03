"""Write the data files the Brownsville demo (demo/) reads.

Inputs are the built graph and the router comparison's result files, so the
page shows nothing that is not behind a file:
  OSW clip of the district            compare/clip_osw.py
  this graph as arrays                compare/graph.py
  pairs, routes and per-pair verdicts compare/pairs.py, run_ours.py, run_ors.py, analyse.py
  DOT's per-ramp assessment           ArcGIS layer CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD, saved pages
  DOT's per-corner progress           NYC Open Data e7gc-ub6z, saved CSV

Output: demo/data/{meta,network,ramps,routes}.js, each one assignment to
window.DEMO, so the page works opened from a file with no server.

usage: python scripts/build_demo_data.py CONFIG_JSON
"""

from __future__ import annotations

import csv
import glob
import gzip
import json
import math
import pickle
import sys
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
from shapely import STRtree
from shapely.geometry import LineString, Point, shape
from shapely.ops import linemerge

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from osw_to_unweaver import RAMP_REACH_M, crossing_ends

from compare.graph import EAST, NORTH, Graph, metres
from compare.measures import Matcher, line

BUILT = {"Constructed", "Complex Constructed"}
STATUS = {"Compliant": 1, "Pending": 2, "Non-Compliant": 3}
STATUS_TEXT = {0: "no DOT assessment", 1: "Compliant", 2: "Pending Technical Review", 3: "Non-Compliant"}
COMPASS = ["north", "north-east", "east", "south-east", "south", "south-west", "west", "north-west"]
TRIPS = 12
NOISE_M = 3.0       # an edge shorter than this does not set a route's "steepest stretch"


def latlon(coords):
    return [[round(y, 6), round(x, 6)] for x, y, *_ in coords]


def bearing(a, b):
    """Degrees clockwise from north, from (lon, lat) a to b."""
    return math.degrees(math.atan2((b[0] - a[0]) * EAST, (b[1] - a[1]) * NORTH)) % 360


def js(name, value, path):
    path.write_text(f"window.DEMO = window.DEMO || {{}};\nwindow.DEMO.{name} = {json.dumps(value, separators=(',', ':'))};\n")


# ---------------------------------------------------------------- ramps

def load_ramps(nodes, compliance_glob, progress_csv):
    """Each surveyed ramp in the clip with DOT's assessment and whether its corner was rebuilt since."""
    by_id = {int(f["properties"]["ext:ramp_id"]): f for f in nodes if f["properties"].get("barrier") == "kerb"}
    layer = {}
    for page in sorted(glob.glob(compliance_glob)):
        with open(page) as f:
            for ft in json.load(f)["features"]:
                a = ft["attributes"]
                if a["RAMPID"] in by_id:
                    layer[a["RAMPID"]] = a
    with open(progress_csv) as f:
        progress = {r["CornerID"].strip(): r for r in csv.DictReader(f)}
    out = []
    for rid, f in by_id.items():
        p, a = f["properties"], layer.get(rid)
        surveyed = datetime.fromtimestamp(a["GEOCYCLORAMA_DATE"] / 1000, UTC).date() if a else None
        corner = progress.get(str(p["ext:corner_id"]))
        rebuilt, built_year = False, None
        if corner and corner["Construction_Status_Value"] in BUILT:
            try:
                end = datetime.strptime(corner["Construction_End_Date"], "%Y/%m/%d").date()  # noqa: DTZ007 (a calendar day)
            except ValueError:
                end = None
            # As in the ramp backlog method (evaluation/ramp_backlog/METHOD.md): a built corner with no usable date counts as rebuilt.
            usable = end is not None and 1990 <= end.year and end <= date.today()  # noqa: DTZ011
            rebuilt = not usable or surveyed is None or end > surveyed
            built_year = end.year if usable and rebuilt else None
        x, y = f["geometry"]["coordinates"][:2]
        streets = " and ".join(s for s in (p.get("ext:street_1"), p.get("ext:street_2")) if s)
        out.append({"node": p["_id"], "ramp_id": rid, "corner_id": str(p["ext:corner_id"]), "xy": (x, y),
                    "status": STATUS.get(a["COMPLIANCY_STATUS"], 0) if a else 0, "rebuilt": rebuilt, "built_year": built_year,
                    "surveyed": surveyed, "slope": p.get("ext:running_slope_pct"), "corner": streets,
                    "progress": corner["Construction_Status_Value"] if corner else "No progress record"})
    return out


# ---------------------------------------------------------------- network

class Network:
    """The clip's edges with the names the text directions need."""

    def __init__(self, feats, ramps):
        self.nodes = {f["properties"]["_id"]: f["geometry"]["coordinates"][:2] for f in feats if f["geometry"]["type"] == "Point"}
        self.edges = [f for f in feats if f["geometry"]["type"] == "LineString"]
        kind = lambda p: ("crossing" if p.get("footway") == "crossing" else "sidewalk" if p.get("footway") == "sidewalk" else "path") \
            if p["highway"] == "footway" else "steps" if p["highway"] == "steps" else "street"
        self.kind = {f["properties"]["_id"]: kind(f["properties"]) for f in self.edges}
        named = [f for f in self.edges if self.kind[f["properties"]["_id"]] == "street" and f["properties"].get("name")]
        self.street_geoms = [LineString(metres([c[:2] for c in f["geometry"]["coordinates"]])) for f in named]
        self.street_names = [f["properties"]["name"] for f in named]
        self.street_tree = STRtree(self.street_geoms)
        curb = {f["properties"]["_id"] for f in feats if f["geometry"]["type"] == "Point" and f["properties"].get("barrier") == "kerb"}
        groups, self.ends = crossing_ends(feats, curb, self.nodes, RAMP_REACH_M)
        self.group_of = {eid: g for g, eids in groups.items() for eid in eids}
        self.groups = groups
        self.ramps = ramps
        self.ramp_xy = np.array([metres(r["xy"]) for r in ramps])
        geom = {f["properties"]["_id"]: LineString(metres([c[:2] for c in f["geometry"]["coordinates"]])) for f in self.edges}
        self.beside = {eid: self._parallel_street(geom[eid]) for eid, k in self.kind.items() if k == "sidewalk"}
        self.over = {g: self._crossed_street([geom[e] for e in eids]) for g, eids in groups.items()}

    def _parallel_street(self, g):
        a, b = g.coords[0], g.coords[-1]
        mine = math.degrees(math.atan2(b[0] - a[0], b[1] - a[1])) % 180
        best = None
        for i in self.street_tree.query(g.buffer(30)):
            s = self.street_geoms[i]
            c, d = s.coords[0], s.coords[-1]
            diff = abs(mine - math.degrees(math.atan2(d[0] - c[0], d[1] - c[1])) % 180)
            if min(diff, 180 - diff) < 30 and (best is None or s.distance(g) < best[0]):
                best = (s.distance(g), self.street_names[i])
        return best[1] if best else None

    def _crossed_street(self, geoms):
        hits = Counter()
        for g in geoms:
            for i in self.street_tree.query(g, predicate="intersects"):
                hits[self.street_names[i]] += 1
        if hits:
            return hits.most_common(1)[0][0]
        near = [(self.street_geoms[i].distance(g), self.street_names[i]) for g in geoms for i in self.street_tree.query(g.buffer(15))]
        return min(near)[1] if near else None

    def ramp_near(self, node):
        """The surveyed ramp within reach of a crossing end, or None."""
        if not len(self.ramp_xy):
            return None
        d = np.hypot(*(self.ramp_xy - metres(self.nodes[node])).T)
        return self.ramps[int(d.argmin())] if d.min() <= RAMP_REACH_M else None


def network_layers(net, cd):
    """Map layers: one entry per pair of opposite edges."""
    walks, crossings, steps, seen = [], [], [], set()
    by_way = defaultdict(list)
    for f in net.edges:
        p = f["properties"]
        coords = [tuple(c[:2]) for c in f["geometry"]["coordinates"]]
        key = min(tuple(coords), tuple(reversed(coords)))
        if key in seen:
            continue
        seen.add(key)
        k = net.kind[p["_id"]]
        pct = None if p.get("incline") is None else round(abs(p["incline"]) * 100, 1)
        if k == "street":
            if p.get("name"):
                by_way[p["name"]].append(LineString(coords))
        elif k == "crossing":
            g = net.group_of[p["_id"]]
            crossings.append([latlon(coords), int(bool(net.ends.get(g)) and all(net.ends[g].values())), net.over.get(g)])
        elif k == "steps":
            steps.append([latlon(coords)])
        else:
            walks.append([latlon(coords), pct, p.get("width"), net.beside.get(p["_id"])])
    streets, labels = [], []
    for name, lines in sorted(by_way.items()):
        merged = linemerge(lines)
        parts = list(merged.geoms) if merged.geom_type == "MultiLineString" else [merged]
        streets += [[latlon(part.coords)] for part in parts]
        inside = [part.intersection(cd) for part in parts]
        longest = max(inside, key=lambda g: g.length)
        if longest.length * NORTH > 150:
            mid = longest.interpolate(0.5, normalized=True)
            labels.append([name, round(mid.y, 6), round(mid.x, 6)])
    return {"walks": walks, "crossings": crossings, "steps": steps, "streets": streets, "street_labels": labels}


# ---------------------------------------------------------------- directions

def turn(prev, now):
    d = (now - prev + 180) % 360 - 180
    return "Continue" if abs(d) < 30 else "Turn around and go" if abs(d) > 150 else ("Turn right, heading" if d > 0 else "Turn left, heading")


def describe(g, net, edges, dest):
    """A route (edges of the city graph, in travel order) as facts and as text steps."""
    runs = []
    for e in edges:
        fid, k = str(g.fid[e]), str(g.kind[e])
        k = "path" if k == "footway" else k
        label = net.group_of.get(fid) if k == "crossing" else net.beside.get(fid) if k == "sidewalk" else (str(g.name[e]) or None)
        if runs and runs[-1]["kind"] == k and runs[-1]["label"] == label:
            runs[-1]["edges"].append(e)
        else:
            runs.append({"kind": k, "label": label, "edges": [e]})
    facts = {"crossings": 0, "no_ramp": 0, "steps": 0, "rebuilt": 0, "street_m": 0.0}
    long_edges = [e for e in edges if g.length[e] >= NOISE_M and not np.isnan(g.incline[e]) and g.kind[e] != "steps"]
    facts["steepest"] = round(float(max(abs(g.incline[e]) for e in long_edges)) * 100, 1) if long_edges else None
    text, prev = [], None
    for r in runs:
        es = r["edges"]
        d = float(g.length[es].sum())
        first, last = g.edge_coords(es[0]), g.edge_coords(es[-1])
        b_in, b_out = bearing(first[0], first[1]), bearing(last[-2], last[-1])
        way = COMPASS[round(bearing(first[0], last[-1]) / 45) % 8]
        lead = "Head" if prev is None else turn(prev, b_in)
        prev = b_out
        dist = f"{round(d)} m" if d >= 1 else "under 1 m"
        if r["kind"] == "crossing":
            facts["crossings"] += 1
            ends = net.ends.get(r["label"], {})
            entry = g.xy[g.u[es[0]]]
            order = sorted(ends, key=lambda n: float(np.hypot(*(metres(net.nodes[n]) - metres(entry)))))
            names = ["near", "far"] if len(order) == 2 else [f"end {i + 1}" for i in range(len(order))]
            missing = [nm for nm, n in zip(names, order, strict=True) if not ends[n]]
            over = f"Cross {r['label'] and net.over.get(r['label']) or 'the street'}" if lead in ("Head", "Continue") else f"{lead.split(',')[0]} and cross {net.over.get(r['label']) or 'the street'}"
            s = f"{over} ({dist})."
            if not ends:
                s += " This crossing does not join the mapped sidewalks, so its ramps cannot be judged."
                facts["no_ramp"] += 1
            elif missing:
                facts["no_ramp"] += 1
                s += f" No surveyed ramp within 5 m of the {' or the '.join(missing)} end: this graph's wheelchair profile does not cross here."
            else:
                s += " A surveyed ramp is within 5 m of both ends."
            notes = []
            for nm, n in zip(names, order, strict=True):
                ramp = net.ramp_near(n)
                if ramp is None:
                    continue
                note = f"{nm} ramp {STATUS_TEXT[ramp['status']]}"
                if ramp["rebuilt"]:
                    facts["rebuilt"] += 1
                    note += f", corner rebuilt{' in ' + str(ramp['built_year']) if ramp['built_year'] else ''} after the survey"
                notes.append(note)
            if notes:
                s += " DOT's assessment of its survey: " + "; ".join(notes) + "."
        elif r["kind"] == "steps":
            facts["steps"] += 1
            s = f"{lead} {way} up or down a flight of steps ({dist})."
        elif r["kind"] == "street":
            facts["street_m"] += d
            s = f"{lead} {way} in the roadway of {r['label'] or 'an unnamed street'} for {dist}. No sidewalk is mapped here."
        else:
            where = f"along the sidewalk beside {r['label']}" if r["kind"] == "sidewalk" and r["label"] else \
                "along the sidewalk" if r["kind"] == "sidewalk" else f"along the path{' (' + r['label'] + ')' if r['label'] else ''}"
            s = f"{lead} {way} {where} for {dist}."
            steep = [e for e in es if g.length[e] >= NOISE_M and not np.isnan(g.incline[e])]
            if steep:
                e = max(steep, key=lambda e: abs(g.incline[e]))
                pct = abs(float(g.incline[e])) * 100
                s += f" Steepest stretch {pct:.1f}% {'uphill' if g.incline[e] > 0 else 'downhill'}." if pct >= 2 else " Close to level."
            widths = g.width[es][~np.isnan(g.width[es])]
            if len(widths):
                s += f" Mapped width at least {float(widths.min()):.1f} m."
        text.append(s)
    text.append(f"Arrive near {dest}.")
    facts["street_m"] = round(facts["street_m"], 1)
    return facts, text


# ---------------------------------------------------------------- routes

def verdict_text(rec, ours, ref):
    v = rec["verdict"]
    diff = (ours["length_m"] - ref["length_m"]) if ours["found"] and ref["found"] else None
    longer = f"{abs(round(diff))} m {'longer' if diff > 0 else 'shorter'} than OpenRouteService's" if diff is not None else ""
    if v["status"] == "same route":
        return "The two wheelchair routes agree.", "This graph and OpenRouteService take the same path, within 10 m over at least 90% of its length."
    if v["status"] == "neither":
        return "Neither router finds a wheelchair route.", "Both this graph's wheelchair profile and OpenRouteService's report no route for this trip."
    if v["status"] == "ours only":
        return "This graph finds a wheelchair route and OpenRouteService does not.", v["detail"][0].upper() + v["detail"][1:] + "."
    head = "This graph finds no wheelchair route; OpenRouteService does." if v["status"] == "reference only" else "The two wheelchair routes differ."
    why = {
        "kerb data": "OpenRouteService crosses where NYC DOT's survey has no ramp within 5 m of one end. OpenStreetMap carries no kerb tag that "
                     "OpenRouteService reads as a barrier there, so it passes. This graph's profile refuses such a crossing",
        "incline data": "OpenRouteService takes a stretch that the LiDAR incline puts over this profile's limits (8.3% up, 10% down). "
                        "OpenStreetMap has no incline tag there, so OpenRouteService passes it",
        "structure": "OpenRouteService takes a bridge, tunnel or raised way where this graph's deck heights give an incline over the profile's limits",
        "connectivity": "OpenRouteService uses a way that this graph's wheelchair profile cannot: a street centreline where no sidewalk is mapped, "
                        "or a way this graph does not include",
        "rule": "By this graph's data OpenRouteService's route is passable too. The difference comes from OpenRouteService's own rules "
                "(its surface and smoothness limits and its route weighting), not from the ramp or incline data",
    }[v["cause"]]
    # Name this graph as the subject: after the incline data and connectivity
    # clauses "its route" would read as OpenRouteService compared with itself.
    tail = f". This graph's route is {longer}." if v["status"] == "different route" else ", and with it barred finds no way through."
    if v["cause"] == "rule" and v["status"] != "different route":
        tail = "."
    return head, why + tail


def choose(recs, pairs, inside):
    """A fixed, mixed selection of Brownsville trips: agreement first, then each kind of disagreement."""
    want = [("same route", None, 3), ("different route", "kerb data", 4), ("different route", "incline data", 2),
            ("different route", "rule", 1), ("different route", "connectivity", 1), ("reference only", None, 2), ("ours only", None, 1)]
    picked, used = [], set()
    for status, cause, n in want:
        pool = [r for r in recs if r["verdict"]["status"] == status and (cause is None or r["verdict"].get("cause") == cause)
                and r["snap_apart_m"] <= 25 and inside(pairs[r["id"]]) and r["routes"].get("ors_a_foot", {}).get("found")]
        pool.sort(key=lambda r: (pairs[r["id"]]["pick"] != "nearest", abs(r["routes"]["ors_a_foot"]["length_m"] - 900), r["id"]))
        for r in pool:
            o = pairs[r["id"]]["o_name"]
            if len([x for x in picked if x[1] == (status, cause)]) >= n:
                break
            if o in used or r["routes"]["ors_a_foot"]["length_m"] < 250:
                continue
            used.add(o)
            picked.append((r, (status, cause)))
    return [r for r, _ in picked][:TRIPS]


def main(cfg):
    out = ROOT / "demo/data"
    out.mkdir(parents=True, exist_ok=True)
    with open(cfg["osw_clip"]) as f:
        osw = json.load(f)
    feats = osw["features"]
    with open(cfg["community_districts"]) as f:
        cd = next(shape(ft["geometry"]) for ft in json.load(f)["features"] if ft["properties"]["boro_cd"] == "316")
    ramps = load_ramps(feats, cfg["compliance_pages"], cfg["progress_csv"])
    net = Network(feats, ramps)
    layers = network_layers(net, cd)
    js("network", layers, out / "network.js")

    in_cd = [r for r in ramps if cd.contains(Point(r["xy"]))]
    js("ramps", [[round(r["xy"][1], 6), round(r["xy"][0], 6), r["status"], int(r["rebuilt"]), r["surveyed"].strftime("%B %Y") if r["surveyed"] else "date unknown",
                  r["slope"], r["corner"].title(), r["built_year"]] for r in ramps], out / "ramps.js")

    # ---- routes
    g = Graph(cfg["graph_npz"])
    matcher = Matcher(g)
    with open(cfg["pairs"]) as f:
        pairs = {p["id"]: p for p in json.load(f)["pairs"] if p["set"] == "brownsville"}
    with gzip.open(cfg["pairs_detail"], "rt") as f:
        recs = [r for r in map(json.loads, f) if r["set"] == "brownsville"]
    with open(cfg["ours_pkl"], "rb") as f:
        ours_routes = pickle.load(f)
    ors = {}
    for path in cfg["ors_arm_a"]:       # later files win: the last one carries the way ids
        with gzip.open(path, "rt") as f:
            for row in f:
                r = json.loads(row)
                if r["id"] in pairs and r["config"] in ("foot_rec", "rec_i10_k6"):
                    ors[(r["id"], r["config"])] = r
    area = shape(osw["region"])
    inside = lambda p: area.contains(Point(p["o"])) and area.contains(Point(p["d"]))
    trips = []
    for rec in choose(recs, pairs, inside):
        p = pairs[rec["id"]]
        options = {}
        mine = ours_routes[p["id"]]["wheelchair"]
        if mine["edges"]:
            facts, text = describe(g, net, mine["edges"], p["d_name"])
            options["ours"] = {"found": True, "length_m": mine["length_m"], **facts,
                               "coords": latlon(g.line(mine["edges"], mine["skip_first_m"], mine["skip_last_m"])), "steps_text": text}
        else:
            options["ours"] = {"found": False, "note": "This graph's wheelchair profile finds no route for this trip."}
        for key, cfg_name in (("ors_wheelchair", "rec_i10_k6"), ("ors_foot", "foot_rec")):
            r = ors[(p["id"], cfg_name)]
            if not r["found"]:
                options[key] = {"found": False, "note": "OpenRouteService finds no route for this trip."}
                continue
            matched, unmatched = matcher.match(line(r["coords"]), ways=np.array(r["osmid"], dtype=np.int64) if r.get("osmid") else None)
            facts, text = describe(g, net, matched, p["d_name"])
            if unmatched > 0.05:
                text.insert(0, f"About {round(unmatched * 100)}% of this route runs on ways this graph does not include. Those parts are drawn on the map and are not described here.")
            options[key] = {"found": True, "length_m": r["length_m"], **facts, "coords": latlon(r["coords"]), "steps_text": text}
        head, reason = verdict_text(rec, rec["routes"]["ours_wheelchair"], rec["routes"]["ors_a_rec_i10_k6"])
        trips.append({"id": p["id"], "from": {"name": p["o_name"], "kind": p["o_kind"], "lat": p["o"][1], "lon": p["o"][0]},
                      "to": {"name": p["d_name"], "kind": p["d_kind"], "lat": p["d"][1], "lon": p["d"][0]},
                      "verdict": {"status": rec["verdict"]["status"], "cause": rec["verdict"].get("cause"), "headline": head, "reason": reason},
                      "options": options})
    js("routes", trips, out / "routes.js")

    # ---- meta: tables, status, sources
    def within(coords):
        return cd.contains(Point(coords[len(coords) // 2][1], coords[len(coords) // 2][0]))
    walks = [w for w in layers["walks"] if within(w[0])]
    km = lambda rows: sum(LineString(metres([(x, y) for y, x in r[0]])).length for r in rows) / 1000
    cls = lambda pct: "no value" if pct is None else "over 8.3%" if pct > 8.3 else "5% to 8.3%" if pct > 5 else "up to 5%"
    by_cls = defaultdict(list)
    for w in walks:
        by_cls[cls(w[1])].append(w)
    wide = [w for w in walks if w[2] is not None]
    groups_in = {net.group_of[f["properties"]["_id"]] for f in net.edges if net.kind[f["properties"]["_id"]] == "crossing"
                 and cd.contains(Point(f["geometry"]["coordinates"][0][:2]))}
    ramped = sum(bool(net.ends.get(gid)) and all(net.ends[gid].values()) for gid in groups_in)
    network_tables = [
        {"caption": "Sidewalks and paths inside the district, by steepest incline", "head": ["Incline", "Length (km)", "Share of length"],
         "rows": [[k, f"{km(by_cls[k]):.1f}", f"{km(by_cls[k]) / km(walks):.1%}"] for k in ("up to 5%", "5% to 8.3%", "over 8.3%", "no value") if by_cls[k]]},
        {"caption": "Sidewalk width, where the planimetric survey gives one", "head": ["Mapped width", "Length (km)", "Share of length with a width"],
         "rows": [[label, f"{km(rows):.1f}", f"{km(rows) / km(wide):.1%}"] for label, rows in (
             ("under 1.5 m", [w for w in wide if w[2] < 1.5]), ("1.5 m to 3 m", [w for w in wide if 1.5 <= w[2] < 3]), ("3 m and over", [w for w in wide if w[2] >= 3]))]},
        {"caption": "Crossings inside the district", "head": ["Crossings", "Count", "Share"],
         "rows": [["A surveyed ramp within 5 m of every end", ramped, f"{ramped / len(groups_in):.1%}"],
                  ["No surveyed ramp near at least one end", len(groups_in) - ramped, f"{1 - ramped / len(groups_in):.1%}"]]},
    ]
    n = len(in_cd)
    by_status = Counter((r["status"], r["rebuilt"]) for r in in_cd)
    ramp_rows = [[STATUS_TEXT[s], by_status[(s, False)] + by_status[(s, True)], by_status[(s, False)], by_status[(s, True)]] for s in (3, 2, 1, 0) if by_status[(s, False)] + by_status[(s, True)]]
    ramp_rows.append(["All surveyed ramps", n, sum(not r["rebuilt"] for r in in_cd), sum(r["rebuilt"] for r in in_cd)])
    dates = sorted(r["surveyed"] for r in in_cd if r["surveyed"])
    # One row per intersection: its corners share a name in the survey.
    corners = defaultdict(lambda: defaultdict(list))
    for r in in_cd:
        corners[r["corner"].title()][r["corner_id"]].append(r)
    corner_rows = []
    for name, by_corner in sorted(corners.items()):
        rs = [r for group in by_corner.values() for r in group]
        c = Counter(STATUS_TEXT[r["status"]] for r in rs)
        rebuilt = [group[0] for group in by_corner.values() if group[0]["rebuilt"]]
        years = [r["built_year"] for r in rebuilt if r["built_year"]]
        if rebuilt:
            since = f"{len(rebuilt)} of {len(by_corner)} surveyed corners rebuilt" + (f", latest in {max(years)}" if years else "") + ". Survey values there are out of date."
        else:
            since = "Not rebuilt. DOT status: " + ", ".join(sorted({group[0]["progress"] for group in by_corner.values()})) + "."
        corner_rows.append([name, len(rs), ", ".join(f"{k}: {v}" for k, v in sorted(c.items())), since])
    cmp_ = cfg["numbers"]
    meta = {
        "bounds": [[cd.bounds[1], cd.bounds[0]], [cd.bounds[3], cd.bounds[2]]],
        "boundary": [latlon(poly.exterior.coords) for poly in (cd.geoms if cd.geom_type == "MultiPolygon" else [cd])],
        "network_tables": network_tables,
        "ramp_dates": f"The {n:,} surveyed ramps in the district were captured between {dates[0].strftime('%B %Y')} and {dates[-1].strftime('%B %Y')}. "
                      f"DOT's assessment layer was last edited in December 2020. DOT's corner progress file, read on {cfg['progress_retrieved']}, lists the corners of "
                      f"{sum(r['rebuilt'] for r in in_cd):,} of these ramps as rebuilt after the survey. A rebuilt corner is drawn as a hollow diamond: the survey's slope and "
                      f"status describe the ramp that was replaced, and the progress file does not say which ramps at the corner were rebuilt.",
        "ramp_table": {"caption": "Surveyed ramps inside the district, by DOT's assessment", "head": ["DOT's assessment of its survey", "Ramps", "Corner not rebuilt since", "Corner rebuilt since"],
                       "rows": ramp_rows},
        "corners": corner_rows,
        "status": cmp_["status"],
        "sources": cmp_["sources"],
    }
    js("meta", meta, out / "meta.js")
    summary = {"trips": [(t["id"], t["verdict"]["status"], t["verdict"]["cause"]) for t in trips], "ramps_in_district": n,
               "walk_km": round(km(walks), 1), "crossings": len(groups_in), "crossings_ramped": ramped,
               "bytes": {p.name: p.stat().st_size for p in sorted(out.glob("*.js"))}}
    print(json.dumps(summary, indent=1))
    return summary


if __name__ == "__main__":
    with open(sys.argv[1]) as fh:
        main(json.load(fh))
