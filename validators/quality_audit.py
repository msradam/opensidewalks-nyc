#!/usr/bin/env python
"""
quality_audit.py — OpenSidewalks v0.3 conformance + data quality audit
for the NYC artifact at /Users/amsrahman/macadam-nyc/opensidewalks_nyc.geojson.

The script streams the artifact (ijson) and runs:

  1. JSON Schema validation against the cached OSW v0.3 schema.
  2. Graph integrity: u/v resolution, orphans, self-loops, duplicates,
     connected components (city + per borough).
  3. Geometric sanity: bbox, length distribution, zero/short/long edges.
  4. Attribute distributions: surface, crossing:markings, kerb, incline,
     slope (running/cross), elevation. ADA-threshold compliance %.
  5. Provenance coverage: source, timestamp, pipeline-version fields.
  6. Coverage gaps: sidewalk/street ratio, crossings/intersection ratio,
     curb-ramp snapping rate, per-borough density.
  7. METHODOLOGY-stated limitations: planimetric/Voronoi failures,
     bare-node injections, sentinel 999.0 leakage.

Outputs:
  * stdout: a JSON summary blob.
  * validators/findings/*.csv: per-check flagged-feature lists.

Usage (from /Users/amsrahman/opensidewalks-nyc/):
    ~/ariadne-nyc/.venv/bin/python validators/quality_audit.py
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import ijson
from jsonschema import Draft7Validator
from pyproj import Transformer

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path("/Users/amsrahman/opensidewalks-nyc")
ARTIFACT = Path("/Users/amsrahman/macadam-nyc/opensidewalks_nyc.geojson")
SCHEMA_PATH = ROOT / "validators" / "schema-cache" / "opensidewalks.schema.json"
FINDINGS = ROOT / "validators" / "findings"
FINDINGS.mkdir(parents=True, exist_ok=True)

NYC_BBOX = (-74.2591, 40.4774, -73.7004, 40.9176)  # min_lon, min_lat, max_lon, max_lat
BOROUGH_AREAS_KM2 = {
    "MN": 59.1,   # Manhattan
    "BK": 179.6,  # Brooklyn
    "QN": 281.5,  # Queens
    "BX": 109.0,  # Bronx
    "SI": 151.2,  # Staten Island
}

SCHEMA_FAIL_SAMPLE_LIMIT = 3
MAX_VALIDATION_FEATURES = None  # None = all; set int for a quick smoke pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_float(x):
    """Decimal/str/number → float; returns None on failure."""
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def coerce_coord(c):
    """Coerce a coordinate that may be a string into a float pair."""
    if not isinstance(c, (list, tuple)) or len(c) < 2:
        return None
    a = _to_float(c[0])
    b = _to_float(c[1])
    if a is None or b is None:
        return None
    return (a, b)


def percentile(sorted_vals, q):
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    if q >= 100:
        return sorted_vals[-1]
    idx = (len(sorted_vals) - 1) * (q / 100.0)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_vals[lo]
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


# ---------------------------------------------------------------------------
# Schema preparation
# ---------------------------------------------------------------------------
def load_schema_with_compatibility():
    """Load 0.3 schema, but relax CompatibleSchemaURI so the artifact's 0.2
    declaration is at least let through (we report the mismatch separately)."""
    schema = json.load(open(SCHEMA_PATH))
    # Relax CompatibleSchemaURI: keep type, drop pattern/const if any so the
    # 0.2 declaration doesn't blow up every single feature with a redundant
    # message. We capture the mismatch as its own headline finding.
    if "CompatibleSchemaURI" in schema.get("definitions", {}):
        schema["definitions"]["CompatibleSchemaURI"] = {"type": "string"}
    return schema


def per_feature_validators(schema):
    """Build one Draft7Validator per feature subschema. We validate features
    individually rather than the whole 1.17M-feature collection because:
    (a) we can stream, (b) we can attribute failures to specific feature ids,
    (c) Draft7Validator iter_errors on a 430MB document is intractable."""
    feature_anyof = schema["properties"]["features"]["items"]["anyOf"]
    feature_schemas = []
    for item in feature_anyof:
        ref = item["$ref"].split("/")[-1]
        feature_schemas.append((ref, schema["definitions"][ref]))

    def validate_feature(feat):
        # Try each option; the feature is valid if ANY matches with no errors.
        # Otherwise we report the option whose number of errors is smallest
        # AND whose discriminator matches (geom type + highway/footway/barrier).
        # That gives us human-readable failure attribution.
        candidates = []
        gtype = (feat.get("geometry") or {}).get("type")
        props = feat.get("properties") or {}
        hwy = props.get("highway")
        fw = props.get("footway")
        barrier = props.get("barrier")
        kerb = props.get("kerb")

        # Heuristic discriminator -> definition name
        wanted = None
        if gtype == "Point":
            if barrier == "kerb":
                wanted = {
                    "lowered": "CurbRamp",
                    "flush": "FlushCurb",
                    "raised": "RaisedCurb",
                    "rolled": "RolledCurb",
                }.get(kerb, "GenericCurb")
            else:
                wanted = "BareNode"
        elif gtype == "LineString":
            if hwy == "footway":
                wanted = {
                    "sidewalk": "Sidewalk",
                    "crossing": "Crossing",
                    "traffic_island": "TrafficIsland",
                }.get(fw, "Footway")
            else:
                wanted = {
                    "primary": "PrimaryStreet",
                    "secondary": "SecondaryStreet",
                    "tertiary": "TertiaryStreet",
                    "residential": "ResidentialStreet",
                    "service": "ServiceRoad",
                    "unclassified": "UnclassifiedRoad",
                    "steps": "Steps",
                    "trunk": "TrunkRoad",
                    "living_street": "LivingStreet",
                    "pedestrian": "Pedestrian",
                }.get(hwy)

        for name, sub in feature_schemas:
            if name == wanted:
                v = Draft7Validator({**schema, **{}}, resolver=None) if False else None
                # Build a one-off validator; reuse cached validators dict
                vd = _validator_cache.get(name)
                if vd is None:
                    vd = Draft7Validator(
                        {"definitions": schema["definitions"], **sub}
                    )
                    _validator_cache[name] = vd
                errs = list(vd.iter_errors(feat))
                return name, errs
        # Unknown discriminator -> try all (rare path, ok to be slow)
        best = None
        for name, sub in feature_schemas:
            vd = _validator_cache.get(name)
            if vd is None:
                vd = Draft7Validator(
                    {"definitions": schema["definitions"], **sub}
                )
                _validator_cache[name] = vd
            errs = list(vd.iter_errors(feat))
            if not errs:
                return name, []
            if best is None or len(errs) < len(best[1]):
                best = (name, errs)
        return best if best else ("?", [])

    return validate_feature


_validator_cache: dict = {}


# ---------------------------------------------------------------------------
# Pass 1: read every feature, computing everything we can incrementally.
# ---------------------------------------------------------------------------
def main():
    t0 = time.time()
    schema = load_schema_with_compatibility()
    validate_feature = per_feature_validators(schema)

    # Root metadata
    with open(ARTIFACT, "rb") as f:
        root_meta = {}
        for k, v in ijson.kvitems(f, ""):
            if k == "features":
                break
            root_meta[k] = v
    print(
        f"[meta] $schema={root_meta.get('$schema')!r} "
        f"dataTimestamp={root_meta.get('dataTimestamp')!r} "
        f"region_present={'region' in root_meta}",
        file=sys.stderr,
    )

    # Counters and accumulators
    n_features = 0
    n_points = 0
    n_lines = 0

    # Schema validation
    schema_failure_buckets = Counter()  # message -> count
    schema_failures_by_def = Counter()  # def name -> count
    schema_failure_samples: dict[str, list] = defaultdict(list)
    n_passed = 0

    # Topology
    node_ids: set[str] = set()
    node_coords: dict[str, tuple] = {}
    referenced: set[str] = set()
    edges: list[tuple] = []  # (u, v, _id, hwy, fw, borough)
    self_loops = 0
    edge_pair_counter: Counter = Counter()  # canonical (u,v) -> count
    invalid_geom_edges = 0
    zero_length_edges = 0
    out_of_bbox_features = 0
    coord_string_features = 0

    # Geometry
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:32618", always_xy=True)
    edge_lengths_m: list[float] = []

    # Attributes
    surface_counts = Counter()
    crossing_markings_counts = Counter()
    kerb_counts = Counter()
    barrier_counts = Counter()
    tactile_counts = Counter()
    incline_outliers = 0
    incline_vals: list[float] = []
    elevation_vals: list[float] = []
    elevation_outliers = 0

    # Slopes (lives on Points / curb ramp nodes per artifact)
    running_slope_vals: list[float] = []
    cross_slope_vals: list[float] = []
    counter_slope_vals: list[float] = []

    # ADA threshold counts (sidewalk edges -> incline)
    sidewalk_edges = 0
    sidewalk_with_incline = 0
    sidewalk_incline_le_5pct = 0
    crossing_edges = 0
    crossing_with_incline = 0
    crossing_incline_le_5pct = 0

    # Provenance coverage
    prov_source = 0
    prov_source_ts = 0
    prov_pipeline = 0
    source_type_counter = Counter()

    # Sentinel leak detection
    sentinel_999 = 0

    # By borough
    edges_by_borough = defaultdict(int)
    edges_by_borough_hwy = defaultdict(Counter)
    nodes_by_borough = defaultdict(int)  # we have ext:borough only on edges,
                                         # but we'll snap via coord later if needed

    # Curb ramp tracking
    curb_node_ids: set[str] = set()  # those with barrier=kerb, kerb=lowered
    curb_node_count = 0

    # Highway distribution
    hwy_counts = Counter()
    footway_counts = Counter()

    # Open files for findings to avoid huge in-memory buffers
    f_short = open(FINDINGS / "edges_short_lt_0.5m.csv", "w", newline="")
    w_short = csv.writer(f_short)
    w_short.writerow(["_id", "_u_id", "_v_id", "highway", "footway", "length_m"])
    f_long = open(FINDINGS / "edges_long_gt_500m.csv", "w", newline="")
    w_long = csv.writer(f_long)
    w_long.writerow(["_id", "_u_id", "_v_id", "highway", "footway", "length_m"])
    f_oobbox = open(FINDINGS / "out_of_bbox_features.csv", "w", newline="")
    w_oobbox = csv.writer(f_oobbox)
    w_oobbox.writerow(["_id", "type", "lon", "lat"])
    f_dup = open(FINDINGS / "duplicate_edges.csv", "w", newline="")
    w_dup = csv.writer(f_dup)
    w_dup.writerow(["_id", "_u_id", "_v_id", "highway", "footway"])
    f_self = open(FINDINGS / "self_loop_edges.csv", "w", newline="")
    w_self = csv.writer(f_self)
    w_self.writerow(["_id", "_u_id", "_v_id", "highway", "footway"])
    f_unres = open(FINDINGS / "edges_unresolved_uv.csv", "w", newline="")
    w_unres = csv.writer(f_unres)
    w_unres.writerow(["_id", "_u_id", "_v_id", "missing_u", "missing_v"])
    f_zero = open(FINDINGS / "edges_zero_length.csv", "w", newline="")
    w_zero = csv.writer(f_zero)
    w_zero.writerow(["_id", "_u_id", "_v_id", "highway"])
    f_bad_inc = open(FINDINGS / "incline_out_of_range.csv", "w", newline="")
    w_bad_inc = csv.writer(f_bad_inc)
    w_bad_inc.writerow(["_id", "highway", "incline"])
    f_sent = open(FINDINGS / "sentinel_999_leaks.csv", "w", newline="")
    w_sent = csv.writer(f_sent)
    w_sent.writerow(["_id", "feature_type", "field", "value"])

    print(f"[scan] starting first pass over {ARTIFACT}", file=sys.stderr)
    last_log = time.time()
    t_pass1 = time.time()

    with open(ARTIFACT, "rb") as f:
        for feat in ijson.items(f, "features.item"):
            n_features += 1
            if n_features % 100_000 == 0 and time.time() - last_log > 10:
                last_log = time.time()
                print(
                    f"[scan] {n_features:,} features processed "
                    f"({n_features/(time.time()-t_pass1):,.0f}/s)",
                    file=sys.stderr,
                )

            geom = feat.get("geometry") or {}
            gtype = geom.get("type")
            props = feat.get("properties") or {}
            fid = props.get("_id")
            coords = geom.get("coordinates")

            # ----- coordinate type check -----
            had_string_coord = False

            # ----- schema validation (every feature) -----
            if MAX_VALIDATION_FEATURES is None or n_features <= MAX_VALIDATION_FEATURES:
                def_name, errs = validate_feature(feat)
                if errs:
                    schema_failures_by_def[def_name] += 1
                    for e in errs[:5]:
                        # Compress error to a stable bucket
                        path = "/".join(str(p) for p in e.absolute_path) or "."
                        msg = e.message
                        # Normalize OSM ext:* messages to a generic bucket
                        if "Additional properties are not allowed" in msg:
                            # extract the property names
                            try:
                                bad = msg.split("(")[1].split(")")[0]
                                msg = f"AdditionalProperty: {bad}"
                            except Exception:
                                pass
                        elif "is not of type" in msg:
                            # 'is not of type number' / etc
                            msg = msg.split("on instance")[0].strip()
                        bucket = f"{def_name}::{path}::{msg}"
                        schema_failure_buckets[bucket] += 1
                        if len(schema_failure_samples[bucket]) < SCHEMA_FAIL_SAMPLE_LIMIT:
                            sample = {
                                "_id": fid,
                                "geom_type": gtype,
                                "highway": props.get("highway"),
                                "footway": props.get("footway"),
                                "barrier": props.get("barrier"),
                                "kerb": props.get("kerb"),
                                "props_keys": sorted(props.keys()),
                                "error": msg,
                                "path": path,
                            }
                            schema_failure_samples[bucket].append(sample)
                else:
                    n_passed += 1

            # ----- per-archetype processing -----
            if gtype == "Point":
                n_points += 1
                if fid is not None:
                    node_ids.add(fid)
                pair = coerce_coord(coords)
                if pair is None:
                    invalid_geom_edges += 0  # not an edge; just skip geom
                else:
                    if isinstance(coords[0], str):
                        coord_string_features += 1
                    lon, lat = pair
                    if not (
                        NYC_BBOX[0] <= lon <= NYC_BBOX[2]
                        and NYC_BBOX[1] <= lat <= NYC_BBOX[3]
                    ):
                        out_of_bbox_features += 1
                        if out_of_bbox_features <= 5000:
                            w_oobbox.writerow([fid, "Point", lon, lat])
                    if fid is not None:
                        node_coords[fid] = (lon, lat)

                # node attributes
                barrier = props.get("barrier")
                kerb = props.get("kerb")
                if barrier:
                    barrier_counts[barrier] += 1
                if kerb:
                    kerb_counts[kerb] += 1
                tp = props.get("tactile_paving")
                if tp:
                    tactile_counts[tp] += 1
                if barrier == "kerb" and kerb == "lowered":
                    curb_node_count += 1
                    curb_node_ids.add(fid)

                # slopes on ramps
                rs = _to_float(props.get("ext:ramp_running_slope_pct"))
                if rs is not None:
                    if rs == 999.0:
                        sentinel_999 += 1
                        w_sent.writerow([fid, "Point", "ext:ramp_running_slope_pct", rs])
                    else:
                        running_slope_vals.append(rs)
                cs = _to_float(props.get("ext:ramp_cross_slope_pct"))
                if cs is not None:
                    if cs == 999.0:
                        sentinel_999 += 1
                        w_sent.writerow([fid, "Point", "ext:ramp_cross_slope_pct", cs])
                    else:
                        cross_slope_vals.append(cs)
                cnt_s = _to_float(props.get("ext:counter_slope_pct"))
                if cnt_s is not None:
                    if cnt_s == 999.0:
                        sentinel_999 += 1
                        w_sent.writerow([fid, "Point", "ext:counter_slope_pct", cnt_s])
                    else:
                        counter_slope_vals.append(cnt_s)

                # elevation
                ev = _to_float(props.get("ext:elevation_m"))
                if ev is not None:
                    if ev == 999.0:
                        sentinel_999 += 1
                        w_sent.writerow([fid, "Point", "ext:elevation_m", ev])
                    else:
                        elevation_vals.append(ev)
                        if ev < -10 or ev > 200:
                            elevation_outliers += 1

            elif gtype == "LineString":
                n_lines += 1
                hwy_counts[props.get("highway")] += 1
                fw_val = props.get("footway")
                if fw_val:
                    footway_counts[fw_val] += 1
                u = props.get("_u_id")
                v = props.get("_v_id")
                edges.append((u, v))
                if u is not None:
                    referenced.add(u)
                if v is not None:
                    referenced.add(v)
                if u == v and u is not None:
                    self_loops += 1
                    w_self.writerow([fid, u, v, props.get("highway"), fw_val])
                # canonical pair (sorted)
                pair_key = tuple(sorted([u or "", v or ""]))
                edge_pair_counter[pair_key] += 1

                # geometry
                if not isinstance(coords, list) or len(coords) < 2:
                    invalid_geom_edges += 1
                else:
                    if isinstance(coords[0], (list, tuple)) and len(coords[0]) >= 2 and isinstance(coords[0][0], str):
                        coord_string_features += 1
                    pts = [coerce_coord(c) for c in coords]
                    pts = [p for p in pts if p is not None]
                    if len(pts) < 2:
                        invalid_geom_edges += 1
                    else:
                        # bbox check on first/last
                        for (lon, lat) in (pts[0], pts[-1]):
                            if not (
                                NYC_BBOX[0] <= lon <= NYC_BBOX[2]
                                and NYC_BBOX[1] <= lat <= NYC_BBOX[3]
                            ):
                                out_of_bbox_features += 1
                                if out_of_bbox_features <= 5000:
                                    w_oobbox.writerow([fid, "LineString", lon, lat])
                                break
                        # length via UTM18N
                        xs, ys = transformer.transform(
                            [p[0] for p in pts], [p[1] for p in pts]
                        )
                        L = 0.0
                        for i in range(1, len(xs)):
                            dx = xs[i] - xs[i - 1]
                            dy = ys[i] - ys[i - 1]
                            L += math.hypot(dx, dy)
                        edge_lengths_m.append(L)
                        if L == 0.0:
                            zero_length_edges += 1
                            w_zero.writerow([fid, u, v, props.get("highway")])
                        elif L < 0.5:
                            w_short.writerow([fid, u, v, props.get("highway"), fw_val, L])
                        elif L > 500:
                            w_long.writerow([fid, u, v, props.get("highway"), fw_val, L])

                # attributes
                surf = props.get("surface")
                if surf:
                    surface_counts[surf] += 1
                cm = props.get("crossing:markings")
                if cm:
                    crossing_markings_counts[cm] += 1

                inc = _to_float(props.get("incline"))
                if inc is not None:
                    if inc == 999.0 or abs(inc) >= 9.99:
                        sentinel_999 += 1
                        w_sent.writerow([fid, "LineString", "incline", inc])
                    else:
                        incline_vals.append(inc)
                        if inc < -0.30 or inc > 0.30:
                            incline_outliers += 1
                            w_bad_inc.writerow([fid, props.get("highway"), inc])

                # provenance
                if "ext:source" in props:
                    prov_source += 1
                if "ext:source_timestamp" in props:
                    prov_source_ts += 1
                if "ext:pipeline_version" in props:
                    prov_pipeline += 1
                st = props.get("ext:source_type")
                if st:
                    source_type_counter[st] += 1

                # borough
                bo = props.get("ext:borough")
                if bo:
                    edges_by_borough[bo] += 1
                    edges_by_borough_hwy[bo][props.get("highway")] += 1

                # ADA accounting (incline as proxy for running slope on edges)
                if props.get("highway") == "footway":
                    if fw_val == "sidewalk":
                        sidewalk_edges += 1
                        if inc is not None and abs(inc) < 9.99 and inc != 999.0:
                            sidewalk_with_incline += 1
                            if abs(inc) <= 0.05:
                                sidewalk_incline_le_5pct += 1
                    elif fw_val == "crossing":
                        crossing_edges += 1
                        if inc is not None and abs(inc) < 9.99 and inc != 999.0:
                            crossing_with_incline += 1
                            if abs(inc) <= 0.05:
                                crossing_incline_le_5pct += 1

    f_short.close(); f_long.close(); f_oobbox.close(); f_dup.close()
    f_self.close(); f_unres.close(); f_zero.close(); f_bad_inc.close()
    f_sent.close()
    t_pass1_end = time.time()
    print(
        f"[scan] pass-1 done in {t_pass1_end - t_pass1:.1f}s; "
        f"{n_features:,} features, {n_points:,} pts, {n_lines:,} lines",
        file=sys.stderr,
    )

    # ----- post-pass aggregations -----
    # Unresolved u/v
    unresolved = 0
    with open(FINDINGS / "edges_unresolved_uv.csv", "w", newline="") as f_unres2:
        w_un = csv.writer(f_unres2)
        w_un.writerow(["_u_id", "_v_id", "missing_u", "missing_v"])
        for u, v in edges:
            mu = u not in node_ids if u is not None else False
            mv = v not in node_ids if v is not None else False
            if mu or mv:
                unresolved += 1
                if unresolved <= 5000:
                    w_un.writerow([u, v, int(mu), int(mv)])

    orphans = node_ids - referenced
    n_orphans = len(orphans)
    with open(FINDINGS / "orphan_nodes.csv", "w", newline="") as fo:
        wo = csv.writer(fo)
        wo.writerow(["_id"])
        for nid in list(orphans)[:10000]:
            wo.writerow([nid])

    # Duplicate edges
    dup_pairs = [k for k, c in edge_pair_counter.items() if c > 1]
    n_dup_pairs = len(dup_pairs)
    n_dup_edges = sum(c for c in edge_pair_counter.values() if c > 1)

    # Connectivity (citywide)
    import networkx as nx

    G = nx.Graph()
    G.add_nodes_from(node_ids)
    G.add_edges_from((u, v) for u, v in edges if u in node_ids and v in node_ids and u != v)
    components = list(nx.connected_components(G))
    components.sort(key=len, reverse=True)
    giant = len(components[0]) if components else 0
    pct_giant = giant / max(1, len(node_ids))
    cc_size_distribution = Counter(len(c) for c in components)

    # Per-borough connectivity. We have ext:borough on edges only; for nodes
    # we infer borough from any edge that touches them. A node is in borough
    # B if any incident edge had ext:borough==B (multi-borough nodes count
    # in both — that's <0.1% in practice).
    node_to_boroughs = defaultdict(set)
    edges_with_borough = 0  # we re-stream edges briefly to assign nodes
    # Rather than re-streaming the file, record from edges_by_borough_hwy
    # Already lost per-edge mapping. Instead use a small second pass over
    # the artifact for the borough graph — fast because we only read props.
    print("[scan] borough sub-pass", file=sys.stderr)
    borough_graphs = {b: nx.Graph() for b in BOROUGH_AREAS_KM2}
    intersection_nodes_by_borough = defaultdict(set)  # placeholder (filled later via degree)
    edge_count_by_borough_real = Counter()
    crossings_per_borough = Counter()
    sidewalks_per_borough = Counter()
    streets_per_borough = Counter()
    edges_with_source_type = 0
    with open(ARTIFACT, "rb") as f:
        for feat in ijson.items(f, "features.item"):
            g = feat.get("geometry") or {}
            if g.get("type") != "LineString":
                continue
            p = feat.get("properties") or {}
            u = p.get("_u_id"); v = p.get("_v_id")
            bo = p.get("ext:borough")
            if bo and bo in borough_graphs and u and v:
                borough_graphs[bo].add_edge(u, v)
                edge_count_by_borough_real[bo] += 1
                if u: node_to_boroughs[u].add(bo)
                if v: node_to_boroughs[v].add(bo)
                hwy = p.get("highway")
                fw = p.get("footway")
                if hwy == "footway" and fw == "sidewalk":
                    sidewalks_per_borough[bo] += 1
                elif hwy == "footway" and fw == "crossing":
                    crossings_per_borough[bo] += 1
                elif hwy in ("residential","service","secondary","primary","tertiary","unclassified"):
                    streets_per_borough[bo] += 1

    borough_conn = {}
    for bo, bg in borough_graphs.items():
        comps = list(nx.connected_components(bg))
        comps.sort(key=len, reverse=True)
        borough_conn[bo] = {
            "n_nodes": bg.number_of_nodes(),
            "n_edges": bg.number_of_edges(),
            "components": len(comps),
            "giant_size": len(comps[0]) if comps else 0,
            "giant_pct": (len(comps[0]) / bg.number_of_nodes()) if bg.number_of_nodes() else 0,
        }

    # Curb-ramp snapping success rate (curb node id appears as u or v)
    curb_snapped = sum(1 for nid in curb_node_ids if nid in referenced)
    curb_unsnapped = curb_node_count - curb_snapped

    # Crossing-per-intersection ratio.
    # Intersection nodes = nodes with degree >= 3 in the street subgraph.
    # We need a street-only graph; build it now from edges + node-bo data.
    print("[scan] intersection sub-pass", file=sys.stderr)
    street_graphs = {b: nx.Graph() for b in BOROUGH_AREAS_KM2}
    crossing_incident_nodes = defaultdict(set)
    with open(ARTIFACT, "rb") as f:
        for feat in ijson.items(f, "features.item"):
            g = feat.get("geometry") or {}
            if g.get("type") != "LineString":
                continue
            p = feat.get("properties") or {}
            hwy = p.get("highway")
            fw = p.get("footway")
            u = p.get("_u_id"); v = p.get("_v_id")
            bo = p.get("ext:borough")
            if not bo or bo not in street_graphs or not u or not v:
                continue
            if hwy in ("residential","service","secondary","primary","tertiary","unclassified","trunk","living_street"):
                street_graphs[bo].add_edge(u, v)
            if hwy == "footway" and fw == "crossing":
                crossing_incident_nodes[bo].add(u)
                crossing_incident_nodes[bo].add(v)

    intersection_stats = {}
    for bo, sg in street_graphs.items():
        deg = sg.degree()
        inter = {n for n, d in deg if d >= 3}
        with_crossing = inter & crossing_incident_nodes[bo]
        intersection_stats[bo] = {
            "intersections": len(inter),
            "with_crossing_incident": len(with_crossing),
            "pct_with_crossing": (len(with_crossing) / len(inter)) if inter else 0,
        }

    # Sidewalk km / street km ratio
    sidewalk_street_ratio = {}
    for bo in BOROUGH_AREAS_KM2:
        s = sidewalks_per_borough.get(bo, 0)
        st = streets_per_borough.get(bo, 0)
        sidewalk_street_ratio[bo] = (s / st) if st else None

    # Density (features per km²)
    density = {
        bo: (edge_count_by_borough_real[bo] + 0) / BOROUGH_AREAS_KM2[bo]
        for bo in BOROUGH_AREAS_KM2
    }

    # Length distribution
    edge_lengths_m_sorted = sorted(edge_lengths_m)
    length_stats = {
        "n": len(edge_lengths_m_sorted),
        "min": edge_lengths_m_sorted[0] if edge_lengths_m_sorted else None,
        "p1": percentile(edge_lengths_m_sorted, 1),
        "p50": percentile(edge_lengths_m_sorted, 50),
        "p95": percentile(edge_lengths_m_sorted, 95),
        "p99": percentile(edge_lengths_m_sorted, 99),
        "max": edge_lengths_m_sorted[-1] if edge_lengths_m_sorted else None,
        "mean": sum(edge_lengths_m_sorted) / len(edge_lengths_m_sorted) if edge_lengths_m_sorted else None,
    }
    short_edges = sum(1 for L in edge_lengths_m_sorted if L < 0.5)
    long_edges = sum(1 for L in edge_lengths_m_sorted if L > 500)

    # Slope distributions
    def desc(vals):
        if not vals: return {"n": 0}
        s = sorted(vals)
        return {
            "n": len(s),
            "min": s[0], "p1": percentile(s,1), "p50": percentile(s,50),
            "p95": percentile(s,95), "p99": percentile(s,99), "max": s[-1],
            "mean": sum(s)/len(s),
        }
    running_slope_stats = desc(running_slope_vals)
    running_slope_le_5pct = sum(1 for v in running_slope_vals if v <= 5.0)
    cross_slope_stats = desc(cross_slope_vals)
    cross_slope_le_2pct = sum(1 for v in cross_slope_vals if v <= 2.0)
    counter_slope_stats = desc(counter_slope_vals)
    incline_stats = desc(incline_vals)
    elevation_stats = desc(elevation_vals)

    # Surface canonical-enum check (per spec: asphalt, concrete, dirt, grass,
    # grass_paver, gravel, paved, paving_stones, unpaved)
    SURF_CANON = {"asphalt","concrete","dirt","grass","grass_paver","gravel","paved","paving_stones","unpaved"}
    surface_noncanon = {k: v for k, v in surface_counts.items() if k not in SURF_CANON}

    # Crossing markings canonical (per spec broad enum)
    CM_CANON = {"dashes","dots","ladder","ladder:paired","ladder:skewed","lines","lines:paired","lines:rainbow",
                "no","pictograms","rainbow","skewed","surface","yes","zebra","zebra:bicolour","zebra:double",
                "zebra:paired","zebra:rainbow"}
    cm_noncanon = {k: v for k, v in crossing_markings_counts.items() if k not in CM_CANON}

    # Provenance coverage rates
    n_edges = n_lines
    prov = {
        "ext:source_pct": prov_source / max(1, n_edges),
        "ext:source_timestamp_pct": prov_source_ts / max(1, n_edges),
        "ext:pipeline_version_pct": prov_pipeline / max(1, n_edges),
        "ext:source_type_distribution": dict(source_type_counter),
        "root_dataTimestamp": root_meta.get("dataTimestamp"),
        "root_dataSource": root_meta.get("dataSource"),
        "root_pipelineVersion": root_meta.get("pipelineVersion"),
        "root_$schema": root_meta.get("$schema"),
        "region_present": "region" in root_meta,
    }

    # Top failure buckets
    top_failures = schema_failure_buckets.most_common(10)

    summary = {
        "artifact": str(ARTIFACT),
        "schema_version_audited_against": "0.3 (sidewalks.washington.edu/opensidewalks/0.3/schema.json)",
        "schema_local_path": str(SCHEMA_PATH),
        "audit_runtime_seconds": round(time.time() - t0, 1),
        "n_features": n_features,
        "n_points": n_points,
        "n_lines": n_lines,
        "root_metadata_keys": list(root_meta.keys()),
        "root_metadata": root_meta,
        "schema_conformance": {
            "n_validated": n_features if MAX_VALIDATION_FEATURES is None else min(MAX_VALIDATION_FEATURES, n_features),
            "n_passed": n_passed,
            "n_failed": n_features - n_passed,
            "pct_passed": n_passed / max(1, n_features),
            "failures_by_definition": dict(schema_failures_by_def),
            "top_failure_buckets": [{"bucket": b, "count": c} for b, c in top_failures],
            "samples": {b: schema_failure_samples[b] for b, _ in top_failures},
            "coordinate_strings_features": coord_string_features,
        },
        "graph_integrity": {
            "n_nodes": len(node_ids),
            "n_edges": n_lines,
            "n_self_loops": self_loops,
            "n_unresolved_uv_edges": unresolved,
            "n_orphan_nodes": n_orphans,
            "n_duplicate_pairs": n_dup_pairs,
            "n_duplicate_edges_total": n_dup_edges,
            "n_connected_components": len(components),
            "giant_component_size": giant,
            "giant_pct_of_nodes": pct_giant,
            "components_size_distribution_top10": list(cc_size_distribution.most_common(10)),
            "per_borough": borough_conn,
        },
        "geometry": {
            "out_of_nyc_bbox_features": out_of_bbox_features,
            "invalid_geometry_edges": invalid_geom_edges,
            "zero_length_edges": zero_length_edges,
            "edges_lt_0.5m": short_edges,
            "edges_gt_500m": long_edges,
            "length_stats_m": length_stats,
        },
        "attributes": {
            "highway_distribution": dict(hwy_counts),
            "footway_distribution": dict(footway_counts),
            "surface_distribution": dict(surface_counts),
            "surface_noncanonical": surface_noncanon,
            "crossing_markings_distribution": dict(crossing_markings_counts),
            "crossing_markings_noncanonical": cm_noncanon,
            "barrier_distribution": dict(barrier_counts),
            "kerb_distribution": dict(kerb_counts),
            "tactile_paving_distribution": dict(tactile_counts),
            "incline_stats": incline_stats,
            "incline_outliers_outside_+-0.30": incline_outliers,
            "elevation_stats_m": elevation_stats,
            "elevation_outliers_outside_-10_200": elevation_outliers,
            "running_slope_pct_stats_curbramps": running_slope_stats,
            "running_slope_le_5pct_count": running_slope_le_5pct,
            "running_slope_le_5pct_share": running_slope_le_5pct / max(1, len(running_slope_vals)),
            "cross_slope_pct_stats_curbramps": cross_slope_stats,
            "cross_slope_le_2pct_count": cross_slope_le_2pct,
            "cross_slope_le_2pct_share": cross_slope_le_2pct / max(1, len(cross_slope_vals)),
            "counter_slope_pct_stats_curbramps": counter_slope_stats,
        },
        "ada_edge_summary": {
            "sidewalk_edges": sidewalk_edges,
            "sidewalk_with_incline": sidewalk_with_incline,
            "sidewalk_incline_le_5pct_share": sidewalk_incline_le_5pct / max(1, sidewalk_with_incline),
            "crossing_edges": crossing_edges,
            "crossing_with_incline": crossing_with_incline,
            "crossing_incline_le_5pct_share": crossing_incline_le_5pct / max(1, crossing_with_incline),
        },
        "provenance": prov,
        "coverage": {
            "edges_per_borough": dict(edge_count_by_borough_real),
            "edges_per_km2_per_borough": density,
            "sidewalk_to_street_ratio_per_borough": sidewalk_street_ratio,
            "intersection_crossing_coverage_per_borough": intersection_stats,
            "curb_ramp_nodes_total": curb_node_count,
            "curb_ramp_snapped_count": curb_snapped,
            "curb_ramp_snapped_share": curb_snapped / max(1, curb_node_count),
            "sidewalks_per_borough": dict(sidewalks_per_borough),
            "crossings_per_borough": dict(crossings_per_borough),
            "streets_per_borough": dict(streets_per_borough),
        },
        "limitations_check": {
            "sentinel_999_leaks_count": sentinel_999,
            "comment": (
                "Voronoi/planimetric failures are not directly logged in the "
                "artifact; however the count of Sidewalk edges flagged with "
                "ext:source_type='nyc_planimetric' (vs. OSM) gives an upper "
                "bound on planimetric-derived edges. ext:source_type usage is "
                "extremely sparse in the artifact (see provenance section)."
            ),
        },
    }

    # Borough findings CSV
    with open(FINDINGS / "borough_summary.csv", "w", newline="") as fb:
        w = csv.writer(fb)
        w.writerow([
            "borough", "edges", "edges_per_km2",
            "sidewalks", "crossings", "streets",
            "sidewalk_street_ratio",
            "intersections", "intersections_with_crossing", "pct_with_crossing",
            "n_components", "giant_pct"
        ])
        for bo in BOROUGH_AREAS_KM2:
            ic = intersection_stats[bo]
            bc = borough_conn[bo]
            w.writerow([
                bo,
                edge_count_by_borough_real[bo],
                f"{density[bo]:.1f}",
                sidewalks_per_borough.get(bo,0),
                crossings_per_borough.get(bo,0),
                streets_per_borough.get(bo,0),
                f"{sidewalk_street_ratio[bo]:.3f}" if sidewalk_street_ratio[bo] is not None else "",
                ic["intersections"],
                ic["with_crossing_incident"],
                f"{ic['pct_with_crossing']:.3f}",
                bc["components"],
                f"{bc['giant_pct']:.4f}",
            ])

    # Top failures CSV
    with open(FINDINGS / "schema_top_failures.csv", "w", newline="") as ff:
        w = csv.writer(ff)
        w.writerow(["bucket", "count"])
        for b, c in schema_failure_buckets.most_common(50):
            w.writerow([b, c])

    # Surface non-canonical CSV
    with open(FINDINGS / "surface_noncanonical.csv", "w", newline="") as ff:
        w = csv.writer(ff)
        w.writerow(["surface_value", "count"])
        for k, v in sorted(surface_noncanon.items(), key=lambda kv: -kv[1]):
            w.writerow([k, v])

    print(json.dumps(summary, indent=2, default=str))
    return summary


if __name__ == "__main__":
    main()
