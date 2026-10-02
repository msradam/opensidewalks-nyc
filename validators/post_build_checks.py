"""Checks the official validator does not do, on a finished build, city-wide
and by borough. Each one caught a defect in v0.3.1-nyc.1 that passed the
validator: zero elevations, cut boroughs, false tactile_paving, detached and
missing ramps, doubled widths, one-way gap-fill, long coordinates.

usage: python validators/post_build_checks.py OSW_GEOJSON OUT_JSON [DATA_DIR]

DATA_DIR (default data) is the build's own data directory: the borough
polygons, the ramp survey as downloaded and the cleaned sidewalk polygons are
read from it. Writes OUT_JSON, a pickle of the node and edge tables beside it,
and the width transect sample as CSV.
"""
import json
import math
import pickle
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components
from shapely.geometry import LineString, Point

osw, out_path = Path(sys.argv[1]), Path(sys.argv[2])
data = Path(sys.argv[3]) if len(sys.argv) > 3 else Path("data")
CODES = {"manhattan": "MN", "brooklyn": "BK", "queens": "QN", "bronx": "BX", "bronx county": "BX",
         "the bronx": "BX", "staten island": "SI"}
res = {"input": str(osw)}

fc = json.loads(osw.read_text())
res["root"] = {k: v for k, v in fc.items() if k not in ("features", "region")}
nrows, erows, bad_precision = [], [], 0
def dec_ok(x):
    return round(x, 7) == x
for f in fc["features"]:
    p, g = f["properties"], f["geometry"]
    if g["type"] == "Point":
        c = g["coordinates"]
        bad_precision += not (dec_ok(c[0]) and dec_ok(c[1]))
        nrows.append((p["_id"], c[0], c[1], p.get("ext:elevation_m"), p.get("barrier"), p.get("kerb"),
                      p.get("tactile_paving"), p.get("ext:ramp_id"), p.get("ext:borough"), p.get("ext:source"),
                      p.get("ext:running_slope_pct"), p.get("ext:cross_slope_pct"), p.get("ext:counter_slope_pct"),
                      p.get("ext:source_timestamp") is not None))
    else:
        c = g["coordinates"]
        bad_precision += not all(dec_ok(x) for pt in c for x in pt)
        L = sum(math.hypot((c[i + 1][0] - c[i][0]) * 111320 * math.cos(math.radians(c[i][1])),
                           (c[i + 1][1] - c[i][1]) * 111320) for i in range(len(c) - 1))
        hw, fw = p.get("highway"), p.get("footway")
        kind = fw if (hw == "footway" and fw in ("sidewalk", "crossing")) else ("footway" if hw == "footway" else ("steps" if hw == "steps" else "street"))
        erows.append((p["_id"], p["_u_id"], p["_v_id"], kind, hw, p.get("ext:borough"), p.get("ext:source"),
                      p.get("incline"), p.get("width"), L, p.get("name"), c[0][0], c[0][1], c[-1][0], c[-1][1], len(c),
                      p.get("surface"), p.get("crossing:markings"), p.get("ext:osm_id") is not None,
                      p.get("ext:source_timestamp") is not None))
del fc
N = pd.DataFrame(nrows, columns=["id", "lon", "lat", "elev", "barrier", "kerb", "tactile", "ramp", "borough", "source",
                                 "run", "cross", "counter", "has_ts"])
E = pd.DataFrame(erows, columns=["id", "u", "v", "kind", "highway", "borough", "source", "incline", "width", "length",
                                 "name", "x0", "y0", "x1", "y1", "npts", "surface", "markings", "has_osm_id", "has_ts"])
del nrows, erows
out_path.with_suffix(".tables.pkl").write_bytes(pickle.dumps((N, E)))

# --- counts and form -------------------------------------------------------
res["counts"] = {"features": len(N) + len(E), "nodes": len(N), "edges": len(E),
                 "edges_by_kind": E.kind.value_counts().to_dict(),
                 "edges_by_source": E.source.value_counts().to_dict(),
                 "edges_by_borough": E.borough.fillna("none").value_counts().to_dict(),
                 "street_edges_by_highway": E[E.kind == "street"].highway.value_counts().to_dict(),
                 "duplicate_node_ids": int(N.id.duplicated().sum()), "duplicate_edge_ids": int(E.id.duplicated().sum())}
nid = pd.Index(N.id)
res["form"] = {"coordinates_over_7_decimals": int(bad_precision),
               "edges_with_unresolved_node": int((~E.u.isin(nid) | ~E.v.isin(nid)).sum()),
               "self_loop_edges": int((E.u == E.v).sum()),
               "zero_length_edges": int((E.length == 0).sum()),
               "edges_over_500m": int((E.length > 500).sum())}
ncoord = N.set_index("id")[["lon", "lat"]]
eu = ncoord.reindex(E.u.values).values; ev = ncoord.reindex(E.v.values).values
res["form"]["edge_ends_off_their_node"] = int(((eu[:, 0] != E.x0.values) | (eu[:, 1] != E.y0.values) |
                                               (ev[:, 0] != E.x1.values) | (ev[:, 1] != E.y1.values)).sum())
pairs = set(zip(E.u, E.v))
rev = np.fromiter(((b, a) in pairs for a, b in zip(E.u, E.v)), bool, len(E))
res["one_direction_only"] = E[~rev].kind.value_counts().to_dict()
res["provenance"] = {"edges_with_source": int(E.source.notna().sum()), "edges_with_timestamp": int(E.has_ts.sum()),
                     "nodes_with_source": int(N.source.notna().sum()), "nodes_with_timestamp": int(N.has_ts.sum()),
                     "osm_edges_with_osm_id": int((E.has_osm_id & (E.source == "osm_walk")).sum()),
                     "osm_edges": int((E.source == "osm_walk").sum())}

# --- borough of every node, by point in polygon ----------------------------
boro = gpd.read_file(data / "raw/nyc_boroughs/boroughs.geojson")
name_col = next(c for c in ("boro_name", "BoroName", "name") if c in boro.columns)
boro["code"] = boro[name_col].str.lower().str.replace("_", " ").map(CODES)
pts = gpd.GeoDataFrame(N[["id"]], geometry=gpd.points_from_xy(N.lon, N.lat), crs=4326)
j = gpd.sjoin(pts, boro[["code", "geometry"]], how="left", predicate="within")
j = j[~j.index.duplicated()]
N["poly_boro"] = j["code"].values
res["nodes_by_borough_polygon"] = N.poly_boro.fillna("outside").value_counts().to_dict()

# --- elevation and incline -------------------------------------------------
el = {}
for b, d in list(N.groupby(N.poly_boro.fillna("outside"))) + [("all", N)]:
    el[b] = {"nodes": len(d), "with_elevation": int(d.elev.notna().sum()),
             "exactly_zero": int((d.elev == 0).sum()), "share_zero": round(float((d.elev == 0).mean()), 4),
             "min": None if d.elev.notna().sum() == 0 else float(d.elev.min()),
             "max": None if d.elev.notna().sum() == 0 else float(d.elev.max()),
             "median": None if d.elev.notna().sum() == 0 else float(d.elev.median())}
res["elevation"] = el
inc = {}
for k, d in list(E.groupby("kind")) + [("all", E)]:
    x = d.incline.dropna()
    inc[k] = {"edges": len(d), "with_incline": len(x), "share_with_incline": round(len(x) / len(d), 4),
              "exactly_zero": int((x == 0).sum()), "share_zero": round(float((x == 0).mean()), 4),
              "p01": round(float(x.quantile(.01)), 4), "p50_abs": round(float(x.abs().median()), 4), "p99": round(float(x.quantile(.99)), 4),
              "share_abs_over_5pct": round(float((x.abs() > .05).mean()), 4),
              "share_outside_wheelchair_limits": round(float(((x > .083) | (x < -.1)).mean()), 4)}
res["incline"] = inc
walk = E[E.kind.isin(["sidewalk", "crossing", "footway"]) & E.incline.notna()]
bins = [0, 2, 5, 10, 20, 50, 1e9]
res["incline_outside_limits_by_edge_length"] = {
    f"{lo} to {hi if hi < 1e9 else 'inf'} m": {"edges": len(d), "share_outside": round(float(((d.incline > .083) | (d.incline < -.1)).mean()), 4)}
    for lo, hi in zip(bins[:-1], bins[1:]) for d in [walk[(walk.length >= lo) & (walk.length < hi)]] if len(d)}
res["incline_by_borough"] = {b: {"edges": len(d), "share_with_incline": round(float(d.incline.notna().mean()), 4),
                                 "share_zero": round(float((d.incline == 0).mean()), 4)}
                             for b, d in E[E.kind != "street"].groupby(E.borough.fillna("none"))}

# --- pedestrian graph components -------------------------------------------
P = E[E.kind != "street"]
codes, uniq = pd.factorize(np.r_[P.u.values, P.v.values]); m = len(P)
k, lab = connected_components(coo_matrix((np.ones(m), (codes[:m], codes[m:])), shape=(len(uniq), len(uniq))), directed=False)
sizes = np.bincount(lab); order = np.argsort(-sizes); giant = order[0]
pn = pd.DataFrame({"id": uniq, "comp": lab}).merge(N[["id", "poly_boro"]], on="id", how="left")
comp = {"pedestrian_nodes": len(uniq), "pedestrian_edges": m, "components": int(k),
        "largest": int(sizes[giant]), "largest_share": round(float(sizes[giant] / len(uniq)), 4),
        "next_five": [int(sizes[i]) for i in order[1:6]],
        "components_of_10_or_more_nodes": int((sizes >= 10).sum()), "by_borough": {}}
for b, d in pn.groupby(pn.poly_boro.fillna("outside")):
    top = d.comp.value_counts()
    comp["by_borough"][b] = {"pedestrian_nodes": len(d), "in_citywide_largest": int((d.comp == giant).sum()),
                             "share_in_citywide_largest": round(float((d.comp == giant).mean()), 4),
                             "largest_component_of_this_borough_is_citywide_largest": bool(top.index[0] == giant),
                             "share_in_own_largest": round(float(top.iloc[0] / len(d)), 4)}
res["components"] = comp
node_comp = dict(zip(uniq, lab))

# Pedestrian edges whose two ends are in different boroughs: the joins.
pb = N.set_index("id").poly_boro
X = P.assign(bu=pb.reindex(P.u.values).values, bv=pb.reindex(P.v.values).values)
X = X[X.bu.notna() & X.bv.notna() & (X.bu != X.bv)]
joins = []
for (pair, name), d in X.assign(pair=[" and ".join(sorted(t)) for t in zip(X.bu, X.bv)]).groupby(["pair", X.name.fillna("(unnamed)")]):
    cs = {node_comp[a] for a in d.u} | {node_comp[b] for b in d.v}
    joins.append({"boroughs": pair, "name": name, "edges": len(d), "in_citywide_largest_component": bool(giant in cs),
                  "lon": round(float(d.x0.mean()), 5), "lat": round(float(d.y0.mean()), 5)})
res["borough_crossing_pedestrian_edges"] = {"edges": len(X), "by_pair": X.assign(pair=[" and ".join(sorted(t)) for t in zip(X.bu, X.bv)]).pair.value_counts().to_dict(),
                                            "named_crossings": sorted(joins, key=lambda r: (r["boroughs"], -r["edges"]))}

# --- curb ramps ------------------------------------------------------------
curb = N[N.barrier == "kerb"].copy()
ref = set(E.u) | set(E.v); pref = set(P.u) | set(P.v)
cr = E[E.kind == "crossing"]; xref = set(cr.u) | set(cr.v)
raw = pd.DataFrame([f["properties"] for f in json.loads((data / "raw/nyc_dot_ramps/nyc_dot_ramps.geojson").read_text())["features"]])
raw = raw.rename(columns={"rampid": "RampID", "dws_conditions": "DWS_CONDITIONS"}).astype({"RampID": str}).set_index("RampID")
exp = {"Good Condition": "yes", "Defective": "yes", "Off Ramp - Good": "yes", "Off Ramp-Defective": "yes", "Missing": "no"}
want = curb.ramp.map(raw.DWS_CONDITIONS.map(exp))
got = curb.tactile
S = {555.0, 777.0, 888.0, 999.0}
res["curb"] = {
    "survey_rows": len(raw), "curb_nodes": len(curb), "distinct_ramp_ids": int(curb.ramp.nunique()),
    "ramp_ids_not_in_survey": int((~curb.ramp.isin(raw.index)).sum()),
    "survey_ramps_missing_from_file": len(set(raw.index) - set(curb.ramp)),
    "attached_to_any_edge": int(curb.id.isin(ref).sum()), "attached_to_pedestrian_edge": int(curb.id.isin(pref).sum()),
    "attached_to_crossing": int(curb.id.isin(xref).sum()),
    "share_attached_to_pedestrian_edge": round(float(curb.id.isin(pref).mean()), 4),
    "tactile_in_file": curb.tactile.fillna("untagged").value_counts().to_dict(),
    "survey_dws": raw.DWS_CONDITIONS.fillna("blank").value_counts().to_dict(),
    "tactile_agrees_with_survey": int(((want == got) | (want.isna() & got.isna())).sum()),
    "tactile_disagrees": int((~((want == got) | (want.isna() & got.isna()))).sum()),
    "nodes_with_sentinel_slope": int((curb.run.isin(S) | curb.cross.isin(S) | curb.counter.isin(S)).sum()),
    "with_running_slope": int(curb.run.notna().sum()),
    "running_within_1_in_12": int((curb.run.abs() <= 100 / 12).sum()),
    "cross_within_1_in_48": int((curb.cross.abs() <= 100 / 48).sum()), "with_cross_slope": int(curb.cross.notna().sum()),
    "crossing_edges": len(cr), "crossing_edges_with_curb_node": int((cr.u.isin(set(curb.id)) | cr.v.isin(set(curb.id))).sum()),
    "by_borough": {b: {"curb_nodes": len(d), "attached_to_pedestrian_edge": int(d.id.isin(pref).sum()),
                       "share": round(float(d.id.isin(pref).mean()), 4),
                       "tactile_yes": int((d.tactile == "yes").sum()), "tactile_no": int((d.tactile == "no").sum())}
                   for b, d in curb.groupby(curb.poly_boro.fillna("outside"))}}
res["nodes_not_on_any_edge"] = int((~N.id.isin(ref)).sum())

# --- widths ----------------------------------------------------------------
sw = E[(E.kind == "sidewalk")]
res["width"] = {"sidewalk_edges": len(sw), "with_width": int(sw.width.notna().sum()),
                "median_m": round(float(sw.width.median()), 2),
                "by_borough": {b: {"with_width": int(d.width.notna().sum()), "median_m": round(float(d.width.median()), 2)}
                               for b, d in sw.groupby(sw.borough.fillna("none"))}}
plan = gpd.read_file(data / "clean/nyc_planimetric_sidewalks.geojson").to_crs(32618)
res["planimetric_polygons"] = len(plan)
osm_sw = sw[(sw.source == "osm_walk") & sw.width.notna() & (sw.npts == 2)]
rng = np.random.default_rng(7)
tr = Transformer.from_crs(4326, 32618, always_xy=True)
sidx = plan.sindex; rows = []
for b, d in osm_sw.groupby("borough"):
    take = d.iloc[rng.choice(len(d), min(1500, len(d)), replace=False)]
    for r in take.itertuples():
        x0, y0 = tr.transform(r.x0, r.y0); x1, y1 = tr.transform(r.x1, r.y1)
        L = math.hypot(x1 - x0, y1 - y0)
        if L < 1:
            continue
        mx, my = (x0 + x1) / 2, (y0 + y1) / 2; nx_, ny_ = -(y1 - y0) / L, (x1 - x0) / L
        cut = LineString([(mx - 30 * nx_, my - 30 * ny_), (mx + 30 * nx_, my + 30 * ny_)]); mid = Point(mx, my)
        for jx in sidx.query(mid, predicate="within"):
            inter = plan.geometry.iloc[jx].intersection(cut)
            parts = list(inter.geoms) if hasattr(inter, "geoms") else [inter]
            chord = [q for q in parts if q.geom_type == "LineString" and q.distance(mid) < 1e-6]
            if chord and chord[0].length < 29:
                rows.append((b, r.id, chord[0].length, r.width)); break
W = pd.DataFrame(rows, columns=["borough", "id", "transect", "width"])
W.to_csv(out_path.with_suffix(".width_transects.csv"), index=False)
def wsum(d):
    return {"n": len(d), "median_transect_m": round(float(d.transect.median()), 2), "median_width_m": round(float(d.width.median()), 2),
            "median_abs_error_m": round(float((d.width - d.transect).abs().median()), 2),
            "median_ratio": round(float((d.width / d.transect).median()), 2)}
res["width_vs_transect"] = {"all": wsum(W), **{b: wsum(d) for b, d in W.groupby("borough")}}

# --- gap-fill ---------------------------------------------------------------
gf = E[E.source == "nyc_planimetric_sidewalks"]
gp = set(zip(gf.u, gf.v))
res["gap_fill"] = {"edges": len(gf), "segments": len(gf) // 2, "with_reverse": int(sum((b, a) in gp for a, b in gp)),
                   "km_one_direction": round(float(gf.length.sum() / 2000), 1),
                   "by_borough": gf.borough.fillna("none").value_counts().to_dict(),
                   "ends_on_an_osm_node": int((gf.u.isin(set(E[E.source == "osm_walk"].u) | set(E[E.source == "osm_walk"].v))).sum())}
out_path.write_text(json.dumps(res, indent=1, default=str))
print(json.dumps({k: v for k, v in res.items() if k not in ("borough_crossing_pedestrian_edges",)}, indent=1, default=str)[:6000])
