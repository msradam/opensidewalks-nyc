"""Markdown tables from comparison.json, for the report. usage: python tables.py comparison.json > tables.md"""
import json, sys
t = json.load(open(sys.argv[1]))
AREAS = ["random/BK", "random/QN", "random/MN", "random/BX", "random/SI", "random/citywide", "brownsville/BK16", "landmark/hand-checked", "landmark/structure"]
AREAS = [a for a in AREAS if a in t]
short = lambda a: a.replace("random/", "").replace("brownsville/BK16", "Brownsville").replace("landmark/", "landmark ")
pct = lambda x: "n/a" if x is None else f"{100 * x:.1f}%"
def table(title, rows, cols):
    print(f"\n### {title}\n\n| | " + " | ".join(short(a) for a in cols) + " |\n|---|" + "---|" * len(cols))
    for label, fn in rows:
        print(f"| {label} | " + " | ".join(fn(t[a]) for a in cols) + " |")
print("# Comparison tables\n\nFrom `comparison.json`. Random areas have 2,000 pairs each. Measured pairs are those whose snapped ends lie within 25 m of each other across routers.")
table("Pairs", [("pairs", lambda r: str(r["pairs"])), ("snaps over 25 m apart", lambda r: str(r["pairs_with_snaps_over_25m_apart"])), ("measured", lambda r: str(r["pairs_measured"]))], AREAS)
R = [("this graph, wheelchair", "ours_wheelchair"), ("ORS raw OSM, incline 10 kerb 0.06", "ors_a_rec_i10_k6"), ("ORS raw OSM, incline 6 kerb 0.06", "ors_a_rec_i6_k6"),
     ("ORS raw OSM, no limits given", "ors_a_no_limits"), ("ORS on this graph, strict kerbs, 10/0.06", "ors_b_strict_rec_i10_k6"), ("ORS on this graph, strict kerbs, 6/0.06", "ors_b_strict_rec_i6_k6"),
     ("ORS on this graph, known kerbs only, 10/0.06", "ors_b_known_rec_i10_k6"), ("Valhalla wheelchair type", "valhalla_wheelchair"),
     ("this graph, walk", "ours_walk"), ("ORS raw OSM, foot", "ors_a_foot"), ("Valhalla foot", "valhalla_foot")]
table("Route found, all pairs", [(n, lambda r, k=k: pct(r["found_all_pairs"].get(k))) for n, k in R], AREAS)
table("Route found, measured pairs", [(n, lambda r, k=k: pct(r["found"].get(k))) for n, k in R], AREAS)
def agree(k):
    def f(r):
        a = r["agreement_with_ours"].get(k)
        return "n/a" if not a else f'{a["both"]} / {a["ours_only"]} / {a["theirs_only"]} / {a["neither"]}'
    return f
table("Found by both / this graph only / the other only / neither (measured pairs)", [(n, agree(k)) for n, k in R[1:8]], AREAS)
def detour(k):
    def f(r):
        d = r["detour_over_own_foot_route"].get(k)
        return "n/a" if not d or d["median"] is None else f'{d["median"]:.2f} ({d["p90"]:.2f}; {pct(d["share_over_1.5"])})'
    return f
table("Detour over the same engine's foot route: median (90th percentile; share over 1.5)", [(n, detour(k)) for n, k in R[:8]], AREAS)
def ov(k):
    def f(r):
        d = r["overlap_with_ours"].get(k)
        return "n/a" if not d or d["median"] is None else f'{d["median"]:.2f} ({pct(d["share_same_route"])})'
    return f
table("Overlap with this graph's wheelchair route: median (share that are the same route)", [(n, ov(k)) for n, k in R[1:]], AREAS)
for field, title in (("with_a_crossing_without_a_surveyed_ramp", "Routes with a crossing that has no surveyed ramp within reach"), ("with_an_edge_over_the_incline_limits", "Routes with an edge over this profile's incline limits"),
                     ("with_steps", "Routes with steps"), ("with_over_10m_of_street_centreline", "Routes with over 10 m in the roadway"), ("with_any_of_these", "Routes with any of the four")):
    table(title + " (audit against this graph's data)", [(n, lambda r, k=k: pct(r["audit_against_this_graph"].get(k, {}).get(field))) for n, k in R], AREAS)
table("Unramped crossings per km of route", [(n, lambda r, k=k: str(r["audit_against_this_graph"].get(k, {}).get("unramped_crossings_per_km", "n/a"))) for n, k in R], AREAS)
flags = sorted({f for a in AREAS for k in t[a]["audit_against_raw_osm"].values() for f in k if f.startswith("with_")})
for f in flags:
    table(f"Raw OSM audit: routes {f.replace('_', ' ')}", [(n, lambda r, k=k: pct(r["audit_against_raw_osm"].get(k, {}).get(f, 0.0) if k in r["audit_against_raw_osm"] else None)) for n, k in R], AREAS)
print("\n### ORS settings matrix, raw OSM: share of all pairs with a route\n\n| setting | " + " | ".join(short(a) for a in AREAS) + " |\n|---|" + "---|" * len(AREAS))
for cfg in sorted(t[AREAS[0]]["ors_setting_matrix_found"]["ors_armA"]):
    print(f"| {cfg} | " + " | ".join(pct(t[a]["ors_setting_matrix_found"]["ors_armA"].get(cfg)) for a in AREAS) + " |")
for arm in ("ors_armB_strict", "ors_armB_known"):
    print(f"\n### ORS settings matrix, {arm}: share of all pairs with a route\n\n| setting | " + " | ".join(short(a) for a in AREAS) + " |\n|---|" + "---|" * len(AREAS))
    for cfg in sorted(t[AREAS[0]]["ors_setting_matrix_found"].get(arm, {})):
        print(f"| {cfg} | " + " | ".join(pct(t[a]["ors_setting_matrix_found"][arm].get(cfg)) for a in AREAS) + " |")
print("\n### This graph against the reference (ORS raw OSM, recommended weighting, incline 10, kerb 0.06), measured pairs\n\n| | " + " | ".join(short(a) for a in AREAS) + " |\n|---|" + "---|" * len(AREAS))
for s in ("same route", "different route", "reference only", "ours only", "neither"):
    print(f"| {s} | " + " | ".join(str(t[a]["ours_against_reference"]["status"].get(s, 0)) for a in AREAS) + " |")
for c in ("kerb data", "incline data", "structure", "connectivity", "rule"):
    print(f"| cause: {c} | " + " | ".join(str(t[a]["ours_against_reference"]["causes_of_disagreement"].get(c, 0)) for a in AREAS) + " |")
print("| ORS on this graph (strict, shortest) matches this graph | " + " | ".join(pct(t[a]["ours_against_reference"]["arm_b_strict_matches_ours"]) for a in AREAS) + " |")
