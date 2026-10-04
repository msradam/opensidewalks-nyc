"""Score the ramp-to-crossing rules against the rated sample.

Truth per end: the primary rater's `ramp_serves`. A crossing is rated ramped
when both ends are rated yes; a crossing with an unclear end is left out. The
raters were language-model instances rating 2018 aerial imagery, so the truth
is a rating, not a check on the ground (docstring reworded 2026-10-03).
Agreement: Cohen's kappa between the primary and the blind rater on the ends
both rated.

Inputs as published: FEATURES_JSON is ../sample/features.json and RATINGS_DIR
is ../sample/ratings/. Run from the repository root with
  uv run python evaluation/crossing_rule/code/score.py \
    evaluation/crossing_rule/sample/features.json \
    evaluation/crossing_rule/sample/ratings OUT_JSON
and OUT_JSON matches ../score.json.

usage: score.py FEATURES_JSON RATINGS_DIR OUT_JSON
"""
import glob, json, sys
from pathlib import Path
import numpy as np, pandas as pd

F = pd.DataFrame(json.load(open(sys.argv[1])))
rdir = Path(sys.argv[2])
primary = pd.concat([pd.read_csv(p) for p in sorted(glob.glob(str(rdir / "rater_[0-9].csv")))])
blind = pd.read_csv(rdir / "rater_blind.csv") if (rdir / "rater_blind.csv").exists() else None
for d in (primary, blind):
    if d is not None:
        d["n"] = d.n.astype(int)
        d["end"] = d.end.str.strip().str.upper()
        d["ramp_serves"] = d.ramp_serves.str.strip().str.lower()
F = F.merge(primary[["n", "end", "ramp_serves", "seen_in_imagery", "end_type", "marked"]], on=["n", "end"], how="left")

# ----- rules, per end: True if the rule says a ramp serves the end -----
def nearest_raw(r, within):
    return any(c["dist"] <= within for c in r.ramps)

def aligned(r, within, across):
    return any(c["dist"] <= within and c["across"] <= across for c in r.ramps)

def on_street(r, within):
    """A ramp within reach whose survey street (RAMP_ONSTR) is a street this crossing crosses."""
    return any(c["dist"] <= within and c["on_street"] in r.crossed_streets for c in r.ramps)

def not_other_street(r, within):
    """A ramp within reach that is not on a street other than the one crossed
    (keeps ramps whose street name did not match anything)."""
    return any(c["dist"] <= within and (not r.crossed_streets or c["on_street"] in r.crossed_streets
                                        or c["on_street"] not in [s for s in c["streets"]]) for c in r.ramps)

RULES = {
    "strict: end node is a curb node": lambda r: bool(r.node_is_curb),
    "graph curb node within 2 m": lambda r: r.nearest_graph_curb_m <= 2,
    "graph curb node within 3 m": lambda r: r.nearest_graph_curb_m <= 3,
    "graph curb node within 5 m (v0.3.2)": lambda r: r.nearest_graph_curb_m <= 5,
    "graph curb node within 8 m": lambda r: r.nearest_graph_curb_m <= 8,
    "surveyed ramp within 3 m": lambda r: nearest_raw(r, 3),
    "surveyed ramp within 5 m": lambda r: nearest_raw(r, 5),
    "surveyed ramp within 8 m": lambda r: nearest_raw(r, 8),
    "surveyed ramp within 10 m": lambda r: nearest_raw(r, 10),
    "surveyed ramp within 8 m and within 3 m of the crossing's axis": lambda r: aligned(r, 8, 3),
    "surveyed ramp within 8 m and within 4 m of the axis": lambda r: aligned(r, 8, 4),
    "surveyed ramp within 10 m and within 3 m of the axis": lambda r: aligned(r, 10, 3),
    "surveyed ramp within 10 m and within 4 m of the axis": lambda r: aligned(r, 10, 4),
    "surveyed ramp within 12 m and within 4 m of the axis": lambda r: aligned(r, 12, 4),
    "surveyed ramp within 8 m on the crossed street (RAMP_ONSTR)": lambda r: on_street(r, 8),
    "surveyed ramp within 10 m on the crossed street": lambda r: on_street(r, 10),
    "surveyed ramp within 10 m, axis 4 m, or on the crossed street within 8 m": lambda r: aligned(r, 10, 4) or on_street(r, 8),
}
for name, fn in RULES.items():
    F[name] = F.apply(fn, axis=1)

def prf(truth, pred):
    tp = int((truth & pred).sum()); fp = int((~truth & pred).sum()); fn = int((truth & ~pred).sum())
    return {"tp": tp, "fp": fp, "fn": fn, "precision": round(tp / max(tp + fp, 1), 3), "recall": round(tp / max(tp + fn, 1), 3),
            "f1": round(2 * tp / max(2 * tp + fp + fn, 1), 3), "says_yes": int(pred.sum())}

rated = F[F.ramp_serves.isin(["yes", "no"])]
truth_end = rated.ramp_serves == "yes"
byc = F.groupby("n").ramp_serves.agg(lambda s: "yes" if (s == "yes").all() else ("unclear" if (s == "unclear").any() else "no"))
out = {"ends_rated": int(len(rated)), "ends_unclear": int((F.ramp_serves == "unclear").sum()), "ends_missing": int(F.ramp_serves.isna().sum()),
       "ends_yes_share": round(float(truth_end.mean()), 3), "crossings": {"yes": int((byc == "yes").sum()), "no": int((byc == "no").sum()), "unclear": int((byc == "unclear").sum())},
       "seen_in_imagery": rated.seen_in_imagery.value_counts().to_dict(), "end_type": rated.end_type.value_counts().to_dict(),
       "ends_yes_by_borough": rated.groupby("borough").apply(lambda d: round(float((d.ramp_serves == "yes").mean()), 3)).to_dict(),
       "rules": {}}
clear = byc[byc != "unclear"].index
for name in RULES:
    per_c = F.groupby("n")[name].all()
    out["rules"][name] = {"per_end": prf(truth_end.values, rated[name].values),
                          "per_crossing": prf((byc.loc[clear] == "yes").values, per_c.loc[clear].values)}
if blind is not None:
    both = blind.merge(primary[["n", "end", "ramp_serves"]], on=["n", "end"], suffixes=("_blind", "_primary"))
    a, b = both.ramp_serves_blind, both.ramp_serves_primary
    po = float((a == b).mean())
    pe = float(sum(((a == v).mean()) * ((b == v).mean()) for v in set(a) | set(b)))
    m = a.isin(["yes", "no"]) & b.isin(["yes", "no"])
    a2, b2 = a[m], b[m]
    po2 = float((a2 == b2).mean()); pe2 = float(sum(((a2 == v).mean()) * ((b2 == v).mean()) for v in ("yes", "no")))
    out["agreement"] = {"ends_rated_by_both": int(len(both)), "observed_agreement_3_classes": round(po, 3),
                        "kappa_3_classes": round((po - pe) / (1 - pe), 3) if pe < 1 else None,
                        "ends_both_yes_or_no": int(m.sum()), "observed_agreement_yes_no": round(po2, 3),
                        "kappa_yes_no": round((po2 - pe2) / (1 - pe2), 3) if pe2 < 1 else None,
                        "confusion": pd.crosstab(a, b).to_dict()}
json.dump(out, open(sys.argv[3], "w"), indent=1)
print(json.dumps({k: v for k, v in out.items() if k != "rules"}, indent=1))
rows = [(name, r["per_end"]["precision"], r["per_end"]["recall"], r["per_crossing"]["precision"], r["per_crossing"]["recall"], r["per_crossing"]["f1"], r["per_crossing"]["says_yes"])
        for name, r in out["rules"].items()]
print(pd.DataFrame(rows, columns=["rule", "end P", "end R", "crossing P", "crossing R", "crossing F1", "crossings called ramped"]).to_string(index=False))
