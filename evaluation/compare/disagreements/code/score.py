"""Score the blind imagery ratings against the automatic cause of each disagreement.

usage: python score.py DIR   (reads key.json, ratings_A_*.json, ratings_B_odd.json; writes score.json)
"""
import glob, json, sys
from collections import Counter
from pathlib import Path
D = Path(sys.argv[1])
key = {k["sheet"]: k for k in json.load(open(D / "key.json"))}
A = {r["sheet"]: r for f in sorted(glob.glob(str(D / "ratings_A_*.json"))) for r in json.load(open(f))}
B = {r["sheet"]: r for r in json.load(open(D / "ratings_B_odd.json"))}

def kappa(pairs):
    n = len(pairs); po = sum(a == b for a, b in pairs) / n
    ca, cb = Counter(a for a, _ in pairs), Counter(b for _, b in pairs)
    pe = sum(ca[c] * cb[c] for c in set(ca) | set(cb)) / n ** 2
    return {"n": n, "observed_agreement": round(po, 3), "kappa": round((po - pe) / (1 - pe), 3) if pe < 1 else None,
            "confusion": {f"{a} / {b}": v for (a, b), v in sorted(Counter(pairs).items())}}

common = sorted(set(A) & set(B))
out = {"sheets": len(key), "rated_by_A": len(A), "rated_by_B": len(B), "rated_by_both": len(common), "agreement": {}, "by_cause": {}}
for q in ("place", "ramps", "sidewalk"):
    out["agreement"][q] = kappa([(A[s][q], B[s][q]) for s in common])
for cause in sorted({k["cause"] for k in key.values()}):
    sheets = [s for s in sorted(A) if key[s]["cause"] == cause]
    row = {"sheets": len(sheets), "place": dict(Counter(A[s]["place"] for s in sheets)), "sidewalk": dict(Counter(A[s]["sidewalk"] for s in sheets))}
    if cause == "kerb data":
        cr = [s for s in sheets if A[s]["place"] == "crossing"]
        row["at_a_crossing"] = len(cr)
        row["ramps_at_those_crossings"] = dict(Counter(A[s]["ramps"] for s in cr))
        # The cause says: a surveyed ramp is missing at an end. The photograph supports it when at most one end shows a ramp,
        # contradicts it when both ends show one (the graph is then behind the street), and cannot decide otherwise.
        row["supported"] = sum(A[s]["ramps"] in ("one", "neither") for s in cr)
        row["contradicted_ramps_visible_at_both_ends"] = sum(A[s]["ramps"] == "both" for s in cr)
        row["undecided"] = len(sheets) - row["supported"] - row["contradicted_ramps_visible_at_both_ends"]
        row["contradicted_sheets"] = [{"sheet": s, "id": key[s]["id"], "area": key[s]["area"], "note": A[s]["note"]} for s in cr if A[s]["ramps"] == "both"]
    elif cause == "connectivity":
        row["reference_route_in_the_roadway"] = sum(A[s]["sidewalk"] == "no" for s in sheets)
        row["reference_route_on_a_sidewalk_or_path"] = sum(A[s]["sidewalk"] == "yes" for s in sheets)
    else:
        row["on_a_structure"] = sum(A[s]["place"] == "structure" for s in sheets)
    row["notes"] = {s: A[s]["note"] for s in sheets}
    out["by_cause"][cause] = row
json.dump(out, open(D / "score.json", "w"), indent=1)
print(json.dumps({"agreement": {q: {k: v for k, v in a.items() if k != "confusion"} for q, a in out["agreement"].items()},
                  "by_cause": {c: {k: v for k, v in r.items() if k not in ("notes", "contradicted_sheets")} for c, r in out["by_cause"].items()}}, indent=1))
