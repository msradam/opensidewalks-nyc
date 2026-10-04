# The ramp to crossing rule, checked against 2018 aerial imagery by language-model raters

2026-10-02, with a note dated 2026-10-03. Numbers come from `score.json`. `code/score.py` writes it from `sample/features.json` and `sample/ratings/`. Nothing in this check was looked at on the ground.

## How to recompute

From the repository root, run `uv run python evaluation/crossing_rule/code/score.py evaluation/crossing_rule/sample/features.json evaluation/crossing_rule/sample/ratings OUT.json`. On 2026-10-03 this reproduced `score.json` exactly, key for key. `sample/features.json` is the per-end input: for each of the 400 sampled ends, the graph node, whether it is a curb node, the distance to the nearest graph curb node, and every surveyed ramp within reach with its distance, its offset along and across the crossing's axis, and its survey street. It was written by `code/features.py` from the v0.3.2 graph tables and the raw ramp survey, and was copied here from the archive on 2026-10-03 so that the scores can be recomputed. `features.py` itself cannot be rerun from published files, because the v0.3.2 graph tables (`groups_v032.pkl` and the table pickle) are not published and v0.3.2 has no release. Only 3 of the 200 sheets are published (`sample/sheets/`), so the ratings cannot be repeated from published files either.

## Question

The routing layer (`scripts/osw_to_unweaver.py`) counts a crossing as having curb ramps when a surveyed ramp sits within 5 m of each end of the crossing. v0.3.2 chose that over the strict reading (a ramp on the crossing's own node) because the strict reading left about 1% of Queens pairs routable. Nobody had checked which crossings the 5 m rule gets wrong.

## Method

- Frame: the 106,295 crossings of the v0.3.2 graph, grouped as the routing layer groups them (crossing edges joined through nodes no sidewalk reaches). 103,622 have exactly two ends. The sample is drawn from those.
- Sample: 40 per borough, 200 in all, seed 20261002 (`code/sample.py`, `sample/sample.csv`, `sample/sample_meta.json`).
- Sheets: one per crossing over the city's 2018 orthoimagery (maps.nyc.gov tiles, about 15 cm per pixel). 2018 was chosen because most of the ramp survey's records are from 2018. The left panel has the two ends A and B. The right panel adds the survey's ramp positions as surveyed. No rule's verdict is drawn.
- Rating: `PROTOCOL.md`. Per end, `ramp_serves` (yes, no, unclear): does a surveyed ramp sit where this crossing meets the kerb. Four primary raters took 50 sheets each. A second rater took every third sheet (66) without seeing the others' files.
- Raters: Anthropic Claude models, run as separate instances through Claude Code. Each instance was given only `PROTOCOL.md` and its sheets, and `PROTOCOL.md` is the instruction it received. The exact model versions were not recorded. No person rated any sheet.
- Scoring: a crossing is rated ramped when both ends are rated yes. Crossings with an unclear end are left out. Each rule is scored per end and per crossing. The pooled figures weight each borough equally.

## What the raters found

| | Count |
|---|---|
| Ends rated | 400 (388 yes or no, 12 unclear) |
| Ends with a ramp serving the crossing | 89.4% of the 388 |
| Crossings rated ramped at both ends | 164 |
| Crossings rated not ramped | 27 |
| Crossings with an unclear end | 9 |
| Ends where the imagery itself showed a ramp | 9 (warning pads); 379 could not be told at this resolution |

By borough, the share of ends with a ramp is 0.92 (BK), 0.87 (BX), 0.90 (MN), 0.94 (QN), 0.85 (SI). Most of the "no" crossings are not street crossings at all: parking lot aisles, park roads, campus paths, a rail platform, places with no surveyed ramp anywhere on the sheet.

Agreement between the primary and the second rater on the 132 ends both rated: 96.2% observed, Cohen's kappa 0.805 over the three classes. On the 125 ends both called yes or no, agreement is 100%. The disagreements are all one rater's "unclear" against the other's call. This kappa is between instances of one language model. It measures consistency, not accuracy, because the instances can share the same errors.

## How the rules score

Per crossing (191 with a clear verdict). The truth is the rating, not the ground.

| Rule | Precision | Recall | Called ramped | False passes | Misses |
|---|---|---|---|---|---|
| Strict: end node is a curb node | 1.000 | 0.555 | 91 | 0 | 73 |
| Graph curb node within 2 m | 1.000 | 0.787 | 129 | 0 | 35 |
| Graph curb node within 3 m | 0.994 | 0.988 | 163 | 1 | 2 |
| Graph curb node within 5 m (the rule in v0.3.2) | 0.988 | 1.000 | 166 | 2 | 0 |
| Graph curb node within 8 m | 0.982 | 1.000 | 167 | 3 | 0 |
| Surveyed position within 3 m | 1.000 | 0.970 | 159 | 0 | 5 |
| Surveyed position within 5 m | 0.988 | 0.994 | 165 | 2 | 1 |
| Surveyed position within 8 m and within 3 m of the crossing's axis | 0.982 | 1.000 | 167 | 3 | 0 |
| Surveyed position within 8 m on the crossed street (RAMP_ONSTR) | 1.000 | 0.732 | 120 | 0 | 44 |

False passes and misses are the per-crossing `fp` and `fn` in `score.json`. The full table, with per-end scores and 17 rules in all, is in `score.json`. Seventeen rules were scored on one sample, so the best-looking rule on this sample is partly chosen by chance.

## Uncertainty

The 95% intervals below are exact (Clopper-Pearson) binomial intervals, computed on 2026-10-03 from the counts in `score.json` with `scipy.stats.beta`. Wilson intervals are within 0.004 of these at both ends.

| Quantity | Counts | Value | 95% interval |
|---|---|---|---|
| 5 m rule, false pass rate | 2 of 166 called ramped | 1.2% | 0.15% to 4.3% |
| 5 m rule, precision | 164 of 166 | 0.988 | 0.957 to 0.999 |
| 5 m rule, recall | 164 of 164 | 1.000 | 0.978 to 1.000 |
| Surveyed position within 3 m, false pass rate | 0 of 159 | 0% | 0% to 2.3% |
| Surveyed position within 3 m, recall | 159 of 164 | 0.970 | 0.930 to 0.990 |

Weighted by each borough's frame size (`sample/sample_meta.json`) instead of equally, the 5 m rule's precision is 0.986. One false pass is in Manhattan and one in Queens. This was computed on 2026-10-03 from `sample/features.json` and the primary ratings.

Recall near 1.0 follows partly from how the sample and the truth were defined. A rater could only say yes where a surveyed ramp square sits at the crosswalk, and such a square is nearly always within 5 m of the graph's end. Recall therefore measures the rule only against ramps that are in the survey. It cannot detect a crossing refused because the survey missed a ramp. The router comparison found that 3,878 of the 15,599 crossing ends with no surveyed ramp within 5 m (24.9%) are tagged lowered or flush in OSM (`../compare/results/kerb_crosscheck.json`), so some refusals may be wrong in a way this check cannot see.

## Decision

The 5 m rule was kept. Of the 166 crossings it calls ramped, 164 were rated ramped, and it misses none of the 164 rated ramped.

The narrower rules do not do worse on false passes. Surveyed position within 3 m has no false pass and misses 5 crossings. Graph curb node within 2 m has no false pass and misses 35. Graph curb node within 3 m has one false pass and misses 2. With 2 false passes against 0 or 1, this sample cannot tell the 5 m rule apart from the 3 m rules on false passes. The project's criterion was that sending a wheelchair user to a corner without a usable ramp is the worse outcome, and on that criterion alone the 3 m survey rule is at least as good. The 5 m rule was kept because it was the rule in place before the check, because the check gave no clear reason to change it, and because the narrower rules refuse more crossings that were rated ramped and so leave fewer routes. No ratio of the cost of a false pass to the cost of a miss was set in advance. The strict rule misses 45% of crossings rated ramped, which is why it left so little routable. The survey's RAMP_ONSTR street name loses a quarter of the crossings rated ramped to name mismatches between OSM and DOT.

Note 2026-10-03: an earlier version of this section said the 3 m rule "trades one false positive for two misses", which describes only the graph curb node rule at 3 m. The surveyed position rule at 3 m, which has no false pass, was added above after external review.

## Limits

- The rated truth is what the 2018 aerial imagery shows, as judged by language-model raters. Nobody checked any crossing on the ground.
- Existence of each ramp rests on the city's ramp survey. A contractor (Cyclomedia) collected it for NYC DOT from vehicle-mounted street-level imagery and LiDAR. Its records run from March 2017 to January 2020, mostly 2018. No error rate for the survey is published. The imagery showed a ramp directly at 9 ends only. What was rated is whether a surveyed ramp is positioned to serve the crossing, not whether it is there today or usable.
- The rated truth and the rules both use the survey's ramp positions. What the rating adds is the real crosswalk and kerb seen in imagery, independent of the OSM crossing line and of any distance threshold.
- 200 crossings, 40 per borough. The false pass rate's 95% interval runs to 4.3%.
- The raters were instances of one language model, and the agreement between them is not a measure of accuracy. No subset was rated by a person, so nothing here measures the model's accuracy against human judgement.
- Crossings with zero, one, or three or more ends (2.5% of the frame) were not sampled.
