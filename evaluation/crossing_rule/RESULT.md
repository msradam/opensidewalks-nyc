# The ramp to crossing rule, checked on the ground

2026-10-02. Numbers come from `score.json` (written by `score.py` from `features.json` and `sample/ratings/`).

## Question

The routing layer (`scripts/osw_to_unweaver.py`) counts a crossing as having curb ramps when a surveyed ramp sits within 5 m of each end of the crossing. v0.3.2 chose that over the strict reading (a ramp on the crossing's own node) because the strict reading left about 1% of Queens pairs routable. Nobody had checked which crossings the 5 m rule gets wrong.

## Method

- Frame: the 106,295 crossings of the v0.3.2 graph, grouped as the routing layer groups them (crossing edges joined through nodes no sidewalk reaches). 103,622 have exactly two ends; the sample is drawn from those.
- Sample: 40 per borough, 200 in all, seed 20261002 (`sample.py`, `sample/sample.csv`, `sample/sample_meta.json`).
- Sheets: one per crossing over the city's 2018 orthoimagery (maps.nyc.gov tiles, the year of the DOT ramp survey, about 15 cm per pixel). Left panel has the two ends A and B; right panel adds the survey's ramp positions as surveyed. No rule's verdict is drawn.
- Rating: `PROTOCOL.md`. Per end, `ramp_serves` (yes, no, unclear): does a surveyed ramp sit where this crossing meets the kerb. Four primary raters took 50 sheets each. A second rater took every third sheet (66) without seeing the others' files. All raters were language-model instances given only the protocol and the sheets; no person rated.
- Scoring: a crossing is ramped on the ground when both ends are yes. Crossings with an unclear end are left out. Each rule is scored per end and per crossing.

## What the raters found

| | Count |
|---|---|
| Ends rated | 400 (388 yes or no, 12 unclear) |
| Ends with a ramp serving the crossing | 89.4% of the 388 |
| Crossings ramped at both ends | 164 |
| Crossings not ramped | 27 |
| Crossings with an unclear end | 9 |
| Ends where the imagery itself showed a ramp | 9 (warning pads); 379 could not be told at this resolution |

By borough, the share of ends with a ramp is 0.92 (BK), 0.87 (BX), 0.90 (MN), 0.94 (QN), 0.85 (SI). Most of the "no" crossings are not street crossings at all: parking lot aisles, park roads, campus paths, a rail platform, places with no surveyed ramp anywhere on the sheet.

Agreement between the primary and the second rater on the 132 ends both rated: 96.2% observed, Cohen's kappa 0.805 over the three classes. On the 125 ends both called yes or no, agreement is 100%. The disagreements are all one rater's "unclear" against the other's call.

## How the rules score

Per crossing (191 with a clear verdict):

| Rule | Precision | Recall | Called ramped |
|---|---|---|---|
| Strict: end node is a curb node | 1.000 | 0.555 | 91 |
| Graph curb node within 2 m | 1.000 | 0.787 | 129 |
| Graph curb node within 3 m | 0.994 | 0.988 | 163 |
| Graph curb node within 5 m (the rule in v0.3.2) | 0.988 | 1.000 | 166 |
| Graph curb node within 8 m | 0.982 | 1.000 | 167 |
| Surveyed position within 3 m | 1.000 | 0.970 | 159 |
| Surveyed position within 5 m | 0.988 | 0.994 | 165 |
| Surveyed position within 8 m and within 3 m of the crossing's axis | 0.982 | 1.000 | 167 |
| Surveyed position within 8 m on the crossed street (RAMP_ONSTR) | 1.000 | 0.732 | 120 |

The full table, with per-end scores and more variants, is in `score.json`.

## Decision

Keep the 5 m rule. Of the 166 crossings it calls ramped, 164 are ramped on the ground (precision 0.988, 95% interval about 0.96 to 1.00), and it misses none of the 164. The 3 m rule trades one false positive for two misses; the sample cannot separate the two. The strict rule misses 45% of ramped crossings, which is why it left so little routable. An alignment test (ramp within a few metres of the crossing's axis) adds nothing, and the survey's RAMP_ONSTR street name loses a quarter of the true crossings to name mismatches between OSM and DOT.

The project's criterion was that a rule which sends a wheelchair user to a corner without a usable ramp is the worse outcome. On this sample the 5 m rule does that at 1.2% of the crossings it passes.

## Limits

- Existence of each ramp rests on DOT's 2018 field survey. The imagery showed a ramp directly at 9 ends only. What was rated is whether a surveyed ramp is positioned to serve the crossing, not whether it is there today or usable.
- The rated truth and the rules both use the survey's ramp positions; what the rating adds is the real crosswalk and kerb seen in imagery, independent of the OSM crossing line and of any distance threshold.
- 200 crossings, 40 per borough. A false positive rate of 1.2% has a wide interval.
- Raters were language-model instances following a written protocol, not people, and nothing was checked on the ground.
- 2.5% of crossings have one end or three or more; they were not sampled.
