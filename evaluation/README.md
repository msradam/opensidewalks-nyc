# Evaluation evidence for opensidewalks-nyc v0.3.3

This folder holds the evidence behind the claims made for v0.3.3 of the graph and for the comparison with other routers: protocols, raw ratings, result files and a few example images. Each section below says what was claimed, how it was tested, the result, where the files are and which tracked code produced it. The section after that lists what the evidence does not show.

## How the evidence was made

The work was done on one machine from public data. Result files were copied here from the project's working archive, which is not published. Some files still name paths in that archive (`research_notes/...`, `sources/`, `raw/`, scripts such as `score.py` or `diagnose.py`); those files are not here unless a section below says so. The scripts that drew the samples and computed the scores are copied as they ran into `code/` folders beside their results (`crossing_rule/code/`, `reachability/code/`, `compare/results/code/`, `compare/disagreements/code/`, `ramp_backlog/code/`). Their input and output paths point into the archive, so they document the method and need their paths changed to run. The pipeline, the converters and the comparison runners are tracked in this repository and are named in each section.

The raters were language-model instances given only the written protocol and the sheets. No person rated any sheet or crossing, and nothing was checked on the ground. This applies to the crossing rule sample, the disagreement sheets, the hand-checked routes, the bridge diagnosis and the literature notes in `background/`. Where a copied file says "I", it is one of those instances writing.

Copies were edited only to remove local paths, to replace working wording with the disclosure above, and to add dated notes where a number had since changed. Every edit is marked as a note or is a wording change; no computed value was changed.

## Evidence lines

### 1. The graph is valid and complete

Claim: v0.3.3 validates under `python-osw-validation` 0.5.0 and was built in one run from an empty cache. Test: the validator and every post-build check on the final build at commit `2442db2`. Result: valid with 0 errors, 4,068,058 features (`build/validator.json`, `build/checks.json`). Every edge difference from v0.3.2 is sorted into the commit that explains it (`build/compare_v032.json`). Code: `validators/post_build_checks.py`.

### 2. Incline on bridges and elevated ways

Claim: deck heights from the LiDAR point clouds put incline on structures. Test: the 2017 heights against the 2014 USGS survey, which the fix did not use for those nodes, and the lifted nodes against the city's planimetric transport structure polygons. Result: 14,402 nodes carry a deck height and 98.5% of bridge edges have an incline (`build/checks.json`); 83.4% of 2017 heights have a 2014 surface within 0.25 m and 91.6% within 1 m (`structure_incline/structure_validate_2014.json`); 73.6% of nodes lifted 2 m or more lie in a structure polygon (`structure_incline/structure_where_check.json`). Footway edges over the wheelchair limits fell from 3.24% to 1.40% under 2 m long (`build/steep_by_length.json`). Code: `pipeline/utils/ept.py`, `pipeline/utils/deck.py`, `pipeline/stages/assemble.py`, `tests/test_structure_incline.py`; method in `METHODOLOGY.md` section 5b.

### 3. The 5 m ramp to crossing rule

Claim: a crossing end counts as ramped when a surveyed ramp is within 5 m, and this rule is right where it passes a crossing. Test: 200 crossings, 40 per borough (seed 20261002), each end rated over the city's 2018 orthoimagery by four raters following `crossing_rule/PROTOCOL.md`, with every third sheet rated again by a fifth rater. Result: precision 0.988 per crossing and recall 1.0; agreement kappa 0.805 over three classes; the strict rule finds 0.555 of ramped crossings (`crossing_rule/score.json`, `crossing_rule/RESULT.md`). Files: `crossing_rule/sample/` (the sample, its metadata, five rating files, three example sheets). Code: `crossings_with_ramps` in `scripts/osw_to_unweaver.py`, `tests/test_crossing_rule.py`; rule in `METHODOLOGY.md`, "Routing layer". The sampling frame is the v0.3.2 graph's crossings (`sample/sample_meta.json`); the rule did not change in v0.3.3.

### 4. Reachability on the final build

Claim: how often the wheelchair profile finds a route between random points. Test: 2,000 seeded random pairs per borough, drawn from nodes on walkable edges, checked from the drawn node. Result: wheelchair profile 84.8% Brooklyn, 63.0% Queens, 53.0% Manhattan, 43.7% Bronx, 43.1% Staten Island; pedestrian edges alone 94.0, 73.3, 79.3, 74.2, 66.3; the Brooklyn Bridge to DUMBO route is 3,769 m (`reachability/reach.json`). Files: `reachability/hand_check.md` (ten routes described over imagery, text only) and `reachability/blocked_on_routes.json` (the edges outside the limits on each landmark route). `blocked_on_routes.json` was regenerated on the final build for this folder. The per-edge figures the v0.3.3 report quotes hold on it: on the Queensboro Bridge a 17.5 m edge spans 11.1 m and the outer roadway reads 8.8%, and the Macombs Dam Bridge's Bronx ramp reads 13% to 19%. Code: the pipeline build; the reachability script is in the archive. See "Two sets of reachability numbers" below before comparing these with section 6.

### 5. Bridges

Claim: each of the five bridges that v0.3.2 did not join has a known cause. Test: each bridge followed node by node in the raw extract and in the built graph. Result: two were the test's own anchors, two are OSM gaps and one is an OSM tag; admitting every untagged cycleway would add 627 ways (`bridges/FINDINGS.md`, `bridges/findings.json`). On the final build 20 of 23 bridges are joined on pedestrian edges and 3 only along the street centreline (`bridges/bridges.json`). `bridges/osm_mapper_notes.md` describes the three OSM gaps for a mapper; nothing in it was checked on the ground.

### 6. Comparison with OpenRouteService, Valhalla and Unweaver

Claim: this graph finds fewer wheelchair routes than the routers people know, and refuses what they cannot: crossings with no surveyed ramp, slopes over a limit, and the roadway. Test: `compare/PROTOCOL.md`; 12,437 pairs on the same OSM extract, engines built from `../engines/`. Result: this graph's wheelchair profile finds a route for 91.2% of Brooklyn pairs against 98.8% for ORS on plain OSM; 87.6% of ORS's Brooklyn routes use a crossing with no surveyed ramp within 5 m of an end (`compare/results/comparison.json`, `compare/results/tables.md`). Of 9,066 disagreements over the five boroughs, 61.9% come from kerb data and 21.7% from incline data (`compare/results/tables.md`, "cause" rows). With the roadway removed (arm C), ORS and this graph agree on whether a route exists for 92.95% of Brooklyn pairs (`compare/results/arm_c.json`). Unweaver and this repository's search agree on whether a route exists for all 1,354 requests, and routes both found differ by at most 6.4 m (`compare/results/unweaver_*.json`). Of the 15,599 crossing ends with no surveyed ramp within 5 m, 3,878 (24.9%) are tagged lowered or flush in OSM (`compare/results/kerb_crosscheck.json`).

The hand check of disagreements (`compare/disagreements/`) rated 48 sheets, 12 per cause, over 2024 imagery. Raters agreed on the kind of place with kappa 0.941 on 24 double-rated sheets; at 10 of 12 connectivity places the route was in the roadway; at 8 of 11 kerb places a rater could not tell whether a ramp was there (`compare/disagreements/score.json`). `ratings_A_001_024.json` and `ratings_A_025_048.json` are the two primary raters, one file each; `ratings_B_odd.json` is the third rater, on the odd sheets. `key.json` gives each sheet's cause and OSM way.

`compare/probes/` holds the test of what ORS does with each kerb and incline tag form and the round trip through TDEI's reformatter. `compare/results/osm_tag_summary.json` backs three counts in the comparison report that had no small file: 4,359 of 393,531 OSM ways under this graph carry an incline tag, 15 nodes in the whole extract carry `kerb:height`, and 0 of the 12,182 routes ORS's reference configuration found use a `highway=steps` way. It was made for this folder from the archived routes and the pinned extract.

Code: `compare/` (`pairs.py`, `run_ours.py`, `run_ors.py`, `run_valhalla.py`, `run_unweaver.py`, `graph.py`, `measures.py`, `analyse.py`, `osm_tags.py`, `ors_tag_probe.py`, `clip_osw.py`), `scripts/osw_to_osm.py`, `scripts/osw_to_unweaver.py`, `tests/test_osw_to_osm.py`, `tests/test_compare_measures.py`.

### 7. The ramp backlog

Claim: how many ramps DOT publishes as Non-Compliant sit at corners its progress data does not show as rebuilt since the survey. Test: a join of the DOT survey layer to the per-corner progress data, method in `ramp_backlog/METHOD.md`. Result: lead with 81,390 ramps at 53,731 corners on `COMPLIANCY_STATUS`, the field DOT publishes; beside it, 126,395 at 79,789 corners on the stricter unpublished `COMPLIANCY_TOTAL`; 45,759 more are Pending Technical Review at such corners (`ramp_backlog/result.json`, `ramp_backlog/backlog_summary.json`, both district CSVs). `ramp_backlog/dot_code_documentation.md` says what DOT documents for each code. `ramp_backlog/SOURCES.txt` lists the URL of every source; no source document is copied here.

`backlog_summary.json` keeps keys named `headline` (under `headlines` and `headline_under_other_treatments`) that hold the stricter 126,395. Those are computed values written by the build and are left as they were. The figure to lead with is the 81,390 in `result.json` (`lead_with`) and `METHOD.md`.

### Lessons and background

`lessons/` holds 41 short notes, one per thing learned. Two quote numbers from the v0.3.2 dry run and carry a dated note with the final figure. `background/landscape_2026-10-03.md` covers comparable NYC efforts and the routers considered, `background/ground_truth_methods.md` is how others validate pedestrian networks and a plan for a field audit in Brownsville, and `background/ramp_standards.md` sets out the ADA and PROWAG limits against what the DOT survey measures.

## Two sets of reachability numbers

Section 4 and section 6 give different wheelchair shares for the same 2,000 seeded pairs per borough:

| | Brooklyn | Queens | Manhattan | Bronx | Staten Island |
|---|---|---|---|---|---|
| Node snapping (`reachability/reach.json`) | 84.8% | 63.0% | 53.0% | 43.7% | 43.1% |
| Nearest-edge snapping (`compare/results/comparison.json`) | 91.2% | 67.8% | 64.0% | 50.8% | 50.3% |

Both are correct for their method. `reach.json` starts each trip at the drawn graph node itself. That node can sit on an edge the wheelchair profile cannot use, or on a small piece of network cut off from the rest, and then the pair fails. The comparison does what the reference engines do with a coordinate: it moves each end to the nearest point on an edge the profile can use, on a network of at least 200 edges (`MIN_NETWORK_EDGES` in `compare/graph.py`, the size below which GraphHopper, inside ORS, drops a network). Run with node snapping, the comparison code reproduces `reach.json`'s counts exactly (1,696, 1,259, 1,060, 874 and 861 of 2,000). Edge snapping raises the shares by 4.8 to 11.0 points, most in Manhattan. Use the node-snapped figures to compare one version of the graph with another, since v0.3.2 was measured that way. Use the edge-snapped figures beside other routers, since that is how they treat a trip end. The same difference gives 94.0% against 94.8% for walking in Brooklyn and 3,769 m against 3,776 m for the Brooklyn Bridge route.

## What this evidence does not show

- Nothing was checked on the ground. Every statement about a ramp rests on DOT's 2018 to 2019 survey and every statement about slope on the 2017 LiDAR.
- The raters were language-model instances following written protocols, not people. No person has looked at any of the 48 disagreement sheets or the 200 crossing sheets as a rater.
- 200 crossings cannot bound the crossing rule's 1.2% false positive rate tightly.
- Imagery shows where a ramp is and almost never whether it is there: it showed a ramp directly at 9 of 400 crossing ends. The rating tests which crossing a surveyed ramp serves; existence rests on DOT's field survey. At 8 of 11 kerb disagreement places a rater could not tell.
- The hand-checked routes were drawn over NY State imagery at about 30 cm. They show that a route lies on sidewalks, crossings and decks, not whether a ramp is there. Those images are not published.
- The structure routes that fail, fail on OSM geometry at the foot of a ramp. Neither OSM nor the profile was changed; the fix is one or the other.
- The 2014 comparison covers nodes whose height came from 2017. The 45 nodes on spans over open water have no second survey and were read for plausibility only (the Brooklyn Bridge deck at 41 to 45 m).
- Structures newer than May 2017 carry the height the survey saw then (LaGuardia, the new Kosciuszko span).
- The ramp backlog rests on DOT's 2020 classifications and 2018 measurements. Progress is recorded per corner, so the count can understate where only some ramps at a corner were rebuilt and overstate where the public file lags.
- The comparison's search is a re-implementation. The v0.3.3 report said Unweaver itself was not run; the comparison later ran it in a container and it agreed on all 1,354 requests. City-wide, Unweaver was sampled at 40 pairs per area, since a query takes seconds to minutes.
- Arm B was converted before the coincident-ways fix and lacks 286 of 1,437,901 ways. Arm C was rebuilt after it. The effect on arm B's shares was not measured.
- 387 pairs across all sets are routed by this graph and not by ORS on the same edges (arm C). Rough surfaces explain 37. The likely cause is where ORS snaps; this was not verified.
- Of the arm C routes that still differ, "1,319 of 1,390 use a stretch over the incline limits" covers only the different-route rows of the six random samples (`compare/results/arm_c_residual.json`).
- The "over the incline limits" audit counts any edge, short ones included. How much of it is LiDAR noise was not measured.
- The way-level `kerb=raised` flag in the raw OSM audit is coarse; the node-level cross-check (0.2% of ramped ends) is the one to use.
- The disagreement sample was drawn before the snapping fix. Causes depend on ORS's route, so they did not move, but the sheets were not redrawn.
- The steps count looks up ORS's way ids in the OSM tags of the ways this graph uses. 0.9% of the ids on the reference routes are not in that table, so a steps way among them would be missed (`compare/results/osm_tag_summary.json`).
- The demo was checked by tools (axe-core and pa11y), not by a screen-reader user.
- The per-edge bridge blockers in `blocked_on_routes.json` are from the final build. `reachability/hand_check.md` was written from a copy made on the build before the George Washington Bridge tower fix (commit `2442db2`), and a dated note in it gives the differences.

## Images

`compare/disagreements/sheets/` has 4 of the 48 disagreement sheets, one per kind of cause:

| File | Cause | Caption |
|---|---|---|
| `sheet_003.jpg` | Connectivity (Staten Island) | ORS's route cuts through an intersection where no sidewalk is mapped. |
| `sheet_018.jpg` | Structure | A bridge deck carrying roadway and rail; no walkway surface is visible. |
| `sheet_032.jpg` | Incline data (Brooklyn) | A diagonal walk rises from the sidewalk into a plaza; slope cannot be seen from above. |
| `sheet_040.jpg` | Kerb data (Brooklyn) | A painted crosswalk with a warning pad visible at one corner only. |

Imagery: NYC Orthos 2024, NYC Office of Technology and Innovation, CC BY 4.0 (credit stamped on each sheet). Route lines and the marked way: © OpenStreetMap contributors, ODbL. Ramp positions: NYC DOT pedestrian ramp survey, NYC Open Data.

`crossing_rule/sample/sheets/` has 3 of the 200 crossing sheets: `001.jpg` (rated yes at both ends), `007.jpg` (rated no at both ends: no surveyed ramp at either end) and `013.jpg` (one end unclear, under an elevated rail structure).

Imagery: NYC Orthos 2018, NYC Office of Technology and Innovation (captured by the New York State orthoimagery programme), CC BY 4.0 per the city's aerial imagery metadata (`github.com/CityOfNewYork/nyc-geo-metadata`, `Metadata_AerialImagery.md`, "Use Limitations"). Crossing ends: © OpenStreetMap contributors, ODbL. Ramp positions: NYC DOT pedestrian ramp survey, NYC Open Data.

## Licences and credits

The result files that hold OpenStreetMap ids, coordinates or route lines are derived from OpenStreetMap and are under the Open Database License 1.0, like the graph (`../LICENSE-DATA.md`). Code is under Apache-2.0 (`../LICENSE`).

- OpenStreetMap: © OpenStreetMap contributors, ODbL 1.0. The graph's geometry and every engine in the comparison start from Geofabrik's `new-york-261001.osm.pbf`.
- NYC Open Data, under the NYC Open Data terms of use: the DOT pedestrian ramp survey (`ufzp-rrqu`) and programme progress (`e7gc-ub6z`), the planimetric sidewalks (`52n9-sdep`), the Facilities Database (`ji82-xba5`), NYCHA developments (`phvi-damg`), Cool It! NYC (`h2bn-gu9k`), MTA subway entrances (`i9wp-a4ja`), and the DCP council and community district boundaries (`872g-cjhh`, `5crt-au7u`).
- LiDAR: the 2017 New York City survey (NYC OTI) and the 2014 survey of the U.S. Geological Survey, served as Entwine point cloud tiles by NOAA Digital Coast; the 2017 topobathymetric terrain model, served by the New York State GIS Program Office.
- Orthoimagery: NYC Orthos 2018 and 2024, NYC Office of Technology and Innovation, CC BY 4.0, as credited under "Images".
