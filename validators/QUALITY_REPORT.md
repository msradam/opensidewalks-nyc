# Quality Report: OpenSidewalks NYC v0.3.3-nyc.1

> Note for v0.3.4-nyc.1 (2026-10-04): this report was measured on v0.3.3. v0.3.4 changes only the fields listed in its [release notes](../release-notes/v0.3.4-nyc.1.md). Geometry, incline and wheelchair reachability are unchanged on all 4,068,058 features, and v0.3.4 passes `python-osw-validation` 0.5.0 with zero errors. Every post-build check and routing result in this report is the same on v0.3.4. Three checks are new: `ext:dws_condition` agrees with the survey on 217,679 of 217,679 ramps, no counter slope is over 100%, and no width is 0 or less. Where this report counts Footway edges (544,672) or `crossing:markings` values, v0.3.4 has 459,570 Footway and 85,102 Pedestrian Road edges, and the markings come from a different rule.

> Audit date: 2026-10-03. Artifact audited: `nyc-osw.geojson` of the v0.3.3-nyc.1 build, made by one run of `python -m pipeline build` from an empty cache at commit `2442db2` plus the post-build endpoint snap (`scripts/snap_endpoints.py`). OpenStreetMap data as of 2026-10-01T20:22:06Z (Geofabrik extract `new-york-261001.osm.pbf`). Schema target: **OSW v0.3**. Validator: **`python-osw-validation` 0.5.0**.
>
> Every number here comes from a script: `validators/post_build_checks.py` for the artifact ([`evaluation/build/checks.json`](../evaluation/build/checks.json)), and the routing, bridge, deck-height and imagery checks described in their sections, whose result files are named there and are kept under [`evaluation/`](../evaluation/).

## Headline verdict

**The artifact passes `python-osw-validation` 0.5.0 against OSW v0.3, which checks form, not the schema's topology rules (the README lists the known deviations from the schema). Incline describes the walking surface on bridges and elevated ways. It is not yet one to present a route from without checking.**

- `python-osw-validation` 0.5.0 returns `is_valid: True` with zero errors across all 4,068,058 features (1,186,910 nodes, 2,881,148 edges).
- The validator checks form. The checks in this report are the ones it does not do, and each number below is traceable to a result file.
- What changed in v0.3.3: nodes on and beside bridges and elevated ways take their height from the LiDAR point clouds, not the terrain under the deck; the terrain model is read at 2 m; node heights are smoothed along the path over short edges so survey noise no longer reads as a grade; and the rule that ties a surveyed ramp to a crossing has been checked remotely against aerial imagery by language-model raters (nothing was checked on the ground). The step-free route from the Brooklyn Bridge's Manhattan end to DUMBO is 3.8 km.
- What remains weak, in order of how much it matters: the curb ramp survey is mostly from 2018; a sixth of the ramps are not on the graph; incline cannot see a curb ramp and is still wrong where this pipeline's height estimate jumps across one short edge at the foot of a ramp; a fifth to a third of pairs outside Brooklyn have no route on pedestrian edges, mostly because the graph has no edge where OSM maps sidewalks as `sidewalk=*` tags on the street, which this pipeline does not read.

## Schema validation

| Check | Result |
|---|---|
| Input | `nyc-osw-osw-split.zip` (split `nyc.nodes.geojson` + `nyc.edges.geojson`) |
| `python-osw-validation` 0.5.0 | `is_valid: True, errors: 0` ([`evaluation/build/validator.json`](../evaluation/build/validator.json)) |
| Coordinates over 7 decimal places | 0 |
| Edge ends that differ from their node's coordinate | 0 |
| Edges with unresolved `_u_id`/`_v_id`, self-loops, zero-length edges, duplicate IDs | 0 of each |
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` |
| Root metadata | `dataSource` (with licence, attribution and OSM extract), `dataTimestamp`, `pipelineVersion` (`0.3.3+nyc.1`, git SHA), `region` |

## Feature composition

| OSW type | Count |
|---|---|
| Sidewalk edges | 933,110 |
| Crossing edges | 437,510 |
| Footway edges | 544,672 (26,756 from OSM cycleways and tracks open to walkers) |
| Steps edges | 15,470 |
| Motor vehicle road edges | 950,386 (residential 418,875; service 289,376; secondary 89,629; tertiary 71,654; primary 58,632; unclassified 21,042; living_street 1,178) |
| Curb nodes | 217,679, one per surveyed ramp |
| Bare nodes | 969,231 |

Every edge is directed and every segment has its reverse (965,381 pedestrian segments). Edges marked `ext:structure`: bridge 14,992, elevated 5,158, tunnel 2,762.

## Graph integrity

The pedestrian graph is the sidewalk, crossing, footway and steps edges.

| Metric | Value |
|---|---|
| Pedestrian-graph nodes | 853,644 |
| Pedestrian-graph directed edges | 1,930,762 |
| Connected components | 4,975 (1,110 with 10 or more nodes) |
| Largest component | 611,968 nodes (71.7%) |
| Second largest | 136,141 nodes: Staten Island |
| Segments stored in one direction only (a project choice, not a schema rule) | 0 |

Share of each borough's pedestrian nodes in the largest component:

| Brooklyn | Queens | Manhattan | Bronx | Staten Island |
|---|---|---|---|---|
| 96.3% | 84.2% | 89.6% | 86.2% | 0% (a separate component) |

Much of the rest lies along streets whose sidewalks OSM maps as `sidewalk=*` tags on the street. That is a valid OSM scheme, and this pipeline does not read it, so the graph has no sidewalk edge there. The pipeline's tag filter also drops some ways that plain OSM routing uses, such as cycleways with no `foot` tag, which the US default treats as walkable: OpenRouteService's walking profile on this graph routes 82.1% of Manhattan pairs, against 100% on plain OSM ([`evaluation/compare/results/tables.md`](../evaluation/compare/results/tables.md), ORS settings matrix).

### Borough joins, bridge by bridge

The bridge check snaps each end of 23 bridges with a pedestrian path to the nearest pedestrian node in the right borough and compares the walking distance with the straight line ([`evaluation/bridges/bridges.json`](../evaluation/bridges/bridges.json)). With the points on the walkways' own landings, 20 of 23 are joined on pedestrian edges (ratio 1.0 to 2.1). The three that are not:

| Bridge | Pedestrian path | With street edges | Cause ([`evaluation/bridges/FINDINGS.md`](../evaluation/bridges/FINDINGS.md)) |
|---|---|---|---|
| 145th Street Bridge | 2,474 m for 545 m | 785 m | Modelling: OSM maps the sidewalk at the Bronx end on East 149th Street as `sidewalk=right` on the street, which this graph does not read (the ramp survey has 16 ramps there) |
| Broadway Bridge | 1,282 m for 232 m | 579 m | The graph has no crossing of 9th Avenue at Broadway, and two sidewalk ways at West 225th Street end 12 m apart. Whether a crossing exists there has not been checked. |
| RFK Bridge, Bronx span | 2,484 m for 597 m | 1,222 m | Pipeline rule: the island path both ramps land on (way 1414563386) is a cycleway with no `foot` tag, and the pipeline keeps a cycleway only with `foot=yes/designated/permissive`, which is stricter than the US default (`foot=yes`). The fix belongs in the pipeline. Admitting every untagged cycleway would add 627 ways, 218 of them one-way bike lanes |

Nothing has been posted to OSM, and no OSM edit would be made from these findings automatically.

### Nodes on no edge

36,180 nodes are not an endpoint of any edge, all of them curb ramps (see below).

## Curb ramps

All 217,679 ramps of the NYC DOT survey are in the file, one node each. 181,499 (83.4%) are an endpoint of a pedestrian edge; 126,152 are on a crossing. `tactile_paving` agrees with the survey's `DWS_CONDITIONS` on all 217,679. No node carries a sentinel slope.

### The ramp to crossing rule

The routing layer counts a crossing as having ramps when a surveyed ramp lies within 5 m of each of its ends. That rule was checked remotely ([`evaluation/crossing_rule/RESULT.md`](../evaluation/crossing_rule/RESULT.md), `score.json`): 200 crossings drawn at random, 40 per borough, each end rated over the city's 2018 orthoimagery (the main survey year) by language-model agents following a written protocol, and every third sheet rated again by another agent that saw nothing of the first ratings. No person rated the sheets and no crossing was visited. Agreement 96%, kappa 0.81 (between model instances, so a measure of consistency, not accuracy); on the ends both called yes or no, 100%. Of the 166 crossings the 5 m rule calls ramped, 164 were rated as having a surveyed ramp positioned to serve them at both ends (precision 0.988), and it misses none of the 164. The false pass rate of 1.2% (2 of 166) has an exact 95% interval of 0.1% to 4.3%. The strict rule (a ramp on the crossing's own node) finds 56%. A 3 m survey rule passes no unramped crossing but misses 5; with 2 errors against 0, the sample cannot tell these rules apart, and the 5 m rule was fixed before the check. The rating says where a surveyed ramp sits relative to the real crosswalk; the imagery showed a ramp directly at 9 of 400 ends, so the ramp's existence rests on DOT's survey, which a contractor (Cyclomedia) collected from vehicle-mounted imagery and LiDAR.

### NYC DOT curb-ramp slopes

The survey records running and cross slopes in percent: 212,194 ramps carry a running slope and 212,111 a cross slope. DOT says the measurements are not indicative of whether a ramp is compliant, and this project does not compare them with design limits.

## Elevation and incline

| | Value |
|---|---|
| Nodes with `ext:elevation_m` | 1,186,860 of 1,186,910 |
| Nodes at exactly 0.0 m | 218 (0.02%) |
| Highest node by borough | Staten Island 122.4 m, Brooklyn 112.2 m (a boardwalk on a hill), Bronx 84.9 m, Manhattan 80.5 m, Queens 79.4 m |
| Nodes with a deck height (`ext:elevation_source`) | 14,402: 13,943 from the 2017 survey, 45 from the 2014 survey, 414 interpolated |
| Structure nodes with no height | 48 |
| Edges with `incline` | 2,877,100 of 2,881,148 (99.9%) |
| Bridge edges with incline | 98.5%; elevated 98.8%; tunnel 0% |
| Sidewalk edges steeper than 5% | 4.3% |
| Sidewalk edges outside the wheelchair limits (up 8.3%, down 10%) | 0.8% |
| Footway edges outside the limits, by length | 1.4% under 2 m, 1.3% at 2 to 5 m, 2.4% at 5 to 10 m, 3.1% at 10 to 20 m, 1.9% at 20 to 50 m, 0.5% over 50 m |

Read incline as an estimate from an airborne survey, not a measurement of the path.

1. **The terrain model is bare earth.** On a bridge, a deck or a pier it holds the ground or water below. v0.3.3 reads the classified LiDAR point clouds instead (the 2017 city survey, and the 2014 USGS survey where the 2017 one has no returns, which is the main spans over open water), around every edge OSM tags as a bridge or as `layer` above 0 and outward along the path until the deck meets the ground. The method is in `METHODOLOGY.md`, section 5b.
2. **Deck heights against a survey the fix did not use.** Of the 13,943 nodes whose height came from the 2017 survey, 13,930 have a surface in the 2014 survey to compare with (13 have none). Among those, the 2014 surface is within 0.25 m at 83% and within 1 m at 92%; the median difference is 5 cm ([`evaluation/structure_incline/structure_validate_2014.json`](../evaluation/structure_incline/structure_validate_2014.json)). The large disagreements are mostly places rebuilt between the two flights: Hudson Yards, Empire Outlets, the Bayonne Bridge, LaGuardia. Of the nodes lifted 2 m or more above the terrain model, 74% lie within 5 m of a transport structure polygon of the city's planimetric database, which comes from photogrammetry and knows nothing of OSM tags or LiDAR (78% for OSM-tagged structures, 54% for untagged approaches; boardwalks, piers and plazas are not in that database).
3. **The remaining steep edges on structures.** 1,708 edges of 3 m or more touching a deck node read steeper than 15%. They cluster at station entrances, where the graph has a plain edge because this pipeline does not carry OSM node tags such as elevators; at airport terminals; and at the foot of ramps, where this pipeline's height estimate jumps by several metres across one short edge.
4. **Short edges.** The graph keeps every OSM vertex as a node, so half its edges are shorter than 6 m. Before incline is taken, each node's height is averaged with its neighbours' along the path over edges shorter than 5 m (never across a step of 0.5 m or more, and not on steps or tunnel edges).
5. **What no airborne survey can see.** A kerb ramp a metre long, a step, a cross slope. The limits in the wheelchair profile are applied to estimates with about 0.1 m of noise per node.

## Sidewalk width

`width` is on 818,622 sidewalk edges (87.7%); the median is 3.12 m (Staten Island 2.57, Queens 3.07, Brooklyn 3.44, Bronx 3.62, Manhattan 4.26). Against 5,805 transects cut across the polygon at the edge's midpoint, the median ratio of `width` to transect is 0.94 and the median absolute difference 0.58 m.

## Planimetric gap-fill sidewalks

Not in the graph. The 2,318 directed edges (1,159 segments, 60 km) derived from planimetric polygons with no OSM sidewalk within 10 m ship as `nyc-gapfill-sidewalks.geojson`, whose root states a sample result (9 of 18 on a sidewalk or walkway, 4 plainly wrong) and that they are not part of the graph.

## Attribute coverage

| Attribute | Coverage | Notes |
|---|---|---|
| `surface` | 1,260,426 edges (43.8%) | asphalt 766,676; concrete 358,354; paving_stones 69,556 |
| `crossing:markings` | 380,888 of 437,510 crossings (87.1%) | `yes` 272,110; `zebra` 108,778. Inferred from OSM `crossing=*`; OSM's `crossing:markings` tag is not read. `crossing=uncontrolled` is mapped to `zebra`, which asserts more than the source says, `traffic_signals` to `yes`, and `unmarked` is dropped, so no crossing carries `no`. |
| `kerb` | 217,679 curb nodes | `lowered` on all. `ufzp-rrqu` has no ramp type column; DOT's cut-through ramps may be flush curbs in schema terms |
| `ext:structure` | 22,912 edges | bridge, elevated, tunnel |
| `ext:elevation_source` | 14,402 nodes | `lidar_2017`, `lidar_2014`, `interpolated` |
| `ext:source`, `ext:pipeline_version` | every feature | `0.3.3+nyc.1` |
| `ext:source_timestamp` | every edge; 36,180 nodes | OSM-derived nodes lack it |
| `ext:osm_id` | every OSM-derived edge | |
| `ext:borough` | every feature | |

## Routing

The check uses the repository's own Unweaver inputs: `scripts/osw_to_unweaver.py` makes the layer Unweaver would read, `unweaver-project/cost-wheelchair.py` decides edge by edge, and only the graph search is re-implemented (results in [`evaluation/reachability/reach.json`](../evaluation/reachability/reach.json)). For the router comparison Unweaver itself was later run in a container: it agrees with this search on whether a route exists for all 1,354 requests tried and on length to within 7 m, when built with `--changes-sign incline`.

The wheelchair profile, adapted from Unweaver's example wheelchair profile (Nick Bolten, Apache-2.0), refuses steps and street centrelines, refuses a crossing unless a surveyed ramp lies within 5 m of each of its ends, and refuses an edge steeper than 8.3% up or 10% down.

Share of 2,000 seeded random origin and destination pairs with a route, both ends in the same borough, each end at the graph node it was drawn at (node snapping; 95% intervals about 2 points either way). The router comparison uses the same pairs with each end snapped to the nearest edge, which gives higher figures (91.2%, 67.8%, 64.0%, 50.8% and 50.3% for Brooklyn, Queens, Manhattan, the Bronx and Staten Island); [`evaluation/README.md`](../evaluation/README.md#two-sets-of-reachability-numbers), under "Two sets of reachability numbers", explains the difference.

| Profile | Brooklyn | Queens | Manhattan | Bronx | Staten Island | Ends anywhere |
|---|---|---|---|---|---|---|
| Every edge, street centrelines included | 98.0% | 93.7% | 79.7% | 93.1% | 93.8% | 61.6% |
| Pedestrian edges only | 94.0% | 73.3% | 79.3% | 74.2% | 66.3% | 52.4% |
| Pedestrian edges without steps | 93.5% | 72.7% | 75.1% | 73.6% | 65.3% | 51.3% |
| Wheelchair profile without its incline limits | 87.8% | 67.0% | 68.1% | 58.6% | 52.3% | 45.5% |
| **Wheelchair profile** | **84.8%** | **63.0%** | **53.0%** | **43.7%** | **43.1%** | **40.5%** |

Taking the constraints away in this order (street centrelines, steps, ramps, incline), excluding street centrelines costs the most in the Bronx, Queens and Staten Island, where many sidewalks are mapped as `sidewalk=*` tags on the street, which this pipeline does not read. In Brooklyn crossings with no surveyed ramp within reach cost the most, and in Manhattan incline does. The incline limits cost 3 points in Brooklyn and 15 in the Bronx and Manhattan. A different order would split the loss differently.

### Landmark routes

Seventeen landmark pairs (`scripts/route_test.py`), nine of them meant to cross a bridge, a viaduct or an elevated walkway. All eight others have a wheelchair route with no steps and no street centreline. Of the structure routes:

| Route | Walker | Wheelchair profile | Why |
|---|---|---|---|
| Brooklyn Bridge (Manhattan end) to DUMBO | 2,258 m | 3,769 m, the step-free way along the promenade | |
| High Line, Gansevoort to 30th Street | 1,689 m on the High Line | 1,835 m on the Tenth Avenue sidewalks | High Line entrances read as near-vertical footways because this pipeline does not carry OSM node tags. At 30th Street, OSM maps the lift as `highway=elevator`, `wheelchair=yes` on node 2823833584. |
| Riverside Park, 137th to 145th Street | 826 m | 826 m | Both landmarks snapped to the greenway beside Riverbank State Park, not its deck; the pair tests nothing |
| Williamsburg Bridge, end to end | 2,917 m | 9,192 m | One 7 m edge at the Manhattan end spans 7 m of height on this pipeline's heights (a ground node meets the ramp) |
| Manhattan Bridge, end to end | 2,034 m | no route | A 56.9 m ramp edge reads 9.7% and a 236 m deck edge reads 11.1% down |
| Queensboro Bridge, end to end | 2,808 m | 24,150 m | A 17 m edge at the Manhattan end spans 11 m, and the outer roadway reads 8.8% over 105 m |
| Pulaski Bridge, end to end | 1,142 m | 14,119 m | One 5 m edge at the Queens end drops 1.2 m |
| Macombs Dam Bridge, end to end | 990 m | 8,691 m | The Bronx ramp reads 13% to 19% on the terrain model |
| East 103rd Street to Wards Island (footbridge) | 740 m | no route | blocked at one end |

[`evaluation/reachability/blocked_on_routes.json`](../evaluation/reachability/blocked_on_routes.json) lists every edge outside the limits on each route's step-free path. The pattern is one to three edges per bridge, most often where this pipeline's height estimate jumps across one short edge at the foot of a ramp. On the Manhattan Bridge a 236 m deck edge also reads 11.1% down.

### Routes checked over imagery

Ten routes drawn over NY State imagery at about 30 cm and rated by a language model ([`evaluation/reachability/hand_check.md`](../evaluation/reachability/hand_check.md)): the six wheelchair routes drawn lie on sidewalks, crossings, park paths and the Brooklyn Bridge promenade, with no steps and no street centreline; the Williamsburg, Manhattan and Pulaski bridge decks carry the route when the incline limits are off; with them on, the Williamsburg and Pulaski are closed by one short edge and the Manhattan by a ramp edge and a deck edge; the High Line route stays on the Tenth Avenue sidewalks because the graph does not carry the elevator node at 30th Street; the Riverbank landmarks snapped to the greenway beside the park, not the deck, so that pair tests nothing. Of the nine structure pairs, only the Brooklyn Bridge routes on its structure under the profile. The imagery shows that a route lies on sidewalks, crossings and decks, not whether a ramp is there.

Do not present a route from this graph as wheelchair accessible. The ramp data is mostly from 2018 and says a ramp was there, not that it meets a standard. Incline is an estimate. Nothing in the graph knows about sidewalk condition, obstructions, construction or signal timing.

## Reproducibility

```bash
# from a fresh checkout, Python >= 3.11
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .

# 1. Build (city-wide: about 48 min, 35 GB peak (with swap) on a 32 GB
#    Apple Silicon machine, 14 GB under data/ including 2.2 GB of LiDAR tiles
#    and 1.1 GB of terrain tiles, 5.5 GB under output/)
python -m pipeline build

# 2. Snap edge endpoints onto node coordinates, emit the validator ZIP
python scripts/snap_endpoints.py --input output/nyc-osw.geojson

# 3. The gate. The validator pins geopandas==0.14.4, which breaks the
#    pipeline, so run it in its own environment.
uv run --no-project --isolated --with python-osw-validation python -c "
from python_osw_validation import OSWValidation
r = OSWValidation('output/nyc-osw-osw-split.zip').validate()
print('valid:', r.is_valid, 'errors:', len(r.errors or []))"

# 4. The checks the validator does not do
python validators/post_build_checks.py output/nyc-osw.geojson output/post_build_checks.json
```

The v0.3.3 build was made in one run at commit `2442db2` from an empty `data/` (the pinned OSM extract was placed in it first and checked against its SHA-256). The tests (`tests/`) pass.

## Open work

- A jump in this pipeline's height estimate across one short edge at the foot of a ramp blocks three of the structure routes (Williamsburg, Queensboro, Pulaski). The fix is in the incline method, for example by treating a jump above a set height on a short edge as unknown. OSM is not wrong there.
- Structures built after May 2017 carry whatever height the 2017 survey saw there.
- The ramp survey is mostly from 2018. DOT's per-corner progress data could mark ramps whose corner was rebuilt since.
- The largest single limit on routing is that the graph has no edge where OSM maps sidewalks as `sidewalk=*` tags on the street, which this pipeline does not read. Reading those tags, keeping untagged cycleways under the US `foot=yes` default, and carrying OSM node tags (kerbs, elevators) are open work. A city-wide list of near-miss endpoints would be shared with the NYC OSM community for review, and no edit would be made from it automatically.
