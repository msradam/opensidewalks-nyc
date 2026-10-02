# Quality Report: opensidewalks-nyc v0.3.2-nyc.1

> Audit date: 2026-10-02. Artifact audited: `nyc-osw.geojson` of the v0.3.2-nyc.1 build, made by `python -m pipeline build` plus the post-build endpoint snap (`scripts/snap_endpoints.py`). OpenStreetMap data as of 2026-10-01T20:22:06Z (Geofabrik extract `new-york-261001.osm.pbf`). Schema target: **OSW v0.3**. Validator: **`python-osw-validation` 0.5.0**.
>
> Every number here comes from a script: `validators/post_build_checks.py` for the artifact, and the routing, bridge and imagery checks described in their sections. The v0.3.1-nyc.1 report, with the defects it lists, is in git history.

## Headline verdict

**The artifact is schema-valid under OSW v0.3 and the defects found in v0.3.1-nyc.1 are fixed. It is still not a graph to route a wheelchair user on without checking.**

- `python-osw-validation` 0.5.0 returns `is_valid: True` with zero errors across all 3,874,332 features (1,189,651 nodes, 2,684,681 edges).
- The validator checks form: file structure, the JSON schema, unique IDs, that `_u_id`/`_v_id` resolve, geometry validity, that edge endpoints equal their node coordinates, and coordinate precision. It does not check connectivity or whether an attribute value is true. v0.3.1-nyc.1 passed it with 80% of its elevations wrong. The checks in this report are the ones the validator does not do.
- What remains weak, in order of how much it matters to a user: incline is a terrain estimate and is absent on bridges and tunnels; the curb ramp data is a 2018 survey; a sixth of the ramps are not on the graph; five of 23 bridges with a pedestrian path are not joined end to end on pedestrian edges; about half of the gap-fill sidewalk edges checked over imagery are on a sidewalk.

## What changed since v0.3.1-nyc.1

| | v0.3.1-nyc.1 | v0.3.2-nyc.1 |
|---|---|---|
| Features | 3,374,261 | 3,874,332 |
| Nodes | 1,155,380 | 1,189,651 |
| Edges | 2,218,881 | 2,684,681 |
| `python-osw-validation` 0.5.0 | invalid (coordinates over 7 decimals) | valid, 0 errors |
| Nodes with elevation exactly 0.0 | 927,754 (80.3%) | 455 (0.04%) |
| Highest node on Staten Island | 0.0 m | 122.5 m |
| Edges with incline exactly 0 | 1,830,745 (82.5%) | 8,744 (0.3%) |
| `tactile_paving` | `yes` on all 199,836 curb nodes | `no` 128,911, `yes` 88,624, untagged 144; agrees with the survey on all 217,679 |
| Surveyed ramps in the file | 199,836 of 217,679 | 217,679 of 217,679 |
| Curb nodes that are an edge endpoint | 134,077 (67.1%) | 181,686 (83.5%) |
| Pedestrian graph components | 10,743 | 6,083 |
| Largest pedestrian component | 428,068 nodes (63.1%), no Bronx node | 611,991 nodes (71.5%), all four land-connected boroughs |
| Share of Bronx pedestrian nodes in it | 0% | 85.7% |
| Williamsburg Bridge, end to end on foot | 7.9 km (by way of the Brooklyn Bridge) | 2.45 km |
| Median sidewalk `width` | 5.65 m | 3.12 m |
| Gap-fill sidewalk edges | 6,122, one direction, 694 km | 2,318 (1,159 segments, both directions), 60 km |
| Edges dropped by the endpoint merge | 359,340 | 66 |
| Edge ends moved by the endpoint snap | not recorded (2,309 moved over 5 m in a Staten Island rebuild) | 1,617, none over 2 m |
| Secondary road edges | 0 | 67,937 |
| Nodes on no edge | 220,084 | 44,065 |
| Licence and attribution in the file | absent | in the root of every asset |
| OSM snapshot | not recorded | recorded, with the extract's checksum |

## Schema conformance

| Check | Result |
|---|---|
| Input | `nyc-osw-osw-split.zip` (split `nyc.nodes.geojson` + `nyc.edges.geojson`) |
| `python-osw-validation` 0.5.0 | `is_valid: True, errors: 0` |
| Coordinates over 7 decimal places | 0 |
| Edge ends that differ from their node's coordinate | 0 |
| Edges with unresolved `_u_id`/`_v_id`, self-loops, zero-length edges, duplicate IDs | 0 of each |
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` |
| Root metadata | `dataSource` (with licence, attribution and OSM extract), `dataTimestamp`, `pipelineVersion`, `region` |

## Feature composition

| OSW type | Count |
|---|---|
| Sidewalk edges | 935,428 (933,110 from OSM, 2,318 planimetric gap-fill) |
| Crossing edges | 437,510 |
| Footway edges | 544,672 |
| Steps edges | 15,470 |
| Street edges | 751,601 |
| Curb-ramp nodes | 217,679 |
| Other point nodes | 971,972 |
| **Total** | **3,874,332** |

Edges are directed. Every pedestrian segment appears once per travel direction, with `incline` signed in the direction of travel (963,620 pedestrian segments). Streets follow OSM's `oneway`: 177,743 street segments appear in one direction only. There are 1,428,249 unique node pairs in all.

26,756 of the footway, crossing and sidewalk edges come from OSM ways tagged `highway=cycleway` or `track` that OSM tags as open to walkers. They carry `ext:osm_highway`. v0.3.1-nyc.1 dropped all of them, which cut several bridges.

Every edge is a segment between consecutive OSM nodes (the OSM graph is not simplified). The median edge is 7.0 m long, 5% are shorter than 1.2 m, and 79 are longer than 500 m.

## Graph integrity

The pedestrian graph is the sidewalk, crossing, footway and steps edges.

| Metric | Value |
|---|---|
| Pedestrian-graph nodes | 855,905 |
| Pedestrian-graph segments | 963,620 (1,933,080 directed edges) |
| Connected components | 6,083, of which 3,171 have fewer than 3 nodes and 1,110 have 10 or more |
| Components that are a single unconnected gap-fill segment | 1,108 |
| Largest component | 611,991 nodes (71.5%) |
| Second largest | 136,147 nodes: Staten Island |

Share of each borough's pedestrian nodes in the largest component:

| Brooklyn | Manhattan | Bronx | Queens | Staten Island |
|---|---|---|---|---|
| 96.2% | 89.4% | 85.7% | 84.0% | 0% (81.9% in its own largest) |

Staten Island has no walkable link to another borough, so it is a separate component by geography. The rest of the fragmentation is OSM: pedestrian ways that share no node with their neighbours.

### Borough joins, bridge by bridge

A citywide component count cannot show a cut bridge, so each bridge with a pedestrian path was tested: snap a point at each landfall to the largest component in that borough, and compare the walking distance with the straight line. "Joined" means the walk is at most 2.5 times the straight line.

| Bridge | Straight line | Walk on pedestrian edges | Walk with street centrelines | Verdict |
|---|---|---|---|---|
| Brooklyn Bridge | 1,866 m | 1,921 m | 1,921 m | joined |
| Manhattan Bridge | 1,944 m | 2,312 m | 2,308 m | joined |
| Williamsburg Bridge | 2,298 m | 2,450 m | 2,450 m | joined |
| Ed Koch Queensboro Bridge | 2,332 m | 2,532 m | 2,515 m | joined |
| Roosevelt Island Bridge | 594 m | 1,034 m | 1,034 m | joined |
| RFK Triborough Bridge, Queens span | 1,633 m | 2,916 m | 2,913 m | joined |
| RFK Triborough Bridge, Bronx span | 597 m | 2,484 m | 2,475 m | not joined: the route found is another crossing |
| Randall's Island Connector | 446 m | 1,301 m | 1,018 m | joined only along a street centreline |
| Willis Avenue Bridge | 564 m | 795 m | 795 m | joined |
| Third Avenue Bridge | 414 m | 545 m | 545 m | joined |
| Madison Avenue Bridge | 476 m | 626 m | 623 m | joined |
| 145th Street Bridge | 545 m | 2,474 m | 785 m | joined only along a street centreline |
| Macombs Dam Bridge | 661 m | 836 m | 780 m | joined |
| High Bridge | 447 m | 801 m | 801 m | joined |
| Washington Bridge | 706 m | 1,033 m | 1,033 m | joined |
| University Heights Bridge | 397 m | 827 m | 827 m | joined |
| Henry Hudson Bridge | 535 m | 1,524 m | 1,516 m | a path exists at 2.85 times the straight line |
| Broadway Bridge (Inwood to Marble Hill) | 232 m | 1,282 m | 579 m | joined only along a street centreline |
| Pulaski Bridge | 944 m | 1,108 m | 1,108 m | joined |
| Greenpoint Avenue Bridge | 665 m | 1,099 m | 1,099 m | joined |
| Kosciuszko Bridge | 958 m | 1,623 m | 1,403 m | joined |
| Grand Street Bridge | 395 m | 754 m | 753 m | joined |
| Marine Parkway Bridge | 2,300 m | 2,573 m | 2,571 m | joined |

18 of 23 are joined on pedestrian edges. The landfall points are approximate and were chosen by hand, so a ratio near the threshold says little. On the three bridges joined only along a street centreline, the pedestrian edges on or beside the bridge do not connect through; the cause was not looked into bridge by bridge. The pedestrian edges that cross a borough line form 36 clusters; all but one small fragment on the Brooklyn and Queens land border are in the largest component.

In v0.3.1-nyc.1 the boroughs shared 13 nodes and every bridge above except the Brooklyn Bridge promenade was cut.

### Nodes on no edge

44,065 nodes are not an endpoint of any edge: 35,993 curb ramps (see below) and 8,072 OSM nodes that no kept edge uses.

## Curb ramps

All 217,679 ramps of the NYC DOT survey are in the file, one node each.

| | Count |
|---|---|
| Curb nodes that are an endpoint of a pedestrian edge | 181,686 (83.5%) |
| of which an endpoint of a crossing edge | 126,128 |
| Not on the graph: no pedestrian vertex within 5 m | 18,305 |
| Not on the graph: another ramp already holds the node | 17,688 |

A node holds one ramp's fields. Where several ramps land on one node, the first keeps it and the others stay in the file at their surveyed position, on no edge. So the second group is on the graph in effect: the node they share is a curb node.

Attached share by borough: Brooklyn 93.8%, Staten Island 93.6%, Queens 82.3%, Manhattan 78.9%, Bronx 60.7%. A ramp attaches only where OSM has a separately mapped sidewalk, crossing or footway within 5 m, so the share follows how completely OSM maps each borough.

`tactile_paving` follows the survey's `DWS_CONDITIONS` field on every one of the 217,679 nodes: `no` where it reads Missing (128,911), `yes` where it reads Good Condition, Defective or one of the two Off Ramp values (88,624), and no tag where it reads Not Applicable (144). No node carries a sentinel code (555, 777, 888, 999) as a slope.

279,172 of the 437,510 crossing edges (63.8%) have a curb node at one of their own ends. A crossing is usually several edges, and a ramp is as often on a sidewalk vertex beside the crossing as on the crossing itself, so this understates how many crossings have ramps. Counting a surveyed ramp within 5 m of each end of the whole crossing, 387,464 crossing edges (88.6%) are on a crossing with a ramp at both ends. The routing layer uses that test (`scripts/osw_to_unweaver.py`).

### NYC DOT curb-ramp slopes

The survey was captured almost entirely in 2018 (216,220 of 217,679 rows), and DOT's program data lists 88,725 of the surveyed ramps at corners rebuilt after their survey date, so many of these measurements describe ramps that have since been replaced.

The values are percentages signed by direction, so magnitudes are compared. Sentinel codes are excluded.

| Screening test (federal design limit) | Share of measured ramps within it |
|---|---|
| Running slope within 1:12, 8.33% (2010 ADA Standards 405.2 via 406.1; PROWAG R304.2.1) | 74.1% (157,199 of 212,194) |
| Cross slope within 1:48, 2.08% (405.3; R304.2.2) | 66.4% (140,866 of 212,111) |
| Counter slope within 1:20, 5% (406.2) | 67.8% (143,827 of 212,104) |

These are not compliance findings. The dataset's own description says its measurements "are not indicative of whether a particular ramp is compliant" with the ADA, because DOT applies tolerances, the existing-site exception for short ramps (Table 405.2) and a technical infeasibility review. DOT's own per-ramp assessment of the same survey (ArcGIS layer `CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD`, last edited December 2020) passes 79.8% of ramps on running slope, 80.0% on cross slope and 76.4% on counter slope, and passes 1.5% (3,366 of 217,679) on every attribute at once.

## Elevation and incline

| | Value |
|---|---|
| Nodes with `ext:elevation_m` | 1,189,642 of 1,189,651 |
| Nodes at exactly 0.0 m | 455 (0.04%), at most 0.12% in any borough |
| Highest node by borough | Staten Island 122.5 m, Bronx 84.9 m, Manhattan 80.4 m, Queens 79.5 m, Brooklyn 62.5 m |
| Nodes below -1 m | 149 |
| Edges with `incline` | 2,668,664 of 2,684,681 (99.4%) |
| Edges with incline exactly 0 | 8,744 (0.3%) |
| Sidewalk edges steeper than 5% | 6.3% |
| Sidewalk edges outside the wheelchair profile's limits (up 8.3%, down 10%) | 1.5% |

Read incline as an estimate of the terrain, not a measurement of the path.

1. The elevation model is bare earth and includes the river bed. A node on a bridge, a deck or a pier gets the ground or water below it; that is where the negative elevations come from. From v0.3.2 an edge whose OSM way is a bridge or a tunnel carries `ext:structure` and no `incline` (15,841 edges). Node elevations on those structures are still the ground below.
2. Each borough's tile is 5 to 12 m per pixel (the 1 m service is resampled to 3,000 pixels a side), and half the edges are shorter than 7 m. Elevations are interpolated between pixel centres. With the nearest-pixel sampling used before, the share of edges outside the limits rose as edges got shorter; now it does not depend on edge length (2% to 4% in every length class up to 50 m).
3. Against an independent measurement, the survey's gutter slope at 6,245 Staten Island ramps, the incline of the adjoining sidewalk edges has a rank correlation of 0.42 and is within 2 percentage points for 74% of ramps. That is agreement in the large, not edge by edge.

## Sidewalk width

`width` is on 828,509 edges, 820,940 of them sidewalks (87.8% of sidewalk edges). The median is 3.12 m: 2.57 m on Staten Island, 3.07 m in Queens, 3.44 m in Brooklyn, 3.62 m in the Bronx and 4.26 m in Manhattan.

The value is 2 × area / perimeter of the planimetric polygon the edge lies in, with interior rings counted: the mean width of the whole polygon, not the clear width at that spot. Against 5,805 transects cut across the polygon at the edge's midpoint, the median ratio of `width` to transect is 0.94 and the median absolute difference 0.58 m. It runs low in Brooklyn (ratio 0.87) and is closest on Staten Island (0.98, 0.30 m).

## Planimetric gap-fill sidewalks

1,159 segments (2,318 directed edges, 60 km) are derived from planimetric sidewalk polygons that have no OSM sidewalk within 10 m. Each is the long axis of the polygon's minimum rotated rectangle, kept only if at least 90% of it lies inside the polygon and it does not lie along a crossing or footway OSM already has.

This is the least reliable layer. In a seeded random sample of 18 drawn over 2022 to 2025 orthoimagery, 9 are on a sidewalk or walkway, 1 is on other pedestrian paving, 4 are unclear and 4 are wrong (a truck apron, a parking lot, a paved yard and a cemetery road). One rater, not blind. They are also unconnected: only 45 of the 1,159 segments are joined to the rest of the graph, and the others make up 1,108 components of their own, so a router cannot use them. They are 0.25% of sidewalk edges, and `ext:source = nyc_planimetric_sidewalks` selects them.

## Attribute coverage

| Attribute | Coverage | Notes |
|---|---|---|
| `surface` | 1,138,913 edges (42.4%) | asphalt 648,630; concrete 356,138; paving_stones 68,532 |
| `crossing:markings` | 380,888 of 437,510 crossings (87.1%) | `yes` 272,110; `zebra` 108,778. OSM `crossing=uncontrolled` is mapped to `zebra`, which asserts more than the source says. |
| `kerb` | 217,679 curb nodes | `lowered` on all (the survey does not distinguish flush from lowered) |
| `ext:source`, `ext:pipeline_version` | every feature | `ext:pipeline_version` is `0.1.0`, the pipeline's own version, not the release's |
| `ext:source_timestamp` | every edge; 38,245 nodes | OSM-derived nodes lack it |
| `ext:osm_id` | every OSM-derived edge | |
| `ext:borough` | all but 64 edges and 2,252 nodes | The 64 are gap-fill segments at the city line; the nodes are injected gap-fill endpoints |

## Routing

Unweaver needs `mod_spatialite`, which is not installed on the build machine and would be a global install. The check below uses the repository's own Unweaver inputs: `scripts/osw_to_unweaver.py` makes the layer Unweaver would read, `unweaver-project/cost-wheelchair.py` decides edge by edge, and only the graph search is re-implemented.

The wheelchair profile now refuses steps and street centrelines, refuses a crossing unless a surveyed ramp lies within 5 m of each of its ends, and refuses an edge steeper than 8.3% up or 10% down.

Share of 2,000 random origin and destination pairs with a route, both ends in the same borough (95% intervals are about 2 points wide either way). A sub-agent with no access to this work recomputed the pedestrian and wheelchair rows from the file with its own code and 20,000 pairs, and came within 3 points in every cell:

| Profile | Brooklyn | Bronx | Manhattan | Queens | Staten Island | Ends anywhere in the city |
|---|---|---|---|---|---|---|
| Every edge, street centrelines included | 97.8% | 92.2% | 81.7% | 93.0% | 93.8% | 61.8% |
| Pedestrian edges only | 94.0% | 72.7% | 81.5% | 71.9% | 68.0% | 52.2% |
| Pedestrian edges without steps (the most the wheelchair profile could reach) | 93.7% | 71.9% | 77.3% | 71.2% | 67.3% | 51.2% |
| Wheelchair profile without its incline limits | 87.9% | 58.3% | 71.0% | 66.1% | 52.2% | 45.3% |
| **Wheelchair profile** | **84.7%** | **37.4%** | **51.4%** | **60.4%** | **41.8%** | **38.2%** |

The citywide column is capped by geography: a pair with one end on Staten Island has no route.

Three things separate the wheelchair profile from the pedestrian network, and none of them is a finding about the city:

- **Fragmentation.** Even with every rule off, a quarter to a third of pairs outside Brooklyn have no route, because OSM's pedestrian ways do not all connect.
- **Ramps.** The profile needs a surveyed ramp within 5 m of each end of a crossing. That costs 5 to 15 points. With the stricter test that the ramp must be on the crossing's own nodes, the profile reaches 25% of pairs in Brooklyn, 16% in Manhattan and 1% to 2% elsewhere, so the result depends heavily on how ramps are tied to crossings.
- **Incline.** The limits cost 3 points in Brooklyn and 21 in the Bronx. Part of that is real hills and part is wrong incline next to structures: the step-free route from the Manhattan end of the Brooklyn Bridge to DUMBO is 3.8 km, and with the limits on it is 9.1 km by way of the Williamsburg Bridge, because two approach edges of the promenade that OSM does not tag as a bridge carry inclines of 14% and 19%.

For the ten landmark pairs of `scripts/route_test.py` the profile finds a route for all ten, none with a steps edge or a street centreline (in v0.3.1-nyc.1 the same profile routed over steps and centrelines, and 7 of 10 had no path once those were excluded). Nine are 1.1 to 2.2 times the straight line; the tenth is the Brooklyn Bridge detour above.

Do not present a route from this graph as wheelchair accessible. The ramp data is from 2018 and says a ramp was there, not that it meets a standard. Incline is a terrain estimate. Nothing in the graph knows about sidewalk condition, obstructions, construction or signal timing.

## Independent imagery check

The v0.3.1-nyc.1 review drew 105 features over NY State orthoimagery, which the pipeline does not use, and found 30 of 30 OSM sidewalk edges, 30 of 30 crossings and 30 of 30 curb nodes on target. That geometry is OSM's and the survey's and has not changed. For v0.3.2 the gap-fill edges were re-sampled (see above) and four landmark routes were drawn over the imagery: all four lie on sidewalks, crosswalks and paths, and one of them is the wrong detour described under Routing.

## Reproducibility

```bash
# from a fresh checkout, Python >= 3.11
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .

# 1. Build (city-wide: about 40 minutes and 33 GB of memory at peak on a 32 GB
#    Apple Silicon machine, 11 GB under data/, 6 GB under output/)
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

The build reads one dated OSM extract, pinned by URL and SHA-256 in `config/sources.yaml`, so two builds from the same commit read the same OSM data. The NYC Open Data sources are downloaded at build time; the ramp survey has not changed since October 2021.

## Open work

1. **Attach ramps to crossings, not to the nearest vertex.** For about a quarter of crossing edges the ramp is on a neighbouring sidewalk vertex. The survey names the street each ramp is on.
2. **Measure incline over a longer baseline** than one edge, and fetch finer DEM tiles, before relying on a slope limit.
3. **Join the ramp survey to DOT's program-progress data** (`e7gc-ub6z`) so rebuilt corners are not described by their old measurements.
4. **Drop or replace the gap-fill layer.** A rectangle axis is not a centerline; a skeleton of the polygon would be.
5. **Fold the endpoint snap into Stage 4** and **replace Stage 5 with the official validator**.
6. **Report the unconnected bridge sidewalks to OSM mappers** (145th Street, Broadway Bridge, the RFK Bronx span, the Randall's Island Connector).
