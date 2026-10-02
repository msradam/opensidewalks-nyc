# Quality Report: opensidewalks-nyc v0.3.1-nyc.1

> Audit date: 2026-07-03. Corrected after an independent review on 2026-10-02 (see "Corrections in this revision"). Artifact audited: the published `nyc-osw.geojson` of release v0.3.1-nyc.1, built by `python -m pipeline build` plus the post-build endpoint snap (`scripts/snap_endpoints.py`). Schema target: **OSW v0.3** (`OpenSidewalks/OpenSidewalks-Schema`, latest tag `0.3`). Validators run: **`python-osw-validation` 0.4.4, 0.4.5 and 0.5.0**.
>
> The v0.3.0-nyc.1 report is preserved in git history.

## Headline verdict

**The artifact is schema-valid under OSW v0.3, and it has known defects that the validator does not see.**

- `python-osw-validation` 0.4.4 and 0.4.5 return `is_valid: True` with zero errors across all 3,374,261 features (1,155,380 nodes, 2,218,881 edges).
- `python-osw-validation` 0.5.0 (released 2026-08-05, after this artifact) returns `is_valid: False`: it rejects coordinates with more than 7 decimal places, which the planimetric gap-fill edges and injected nodes have.
- Elevation and incline are wrong for about 80% of the graph, every curb ramp is tagged `tactile_paving=yes` regardless of the survey, the Bronx is not connected to the other boroughs, sidewalk widths are overstated, and most planimetric gap-fill "sidewalks" are not sidewalks. Each is described under "Known defects". All are fixed in the pipeline code after this release; the published files are unchanged.

The validator checks file structure, the JSON schema, unique IDs, that `_u_id`/`_v_id` resolve, geometry validity, and that edge endpoints equal their node coordinates. It does not check connectivity, whether an attribute value is true, or whether a line is where a sidewalk is. "Zero errors" is a statement about form.

## Corrections in this revision

| Earlier statement | Correction |
|---|---|
| "Running slope ≤ 5% (ADA running-slope cap)": 33.0% compliant; "two-thirds of NYC's surveyed curb ramps exceed the ADA running-slope threshold" | Wrong threshold. A curb ramp run may be as steep as 1:12 (8.33%); 5% is the limit for a walkway. 73.8% of measured ramps are within 1:12. See the ramp section. |
| "Cross slope ≤ 2%": 82.6% compliant | Computed on signed values, so every negative cross slope passed. On magnitudes, 66.4% are within 1:48 (2.08%). |
| Largest component "spanning Manhattan, Brooklyn, Queens, and the Bronx" | The Bronx is its own component. The largest component covers Manhattan, Brooklyn and Queens. |
| 11,807 components, 849,417 pedestrian nodes, largest 515,336 | These came from the Stage 4 topology log, which runs before the endpoint merge. Measured on the artifact: 10,743 components, 678,310 nodes, largest 428,068. |
| `incline` on 99.98% of edges, `ext:elevation_m` on 100% of nodes | The fields are present, but 80.3% of node elevations are a spurious 0.0. |
| `width` on 815,782 sidewalk edges | 620,595 edges carry `width` (615,099 of them sidewalks). 815,782 was a Stage 3 count before the merge dropped edges. |
| `tactile_paving=yes` "wherever the DOT survey recorded a detectable warning surface" | It is `yes` on every curb node. The survey records the surface as missing on 59% of ramps. |
| "zero leaked `999.0` sentinels" | True for 999. The survey also uses 888 and 777, and 829 curb nodes carry one of those as a slope. |
| `ext:source_timestamp` on 100% of features | Present on 66.6%. OSM-derived nodes lack it. |

## What changed since v0.3.0-nyc.1

| | v0.3.0-nyc.1 (restore script) | v0.3.1-nyc.1 (pipeline) |
|---|---|---|
| Features | 955,026 | 3,374,261 |
| Edges | 460,051 | 2,218,881 |
| Nodes | 494,975 | 1,155,380 |
| Connected components (pedestrian graph) | 107,604 | 10,743 |
| Largest component | 159,506 nodes (32.6%) | 428,068 nodes (63.1% of pedestrian-graph nodes) |
| Planimetric widths + gap-fill | absent | present, both defective (see below) |
| Incline / elevation | present (USGS 10 m DEM) | NYC 2017 LiDAR DTM, valid only inside the Bronx tile |
| Provenance | reconstructed heuristically post-hoc | native from acquisition |
| Reproducible from public sources | no (source file gone) | yes |

## Schema conformance

| Check | Result |
|---|---|
| Input | `nyc-osw-osw-split.zip` (split `nyc.nodes.geojson` + `nyc.edges.geojson`) |
| `python-osw-validation` 0.4.4 | `is_valid: True, errors: 0` |
| `python-osw-validation` 0.4.5 | `is_valid: True, errors: 0` |
| `python-osw-validation` 0.5.0 | `is_valid: False`; first errors are "contains coordinates with more than 7 decimal places" on the gap-fill edges. The validator stops reporting at 20 errors. |
| Geometry-to-node coordinate check (0.4.0+) | Pass: every edge terminal vertex equals its referenced node coordinate exactly |
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` |
| Root metadata | `dataSource`, `dataTimestamp`, `pipelineVersion`, `region` present |

Two pipeline steps are behind the 0.4.x result. Stage 4 drops edges that the 2 m endpoint merge collapses into zero-length self-loops (359,340 edges in the release build, per its build log). The post-build snap then moves every edge endpoint onto its node's coordinate. The merge is single-linkage, so it chains: in a Staten Island rebuild, 10% of endpoints moved, the median move was 1.8 m, 2,309 endpoints moved more than 5 m and the largest move was 33 m.

## Feature composition

| OSW type | Count |
|---|---|
| Sidewalk edges | 703,467 (697,345 from OSM, 6,122 planimetric gap-fill) |
| Crossing edges | 424,564 |
| Footway / steps edges | 429,328 (416,950 footway, 12,378 steps) |
| Street edges | 661,522 |
| Curb-ramp nodes | 199,836 |
| Other point nodes | 955,544 |
| **Total** | **3,374,261** |

Edges are directed. Most walkable segments appear once per travel direction, with `incline` signed in the direction of travel. There are 1,184,309 unique node pairs. Two groups appear in one direction only: 151,145 street segments (OSM one-way streets, which a pedestrian can walk both ways) and 5,934 sidewalk segments (the gap-fill edges).

The curb-ramp nodes hold 199,836 of the 217,679 surveyed ramps. When several ramps snap to one node, the node keeps one ramp's fields and the other 17,843 survey records are not in the artifact.

## Graph integrity

Measured on the published artifact. The pedestrian graph is the sidewalk, crossing, footway and steps edges.

| Metric | Value |
|---|---|
| Edges with unresolved `_u_id`/`_v_id` | 0 |
| Zero-length edges | 0 (dropped at the Stage 4 merge) |
| Pedestrian-graph nodes | 678,310 |
| Pedestrian-graph segments | 778,123 (1,557,359 directed edges) |
| Connected components | 10,743, of which 7,569 have fewer than 3 nodes |
| Largest component | 428,068 nodes (63.1%): Manhattan, Brooklyn and Queens |
| Second largest | 83,984 nodes: Staten Island |
| Third largest | 61,919 nodes: the Bronx |
| Share of each borough's pedestrian nodes in the largest component | Brooklyn 95.3%, Manhattan 89.8%, Queens 83.2%, Bronx 0%, Staten Island 0% |

**The boroughs are barely joined.** OSM is queried one borough polygon at a time and each result is cut at the polygon, so every segment that crosses a borough line is missing. Only 13 nodes are shared between boroughs: 10 between Brooklyn and Queens, 1 between Manhattan and Brooklyn (on the Brooklyn Bridge promenade), 2 between Manhattan and the Bronx (on a sidewalk fragment at Marble Hill, outside the largest component), and none between Manhattan and Queens. The Williamsburg, Manhattan, Queensboro, RFK and Harlem River bridges are all cut mid-span. A route from the Manhattan end of the Williamsburg Bridge to the Brooklyn end is 7.9 km in this graph.

Two integration numbers to know before consuming the nodes:

- **220,084 nodes are not referenced by any edge.** Most are original endpoint locations whose references were remapped to a canonical node by the 2 m merge, plus curb ramps that did not integrate into the graph.
- **134,077 of 199,836 curb-ramp nodes (67%) are edge endpoints**, and 111,292 are endpoints of a crossing edge. The rest carry their survey data in the file but are not attached. The cause is a pipeline defect: the ramp took the ID of the endpoint it snapped to, and the merge then renamed that endpoint without renaming the ramp. In an imagery sample, 15 of 15 unattached ramps sat at a corner on the graph.

Per-borough edge counts: QN 768,101; BK 515,555; SI 371,831; MN 290,897; BX 266,375. 6,122 gap-fill edges and 10,858 nodes have no `ext:borough`. The sidewalk-to-street edge ratio falls from 1.80 in Manhattan to 0.76 in the Bronx, which tracks OSM sidewalk mapping density.

## Geometric sanity

| Check | Result |
|---|---|
| Invalid geometries (SFA) | 0 |
| Zero-length or sub-0.5 m edges | 0 |
| Features outside a conservative NYC bbox | 27, all within ~50 m of the bbox edge (real boundary features) |
| Edge length (metres) | min 2.0, median 8.4, p95 102, p99 213, max 2,943 |
| Edges over 500 m | 147 (long arterials and park paths without intermediate intersections) |

Every edge is a two-point segment between consecutive OSM nodes (the OSM graph is not simplified), which is why the median edge is 8.4 m.

## Known defects in the published artifact

Each is fixed in the pipeline code after this release. A corrected release needs a rebuild.

1. **Elevation and incline are wrong outside the Bronx tile.** 927,754 of 1,155,380 nodes (80.3%) have `ext:elevation_m` of exactly 0.0, and 1,787,431 edges (80.6%) have both endpoints at 0.0 and so an `incline` of 0. This covers all of Brooklyn and Staten Island, 91% of Manhattan and 82% of Queens. The DEM is fetched as one tile per borough, the tiles carry no nodata value, and a point outside a tile samples as 0.0, so the first tile (the Bronx) assigned 0.0 to every node outside its extent. Staten Island's high point is about 118 m in the same DEM; the artifact's maximum there is 0.0.
2. **Where elevation is real, incline is coarse.** Each borough tile is capped at 3,000 pixels a side, which is 5 to 10 m per pixel, not the 1 ft native resolution. On the 430,999 edges with real elevations, the 1st and 99th percentile inclines are -16.6% and +16.5%, and 85.9% of sidewalk edges are at or below 5%.
3. **`tactile_paving=yes` is on all 199,836 curb nodes.** The pipeline set it whenever the survey's `DWS_CONDITIONS` field was non-empty. That field reads "Missing" on 128,911 of 217,679 ramps (59%).
4. **Sidewalk `width` is overstated.** Width is 2 × area / perimeter of the planimetric polygon, using the outer ring only. Many polygons are rings around a block, and for those the formula returns about twice the width. The median sidewalk width in the artifact is 5.65 m. Against polygon transects in Staten Island the published formula had a median error of 0.76 m; counting interior rings cuts that to 0.30 m.
5. **Planimetric gap-fill sidewalks are mostly not sidewalks.** The 6,122 gap-fill edges (694 km) are the long axis of each polygon's minimum rotated rectangle. For a block-shaped polygon that axis runs through the block. In a random sample of 15 checked against 2022 to 2025 orthoimagery, 13 crossed buildings, yards, cemeteries or roadway, 1 followed a real path and 1 was unclear.
6. **The Bronx and the bridges** (see Graph integrity).
7. **Survey sentinels 888 and 777 are present as slopes** on 829 curb nodes.
8. **Ramps detached from the graph** (see Graph integrity).

## Attribute coverage and distributions

| Attribute | Coverage | Notes |
|---|---|---|
| `incline` | 2,218,430 / 2,218,881 edges (99.98%) | Defect 1 applies: 1,830,745 values are exactly 0. |
| `ext:elevation_m` | 1,155,380 / 1,155,380 nodes | Defect 1 applies. min -3.5 m, max 84.9 m (the Bronx). |
| `width` | 620,595 edges (615,099 sidewalks) | Defect 4 applies. OSM `width` tags first, planimetric estimate fills the gaps. |
| `surface` | 921,303 edges (41.5%) | asphalt 555,711; concrete 267,531; paving_stones 46,470; the rest smaller |
| `crossing:markings` | 369,932 / 424,564 crossings (87%) | `yes` 266,688; `zebra` 103,244. OSM `crossing=uncontrolled` is mapped to `zebra`, which asserts more than the source says. |
| `kerb` | 199,836 curb nodes | `kerb=lowered` universally (the survey does not distinguish flush from lowered) |
| `tactile_paving` | 199,836 curb nodes | Defect 3 applies. |

All `surface` and `crossing:markings` values are OSW-canonical.

### NYC DOT curb-ramp slopes

The artifact carries the DOT survey's running, cross and counter slope on 196,069 curb nodes. The survey was captured almost entirely in 2018 (216,220 of 217,679 rows), and DOT reports 70,615 corners rebuilt between July 2017 and June 2026, so many of these measurements describe ramps that have since been replaced.

The values are percentages signed by direction, so magnitudes are compared. Sentinel codes are excluded.

| Screening test (federal design limit) | Share of measured ramps within it |
|---|---|
| Running slope within 1:12, 8.33% (2010 ADA Standards 405.2 via 406.1; PROWAG R304.2.1) | 73.8% (144,175 of 195,240) |
| Cross slope within 1:48, 2.08% (405.3; R304.2.2) | 66.4% (129,559 of 195,161) |
| Counter slope within 1:20, 5% (406.2) | 68.0% (132,671 of 195,153) |

These are not compliance findings. The dataset's own description says its measurements "are not indicative of whether a particular ramp is compliant" with the ADA, because DOT applies tolerances, the existing-site exception for short ramps (Table 405.2) and a technical infeasibility review. DOT's own per-ramp assessment of the same survey (ArcGIS layer `CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD`, last edited December 2020) passes 79.8% of ramps on running slope, 80.0% on cross slope and 76.4% on counter slope, and passes 1.5% (3,366 of 217,679) on every attribute at once.

At the edge level, incline cannot be summarised citywide because of defect 1.

## Provenance coverage

| Field | Coverage |
|---|---|
| `ext:source` | 100% of 3,374,261 features |
| `ext:source_timestamp` | 2,248,847 features (66.6%): every edge, and 29,966 nodes. OSM-derived nodes lack it. |
| `ext:pipeline_version` | 100%, value `0.1.0` (the `pipeline_version` in `config/build.yaml`, not the release version) |
| `ext:osm_id` | OSM-derived edges |
| `ext:borough` | `MN`/`BK`/`QN`/`BX`/`SI` on all but 6,122 edges and 10,858 nodes |

When a DOT curb ramp and an OSM node occupy the same location, the merged node keeps the OSM node's `ext:source` while the DOT fields (`ext:ramp_id`, slopes, streets) preserve the survey linkage. Only 19,108 curb nodes have `ext:source` of `nyc_dot_ramps`.

The borough polygons come from OSM through Nominatim, because the NYC Open Data boundary dataset the pipeline names (`7t3b-ywvw`) has been withdrawn. The region polygon is therefore ODbL data too.

## Other limitations

1. **Coverage follows OSM.** Where OSM has no separately mapped sidewalk, the graph has none (apart from the gap-fill edges of defect 5).
2. **`kerb=lowered` for every DOT ramp.** The survey does not distinguish flush from lowered ramps.
3. **Crossings are not always incident to street-edge endpoints,** consistent with OSM's representation.
4. **No MTA ADA station index ships.** The dataset ID the pipeline names for it (`drh3-e2fd`) is a hydrography layer, and the GTFS fallback has no accessibility column.

## Independent imagery check

A seeded random sample of 105 features was drawn from the artifact and overlaid on NY State orthoimagery (2022 to 2025), which the pipeline does not use: 6 OSM sidewalk edges, 6 crossing edges and 6 curb-ramp nodes in each borough, and 15 gap-fill edges.

| Stratum | On target |
|---|---|
| OSM sidewalk edges | 30 of 30 on or within about 3 m of a visible sidewalk |
| Crossing edges | 30 of 30 at a crossing |
| Curb-ramp nodes | 30 of 30 at a corner or crossing end |
| Gap-fill sidewalk edges | 1 of 15 (13 wrong, 1 unclear) |

One rater, not blind to the stratum. The sample is small: it can show a gross problem (the gap-fill edges) and cannot bound a low error rate.

## Routing

The v0.3.0-nyc.1 artifact was tested with Unweaver (`validators/route_test_results.md`). Those results do not apply to this artifact, and the suite has not been run against it.

A re-implementation of the shipped profiles on this artifact, for the same ten landmark pairs, found:

- The wheelchair cost function (`unweaver-project/cost-wheelchair.py`) blocks crossings that have no curb node and edges outside the incline limits, and nothing else. It allows 11,010 of the 12,378 steps edges and every street-centreline edge. The wheelchair route from Penn Station to Grand Central starts and ends on steps and runs 29% of its length along a street centreline.
- The incline limits do nothing on these routes: every edge on every route has the spurious zero incline of defect 1.
- With steps and street centrelines excluded, 7 of 10 pairs have no path.
- `scripts/route_test.py` snaps landmarks to the largest component, which moves both Bronx landmarks about 600 m across the Harlem River into Manhattan.

Do not present routes from this artifact as wheelchair-accessible.

## Reproducibility

```bash
# from a fresh checkout, Python >= 3.11
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .

# 1. Build (city-wide: ~60-90 min, ~10 GB scratch)
python -m pipeline build

# 2. Snap edge endpoints onto node coordinates, emit the validator ZIP
python scripts/snap_endpoints.py --input output/nyc-osw.geojson

# 3. The gate. The validator pins geopandas==0.14.4, which breaks the
#    pipeline, so run it in its own environment.
uv run --no-project --isolated --with python-osw-validation python -c "
from python_osw_validation import OSWValidation
r = OSWValidation('output/nyc-osw-osw-split.zip').validate()
print('valid:', r.is_valid, 'errors:', len(r.errors or []))"

# 4. Data-quality audit. Needs the schema once:
mkdir -p validators/schema-cache
curl -sLo validators/schema-cache/opensidewalks.schema.json \
  https://raw.githubusercontent.com/OpenSidewalks/OpenSidewalks-Schema/0.3/opensidewalks.schema.json
python validators/quality_audit.py output/nyc-osw.geojson
```

A Staten Island bounding-box build from a fresh clone on 2026-10-02 took about 8 minutes and passed validator 0.4.4. With the fixes that followed this release it passes 0.5.0 as well.

## Open work

1. **Cut a corrected release** from the fixed pipeline, and state in its notes what changed.
2. **Tile the DEM finer** than one tile per borough, so incline on a short edge means something.
3. **Replace the single-linkage 2 m merge** with one that cannot chain, or restrict it to cross-source endpoint pairs.
4. **Fold the endpoint snap into Stage 4** and **replace Stage 5 with the official validator**.
5. **Derive real centerlines** for planimetric polygons (a skeleton, not a rectangle axis) before using them for gap-fill again.
6. **Join the ramp survey to DOT's program-progress data** (`e7gc-ub6z`) so rebuilt corners are not described by their old measurements.
7. **Give the routing profiles a steps rule and a street rule,** then run them.
