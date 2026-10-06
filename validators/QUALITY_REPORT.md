# Quality Report: OpenSidewalks NYC v0.3.7-nyc.1

> Audit date: 2026-10-06. Artifact audited: `nyc-osw.geojson` of the v0.3.7-nyc.1 build, made by `python -m pipeline build` from an empty cache at commit `070f35a` for stages 1 to 3 and, after a merge step failed and was corrected, stages 4 to 6 at commit `c442658` on the same staged data ([`evaluation/v0.3.7/build.json`](../evaluation/v0.3.7/build.json) records the failure), plus the post-build endpoint snap (`scripts/snap_endpoints.py`). OpenStreetMap data as of 2026-10-01T20:22:06Z (Geofabrik extract `new-york-261001.osm.pbf`). Schema target: **OSW v0.3**. Validator: **`python-osw-validation` 0.5.0**.
>
> The counts and checks of the artifact were measured on v0.3.7 by `validators/post_build_checks.py` ([`evaluation/v0.3.7/checks.json`](../evaluation/v0.3.7/checks.json)). The same checks run on v0.3.6 are in [`checks_v0.3.6.json`](../evaluation/v0.3.7/checks_v0.3.6.json), for the before figures; v0.3.6's own evidence is in [`evaluation/v0.3.6/`](../evaluation/v0.3.6/). The routing, landmark, deck-height and imagery sections were measured on v0.3.3 and say so; their result files are named there and are kept under [`evaluation/`](../evaluation/).

## Headline verdict

**The artifact passes `python-osw-validation` 0.5.0 against OSW v0.3, which checks form, not the schema's topology rules (the README lists the known deviations from the schema). Incline describes the walking surface on bridges and elevated ways. Do not present a route from it to anyone without checking the route.**

- `python-osw-validation` 0.5.0 returns `is_valid: True` with zero errors across all 3,995,589 features (1,187,062 nodes, 2,806,326 edges, 2,201 zones).
- The validator checks form. The checks in this report are the ones it does not do, and each number below is traceable to a result file.
- What changed in v0.3.3: nodes on and beside bridges and elevated ways take their height from the LiDAR point clouds, not the terrain under the deck; the terrain model is read at 2 m; node heights are smoothed along the path over short edges so survey noise no longer reads as a grade; and the rule that ties a surveyed ramp to a crossing has been checked remotely against aerial imagery by language-model raters (nothing was checked on the ground). The step-free route from the Brooklyn Bridge's Manhattan end to DUMBO is 3.8 km.
- What changed in v0.3.5, an internal build that was not released, and so is first released in v0.3.6: the 2,201 pedestrian areas that OSM maps as `area=yes` are Pedestrian Zones, not Edges along their outlines; the root `dataTimestamp` is the OSM data time; a curb ramp's provenance names the survey; an OSM `path` keeps its origin. No geometry changed on the features shared with v0.3.4 ([`compare_v034.json`](../evaluation/v0.3.5/compare_v034.json)).
- What changed in v0.3.6: the terrain service's "no data" value is no longer read as ground at 0 m; a node inside a tunnel no longer carries the height of the ground above it; an edge whose two heights give a grade at a staircase's pitch or more has no incline and is marked `ext:incline_unknown`, and the wheelchair profile refuses it; every GraphML edge carries its length ([`evaluation/v0.3.6/`](../evaluation/v0.3.6/)).
- What changed in v0.3.7: a street edge carries what OSM says about its sidewalks in `ext:sidewalk`, and the wheelchair profile walks a street tagged as having one; a surveyed ramp sits on the end of the crossing it serves; OSM's own kerb and elevator nodes are carried; a cycleway or track with no `foot` tag is kept unless it is a one-way cycleway. The counts are under [What v0.3.7 changed](#what-v037-changed).
- What remains weak, in order of how much it matters: the curb ramp survey is mostly from 2018; a seventh of the ramps are not on the graph; incline cannot see a curb ramp, and where this pipeline's height estimate jumps across one short edge at the foot of a ramp the edge reads steep or, at a grade of 0.5 or more, has no incline and is marked unknown; an edge with no incline and no mark (a tunnel, an elevator) was not measured and the wheelchair profile passes it; a fifth to a half of pairs outside Brooklyn have no wheelchair route, mostly because much of the Bronx, Queens and Staten Island has streets with no sidewalk mapped in OSM in any form (the `sidewalk=*` tags, which earlier reports blamed, are on few of those streets and are now read).

## What v0.3.7 changed

Four data changes, each measured on the whole graph before and after. [`compare_v036.json`](../evaluation/v0.3.7/compare_v036.json) holds every feature of v0.3.6 against v0.3.7, each difference grouped by the change that caused it; the tests are in `tests/test_v037.py`. The v0.3.6 changes and their table are in [`evaluation/v0.3.6/`](../evaluation/v0.3.6/) and the git history of this file.

| Change | v0.3.6 | v0.3.7 |
|---|---|---|
| A street's sidewalk tags. OSM tags a street `sidewalk=*` or per side; the pipeline read neither | No street edge carried them | 344,718 of 950,416 street edges carry `ext:sidewalk` (`separate` 307,240; `both` 17,460; `no` 14,342; `right` 4,358; `left` 1,300; `yes` 18). 23,136 say the street has a sidewalk and no separate way (`both`, `left`, `right` or `yes`), and the wheelchair profile walks those. In the pinned extract, 104,555 of 108,703 street ways carry no `sidewalk=*` tag, and the per-side form on 26,938 of them mostly says `separate` or `no` ([`tag_counts.json`](../evaluation/v0.3.7/tag_counts.json)) |
| Where a surveyed ramp sits. The nearest vertex within 5 m won, as often a sidewalk vertex beside the crossing as its end | 181,499 ramps attached; 120,625 (66.5%) on a crossing end; 55,347 on no crossing | 184,939 attached (85.0%); 166,310 (89.9%) on a crossing end; 16,159 on no crossing. 48,630 ramps changed node; every survey field on every ramp is unchanged |
| OSM's own kerb nodes | Not carried | 22,238 curb nodes from OSM alone (14,392 lowered, 4,903 generic, 1,610 flush, 1,220 raised, 113 rolled), 13,078 of them with `tactile_paving`, 15,272 on a crossing end. 85,931 surveyed ramps sit on an OSM kerb node and carry OSM's values beside the survey's; 4,383 of them disagree about the kerb, almost all OSM `flush` against the survey's `lowered` |
| Elevators | Not carried; 14 elevator nodes sat on an edge marked unknown | 137 nodes with `ext:osm_highway=elevator`; the 524 edges at them carry no incline and no mark |
| Cycleways and tracks with no `foot` tag | Dropped (627 cycleway and 247 track ways) | Kept unless the cycleway is one-way: 8,792 edges added (3,622 cycleway, 5,170 track; 72.4 km one way), among them the RFK Bridge's island path, which now joins the Bronx span on pedestrian edges. 218 one-way cycleway ways stay out |
| Edges marked `ext:incline_unknown` | 2,068 | 2,042 (the edges at elevators lost their mark) |
| Edges with `incline` | 2,792,346 of 2,797,538 | 2,800,654 of 2,806,326 |
| Pedestrian components | 4,975; largest 611,968 nodes (71.7%) | 4,945; largest 615,127 nodes (71.7%) |

Geometry is the same on 2,797,528 of the 2,797,534 edges the two versions share (the other six, four road edges and two footways, follow one endpoint the near-miss merge now joins differently beside a new edge). Incline is the same on 2,795,142 shared edges; it differs on the 492 edges at elevators, on 1,746 edges within three hops of a new edge (the heights are smoothed over short edges, so a new neighbour moves them), and on 10 edges by at most 0.0047 that no change explains. All 2,201 zones, all widths, surfaces, names and crossing markings are unchanged, and all 217,679 ramps carry the same survey fields.

| Fix | v0.3.5 (internal build) | v0.3.6 |
|---|---|---|
| Terrain "no data". The service writes it as exactly 0.0, and it was read as ground at 0 m | 218 nodes at exactly 0.0 m | 39, all real ground that rounds to 0.0. 197 nodes changed: 182 lost their height and 15 moved by 0.1 to 0.7 m |
| Tunnels. A node all of whose edges are tunnel edges (1,308 nodes) carried the height of the ground above it | 1,300 of them carried a terrain height | None does. The other 8 are on the outline of a Pedestrian Zone, which is open ground, and keep their height, as does the mouth of a tunnel |
| Nodes without `ext:elevation_m`, after both fixes | 50 | 1,532 |
| Seams and other jumps. Edges that are not steps reading 50% or steeper | 1,076 (926 of them on no tagged structure) | 0 |
| Edges marked `ext:incline_unknown` | The property did not exist. 994 edges had no incline and no mark because their grade computed over 100% | 2,068 (1,034 segments in both directions): the 994, and 1,074 that read 50% or more in v0.3.5. 1,008 Footway, 378 Sidewalk, 336 road, 202 Steps, 138 Crossing, 6 Pedestrian Road. 1,728 are on no tagged structure, 258 on bridge edges, 82 on elevated edges |
| Edges with `incline` | 2,793,720 | 2,792,346 |
| Unknown grades in routing | An edge whose grade was dropped had no incline and passed the wheelchair profile as if level | The routing layer writes `incline_unknown` and the profile refuses a marked edge. Zone edges follow the same rule; 410 of the 96,650 zone edges are marked |
| GraphML `length_m` | On the 96,650 zone edges only, 3% of the directed file's 2,894,188 edges | On every edge of both GraphML files ([`graphml_lengths.json`](../evaluation/v0.3.6/graphml_lengths.json)) |

Incline differs on 1,422 edges in all: the 1,074 newly marked, and 348 beside the 197 no-data nodes (300 lost their incline and 48 changed slightly through the smoothing). It is identical on the other 2,796,116 edges. No geometry, curb ramp field, width or other property differs on any of the 3,986,649 features, all of which are in both versions with the same `_id`. Deck heights (`ext:elevation_source`) are unchanged except on two nodes: one inside a tunnel now has none, and one on a shoreline now takes the terrain height.

Of the 926 edges that read 50% or more off any tagged structure in v0.3.5, only 318 join a deck height to a terrain height, and 590 have terrain heights at both ends, so the rule keys on the grade and not on where OSM tags a structure. The node heights beside a marked edge may themselves be wrong (terrain under a bridge OSM does not tag). That is a known limit and is not fixed.

An edge with no incline and no mark (a tunnel edge, a structure edge with no deck height, an edge at a node with no height) was not measured. The wheelchair profile passes it, as before, because refusing those would cut every underpass on no evidence.

## Schema validation

| Check | Result |
|---|---|
| Input | `nyc-osw-osw-split.zip` (`nyc.nodes.geojson`, `nyc.edges.geojson` and `nyc.zones.geojson`), SHA-256 `139fd5b05c985727e028a9a9b06a70c1cd903b4d88e516a7aa3a21b4106db584` |
| `python-osw-validation` 0.5.0 | `is_valid: True, errors: 0` ([`evaluation/v0.3.6/validator.json`](../evaluation/v0.3.6/validator.json)) |
| Coordinates over 7 decimal places | 0 |
| Edge ends that differ from their node's coordinate | 0 |
| Edges with unresolved `_u_id`/`_v_id`, self-loops, zero-length edges, duplicate IDs | 0 of each |
| Zones with a `_w_id` that names no node, or a ring vertex off its node | 0 of each |
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` |
| Root metadata | `dataSource` (with licence, attribution and OSM extract), `dataTimestamp` (the OSM data time, 2026-10-01T20:22:06Z), `pipelineVersion` (name, `0.3.6+nyc.1`, URL, git SHA `3383c6a`, `builtAt`), `region` |

## Feature composition

| OSW type | Count |
|---|---|
| Sidewalk edges | 933,110 |
| Crossing edges | 437,510 |
| Footway edges | 457,924 (45,600 from OSM paths and 35,548 from cycleways and tracks, each marked in `ext:osm_highway`; 8,792 of those carry no `foot` tag and are kept under OSM's United States default) |
| Pedestrian Road edges | 11,402 (linear pedestrian streets) |
| Steps edges | 15,470 |
| Motor vehicle road edges | 950,416 (residential 418,886; service 289,376; secondary 89,644; tertiary 71,658; primary 58,632; unclassified 21,042; living_street 1,178), 344,718 of them with `ext:sidewalk` |
| Pedestrian Zones | 2,201 (1,936 pedestrian areas, 262 footway areas, 3 path areas), with 41,826 ring vertices; median 12, largest 256. Manhattan 1,460, Brooklyn 398, Queens 160, Bronx 122, Staten Island 61 |
| Curb nodes | 217,679 surveyed ramps, and 22,238 from OSM's own `kerb` tags |
| Elevator nodes | 137 (`ext:osm_highway=elevator`) |
| Bare nodes | 947,008 |

Every edge is directed and every segment has its reverse. Edges marked `ext:structure`: bridge 14,672, elevated 3,718, tunnel 2,776; 44 zones carry `ext:structure`.

## Graph integrity

The pedestrian graph is the Sidewalk, Crossing, Footway, Pedestrian Road and Steps edges, with each zone's outline counted as a connection between its consecutive ring nodes.

| Metric | Value |
|---|---|
| Pedestrian-graph nodes | 857,570 |
| Pedestrian-graph directed edges | 1,855,910, plus 41,826 zone outline segments |
| Connected components | 4,945, 1,101 of them with 10 or more nodes (4,975 and 1,110 in v0.3.6) |
| Largest component | 615,127 nodes, 71.7% |
| Second largest | 136,855 nodes, Staten Island |
| Segments stored in one direction only (a project choice, not a schema rule) | 0 |

Share of each borough's pedestrian nodes in the largest component (v0.3.6 gave 96.3%, 84.2%, 89.6%, 86.2% and 0%):

| Brooklyn | Queens | Manhattan | Bronx | Staten Island |
|---|---|---|---|---|
| 96.5% | 84.2% | 89.6% | 86.2% | 0% (a separate component) |

Much of the rest lies along streets with no sidewalk mapped in OSM in any form. Earlier versions of this report blamed streets whose sidewalks OSM maps as `sidewalk=*` tags; a count of the pinned extract shows that 104,555 of its 108,703 street ways carry no such tag, and the tags are now read. This graph also has no ferry edges. OpenRouteService's walking profile on this graph routes 82.1% of Manhattan pairs, against 100% on plain OSM (measured on v0.3.3). Of the 358 pairs it loses, 349 have one end on Governors Island, Liberty Island or Ellis Island, which plain OSM reaches by ferry. With the wheelchair profile and no limits, 348 of the 357 lost routes used a ferry on plain OSM, so the pipeline's tag filter, which drops ways such as cycleways with no `foot` tag, accounts for at most 9 ([`evaluation/compare/results/ferries.json`](../evaluation/compare/results/ferries.json)).

### Borough joins, bridge by bridge

The bridge check snaps each end of 23 bridges with a pedestrian path to the nearest pedestrian node in the right borough and compares the walking distance with the straight line ([`evaluation/v0.3.7/bridges.json`](../evaluation/v0.3.7/bridges.json)). With the points on the walkways' own landings, 21 of 23 are joined on pedestrian edges in v0.3.7, against 20 in v0.3.3, v0.3.5 and v0.3.6 ([`evaluation/v0.3.6/bridges.json`](../evaluation/v0.3.6/bridges.json)): the RFK Bridge's Bronx span joins through the island path, a cycleway with no `foot` tag that v0.3.7 keeps, at 1,201 m for 597 m straight. The two that are not:

| Bridge | Pedestrian path | With street edges | Cause ([`evaluation/bridges/FINDINGS.md`](../evaluation/bridges/FINDINGS.md)) |
|---|---|---|---|
| 145th Street Bridge | 2,048 m for 545 m | 785 m | Modelling: OSM maps the sidewalk at the Bronx end on East 149th Street as `sidewalk=right` on the street. The tag is now carried on the street edge, but a street edge is not a pedestrian edge for this test (the ramp survey has 16 ramps there) |
| Broadway Bridge | 1,282 m for 232 m | 579 m | The graph has no crossing of 9th Avenue at Broadway, and two sidewalk ways at West 225th Street end 12 m apart. Whether a crossing exists there has not been checked. |

Nothing has been posted to OSM, and no OSM edit would be made from these findings automatically.

### Nodes on no edge

32,740 nodes are not an endpoint of any edge or a vertex of any zone, all of them surveyed curb ramps (see below). 32,563 nodes are on a zone outline and on no edge.

## Curb ramps

All 217,679 ramps of the NYC DOT survey are in the file, one node each. 184,939 (85.0%) are an endpoint of a pedestrian edge or a zone vertex (107 of them on a zone outline only); 168,780 are on a crossing and 166,310 (89.9% of the attached) on a crossing's end, where the schema puts a curb. From v0.3.7 a ramp goes to a crossing end within 5 m when there is one; in v0.3.6 the nearest vertex won and 55,347 of 181,499 attached ramps (30%) sat on a sidewalk vertex beside their crossing. 16,159 attached ramps are still on no crossing, because no crossing end lies within 5 m, and the routing layer's 5 m rule stays for them. Every one carries `ext:source=nyc_dot_ramps`, and `ext:dws_condition` agrees with the survey on all 217,679. `tactile_paving` agrees with the survey's `DWS_CONDITIONS` on all 217,679. No node carries a sentinel slope.

OSM's own kerb nodes are carried from v0.3.7: 22,238 nodes with no surveyed ramp (14,392 lowered, 4,903 generic, 1,610 flush, 1,220 raised, 113 rolled), with `ext:source=osm_walk`. 85,931 surveyed ramps sit on an OSM kerb node and keep OSM's values in `ext:osm_kerb` and `ext:osm_tactile_paving`; 4,383 disagree about the kerb (OSM `flush` on 4,245, `raised` on 114, `rolled` on 24, against the survey's `lowered`), and the file does not say which is right.

### The ramp to crossing rule

The routing layer counts a crossing as having ramps when a surveyed ramp lies within 5 m of each of its ends. That rule was checked remotely ([`evaluation/crossing_rule/RESULT.md`](../evaluation/crossing_rule/RESULT.md), `score.json`): 200 crossings drawn at random, 40 per borough, each end rated over the city's 2018 orthoimagery (the main survey year) by language-model agents following a written protocol, and every third sheet rated again by another agent that saw nothing of the first ratings. No person rated the sheets and no crossing was visited. Agreement 96%, kappa 0.81 (between model instances, so a measure of consistency, not accuracy); on the ends both called yes or no, 100%. Of the 166 crossings the 5 m rule calls ramped, 164 were rated as having a surveyed ramp positioned to serve them at both ends (precision 0.988), and it misses none of the 164. The false pass rate of 1.2% (2 of 166) has an exact 95% interval of 0.15% to 4.3%. The strict rule (a ramp on the crossing's own node) finds 56%. A 3 m survey rule passes no unramped crossing but misses 5; with 2 errors against 0, the sample cannot tell these rules apart, and the 5 m rule was fixed before the check. The rating says where a surveyed ramp sits relative to the real crosswalk; the imagery showed a ramp directly at 9 of 400 ends, so the ramp's existence rests on DOT's survey, which a contractor (Cyclomedia) collected from vehicle-mounted imagery and LiDAR.

### NYC DOT curb-ramp slopes

The survey records running and cross slopes in percent: 212,194 ramps carry a running slope and 212,111 a cross slope. DOT says the measurements are not indicative of whether a ramp is compliant, and this project does not compare them with design limits.

## Elevation and incline

| | Value |
|---|---|
| Nodes with `ext:elevation_m` | 1,185,524 of 1,187,062. The 1,538 without are tunnel nodes, nodes where the terrain model has no data, and structure nodes whose deck could not be read |
| Nodes at exactly 0.0 m | 39, real ground that rounds to 0.0 (218 in v0.3.5) |
| Highest node by borough | Staten Island 122.4 m, Bronx 84.9 m, Manhattan 80.5 m, Queens 79.4 m, Brooklyn 62.6 m (nodes inside each borough's polygon; the same as in v0.3.5) |
| Nodes with a deck height (`ext:elevation_source`) | 14,465: 14,005 from the 2017 survey, 45 from the 2014 survey, 415 interpolated (14,400 in v0.3.6; the cycleway edges on bridges that v0.3.7 keeps bring 65 more nodes into the deck region) |
| Structure nodes with no height | 48, as in v0.3.5 |
| Edges with `incline` | 2,800,654 of 2,806,326 (99.8%; 2,792,346 in v0.3.6). The 5,672 without: 2,776 tunnel edges, 2,042 marked unknown, 492 at an elevator, 362 with an end that has no height. Zones carry no incline |
| Edges marked `ext:incline_unknown` | 2,042 (2,068 in v0.3.6; the edges at elevators lost their mark) |
| Edges that are not steps at 50% or steeper | 0 |
| Bridge edges with incline | bridge 97.7%, elevated 97.6%; tunnel 0% |
| Sidewalk edges steeper than 5% | 4.3%, as in v0.3.5 |
| Sidewalk edges outside the wheelchair limits (up 8.3%, down 10%) | 0.8%, as in v0.3.5 |
| Sidewalk, Crossing, Footway and Pedestrian Road edges outside the limits, by length | 1.2% under 2 m, 1.1% at 2 to 5 m, 2.3% at 5 to 10 m, 3.1% at 10 to 20 m, 1.9% at 20 to 50 m, 0.5% over 50 m (in v0.3.5: 1.3%, 1.2%, 2.3%, 3.2%, 1.9%, 0.5%) |

Read incline as an estimate from an airborne survey, not a measurement of the path.

1. **The terrain model is bare earth.** On a bridge, a deck or a pier it holds the ground or water below. v0.3.3 reads the classified LiDAR point clouds instead (the 2017 city survey, and the 2014 USGS survey where the 2017 one has no returns, which is the main spans over open water), around every edge OSM tags as a bridge or as `layer` above 0 and outward along the path until the deck meets the ground. The method is in `METHODOLOGY.md`, section 5b.
2. **Deck heights against a survey the fix did not use (measured on v0.3.3).** Of the 13,943 nodes whose height came from the 2017 survey, 13,930 have a surface in the 2014 survey to compare with (13 have none). Among those, the 2014 surface is within 0.25 m at 83% and within 1 m at 92%; the median difference is 5 cm ([`evaluation/structure_incline/structure_validate_2014.json`](../evaluation/structure_incline/structure_validate_2014.json)). The large disagreements are mostly places rebuilt between the two flights: Hudson Yards, Empire Outlets, the Bayonne Bridge, LaGuardia. Of the nodes lifted 2 m or more above the terrain model, 74% lie within 5 m of a transport structure polygon of the city's planimetric database, which comes from photogrammetry and knows nothing of OSM tags or LiDAR (78% for OSM-tagged structures, 54% for untagged approaches; boardwalks, piers and plazas are not in that database).
3. **The remaining steep edges on structures.** 1,592 edges of 3 m or more touching a deck node have end heights that give more than 15% (1,582 in v0.3.6; the ten more touch the new cycleway edges on bridges); the ones at 50% or more carry no incline and the `ext:incline_unknown` mark. They cluster at station entrances, at airport terminals, and at the foot of ramps, where this pipeline's height estimate jumps by several metres across one short edge. From v0.3.7 an edge at an elevator node carries no incline and no mark, so a station entrance OSM maps with an elevator is no longer refused.
4. **Short edges.** The graph keeps every OSM vertex as a node, so half its edges are shorter than 7.4 m. Before incline is taken, each node's height is averaged with its neighbours' along the path over edges shorter than 5 m (never across a step of 0.5 m or more, and not on steps or tunnel edges).
5. **Tunnels and open water.** A tunnel edge has no incline, and from v0.3.6 a node all of whose edges are tunnel edges has no height. A node where the terrain model has no data has no height either (203 nodes; another 17 on a shoreline take theirs from the pixels that hold data), and its edges no incline. None of these edges is marked `ext:incline_unknown`: nothing measured them.
6. **What no airborne survey can see.** A curb ramp a meter long, a step, a cross slope. The limits in the wheelchair profile are applied to estimates with about 0.1 m of noise per node.

## Sidewalk width

`width` is on 818,686 sidewalk edges (87.7%); the median is 3.12 m (Staten Island 2.57, Queens 3.07, Brooklyn 3.44, Bronx 3.62, Manhattan 4.26). Against 5,805 transects cut across the polygon at the edge's midpoint, the median ratio of `width` to transect is 0.94 and the median absolute difference 0.58 m.

## Planimetric gap-fill sidewalks

Not in the graph. The 2,348 directed edges (1,174 segments) derived from planimetric polygons with no OSM sidewalk within 10 m ship as `nyc-gapfill-sidewalks.geojson`, whose root states a sample result (9 of 18 on a sidewalk or walkway, 4 plainly wrong) and that they are not part of the graph.

## Attribute coverage

| Attribute | Coverage | Notes |
|---|---|---|
| `surface` | 1,200,310 edges (42.9%) | asphalt 749,394; concrete 344,360; paving_stones 46,982 |
| `crossing:markings` | 400,006 of 437,510 Crossing edges (91.4%) | `zebra` 227,508; `yes` 64,992; `no` 57,616; `ladder` 44,206. Read from OSM's `crossing:markings` tag first, then from `crossing=*` as the schema advises (see SCHEMA.md). |
| `foot` | 75,314 edges | OSM's tag where it is one of the schema's values; 12,662 on roads |
| `ext:sidewalk` | 344,718 street edges | `separate` 307,240; `both` 17,460; `no` 14,342; `right` 4,358; `left` 1,300; `yes` 18. New in v0.3.7; `ext:sidewalk_left` and `ext:sidewalk_right` carry the raw per-side values |
| `ext:osm_highway` | 81,164 edges, 265 zones and 137 nodes | `path` 45,616, `cycleway` 29,790, `track` 5,758 on edges; `elevator` on nodes |
| `kerb` | 217,679 surveyed ramps and 22,238 OSM kerb nodes | `lowered` on every surveyed ramp (`ufzp-rrqu` has no ramp type column; DOT's cut-through ramps may be flush curbs in schema terms); 14,392 lowered, 4,903 generic, 1,610 flush, 1,220 raised, 113 rolled from OSM |
| `ext:osm_kerb`, `ext:osm_tactile_paving` | 85,931 surveyed ramps | OSM's kerb and tactile values on the vertex the ramp shares. New in v0.3.7 |
| `ext:structure` | 21,166 edges and 44 zones | bridge, elevated, tunnel |
| `ext:elevation_source` | 14,465 nodes | `lidar_2017`, `lidar_2014`, `interpolated` (14,400 in v0.3.6; the new edges on bridges bring 65 more nodes into the deck region) |
| `ext:incline_unknown` | 2,042 edges | `yes`; new in v0.3.6 |
| `ext:source`, `ext:pipeline_version` | every feature | `0.3.7+nyc.1`; `ext:source` is `nyc_dot_ramps` on every surveyed ramp and `osm_walk` on an OSM kerb node |
| `ext:source_timestamp` | every edge and zone; 217,679 nodes | Bare nodes and OSM kerb nodes lack it |
| `ext:osm_id` | every OSM-derived edge | |
| `ext:borough` | every feature | |

## Routing

The figures in this section were measured on v0.3.3. On v0.3.7 the pairs drawn on v0.3.4 were routed again: the wheelchair profile routes 1,760 pairs in Brooklyn, 1,311 pairs in Queens, 1,097 pairs in Manhattan, 933 pairs in Bronx, 885 pairs in Staten Island (88.0%, 65.6%, 54.9%, 46.7%, 44.3%), against 1,694, 1,258, 1,056, 872, 861 in v0.3.6, and 848 of the 2,000 pairs with ends anywhere in the city against 803. It lost 2 pairs (both with ends anywhere in the city, each at a crossing whose surveyed ramp moved onto the end of the crossing it serves and out of the 5 m reach of this one) and gained 292: streets tagged as having a sidewalk, OSM's lowered and flush kerbs at crossing ends, elevators, and the cycleways and tracks kept under the United States default. No other profile lost a pair, and the walking profile gained 10 across the five boroughs on the new cycleway and track edges ([`evaluation/v0.3.7/reach_same_pairs_v0.3.7.json`](../evaluation/v0.3.7/reach_same_pairs_v0.3.7.json)). The v0.3.6 run on the same pairs ([`evaluation/v0.3.6/reach_same_pairs_v0.3.6.json`](../evaluation/v0.3.6/reach_same_pairs_v0.3.6.json)) gave 1,694, 1,258, 1,056, 872, 861 and 803. On the internal build v0.3.5 the same node pairs were routed: no pair lost its route under any profile, and the wheelchair profile gained 8 pairs in Manhattan, 5 in the Bronx and 2 of the pairs with ends anywhere, where a route crosses a plaza ([`evaluation/v0.3.5/reach_same_pairs_v0.3.5.json`](../evaluation/v0.3.5/reach_same_pairs_v0.3.5.json)). On v0.3.6 the wheelchair profile finds no route between the two ends of the High Line: the route below left the deck over an edge that had no incline and is now marked. On v0.3.5 the landmark routes below are the same to within 20 m, except that the wheelchair route beside the High Line is 1,780 m, not 1,835 m ([`evaluation/v0.3.5/reach.json`](../evaluation/v0.3.5/reach.json)). A zone edge under 5 m carries no incline unless its ends differ by more than 0.5 m, so part of these gains may come from short plaza edges whose small height differences are no longer read as a grade.

The check uses the repository's own Unweaver inputs: `scripts/osw_to_unweaver.py` makes the layer Unweaver would read, `unweaver-project/cost-wheelchair.py` decides edge by edge, and only the graph search is re-implemented (results in [`evaluation/reachability/reach.json`](../evaluation/reachability/reach.json)). For the router comparison Unweaver itself was later run in a container: it agrees with this search on whether a route exists for all 1,354 requests tried and on length to within 7 m, when built with `--changes-sign incline`.

The wheelchair profile, adapted from Unweaver's example wheelchair profile (Nick Bolten, Apache-2.0), refuses steps and street centrelines, refuses a crossing unless a lowered or flush kerb lies within 5 m of each of its ends, and refuses an edge steeper than 8.3% up or 10% down. From v0.3.6 it also refuses an edge marked `ext:incline_unknown`. From v0.3.7 it walks a street centreline that OSM tags as having a sidewalk (`ext:sidewalk` `both`, `left`, `right` or `yes`), with no kerb check at that street's intersections, and a kerb from OSM counts at a crossing end as a surveyed ramp does.

Share of 2,000 seeded random origin and destination pairs with a route, both ends in the same borough, each end at the graph node it was drawn at (node snapping; 95% intervals about 2 points either way). The router comparison uses the same pairs with each end snapped to the nearest edge, which gives higher figures (91.2%, 67.8%, 64.0%, 50.8% and 50.3% for Brooklyn, Queens, Manhattan, the Bronx and Staten Island); [`evaluation/README.md`](../evaluation/README.md#two-sets-of-reachability-numbers), under "Two sets of reachability numbers", explains the difference.

| Profile | Brooklyn | Queens | Manhattan | Bronx | Staten Island | Ends anywhere |
|---|---|---|---|---|---|---|
| Every edge, street centrelines included | 98.0% | 93.7% | 79.7% | 93.1% | 93.8% | 61.6% |
| Pedestrian edges only | 94.0% | 73.3% | 79.3% | 74.2% | 66.3% | 52.4% |
| Pedestrian edges without steps | 93.5% | 72.7% | 75.1% | 73.6% | 65.3% | 51.3% |
| Wheelchair profile without its incline limits | 87.8% | 67.0% | 68.1% | 58.6% | 52.3% | 45.5% |
| **Wheelchair profile** | **84.8%** | **63.0%** | **53.0%** | **43.7%** | **43.1%** | **40.5%** |

Taking the constraints away in this order (street centrelines, steps, ramps, incline), excluding street centrelines costs the most in the Bronx, Queens and Staten Island, where many streets have no sidewalk mapped in OSM in any form (v0.3.3's report said the `sidewalk=*` tags were the cause; the count in [`tag_counts.json`](../evaluation/v0.3.7/tag_counts.json) says otherwise). In Brooklyn crossings with no surveyed ramp within reach cost the most, and in Manhattan incline does. The incline limits cost 3 points in Brooklyn and 15 in the Bronx and Manhattan. A different order would split the loss differently.

### Landmark routes

Seventeen landmark pairs (`scripts/route_test.py`), nine of them meant to cross a bridge, a viaduct or an elevated walkway. All eight others have a wheelchair route with no steps and no street centreline. Of the structure routes:

| Route | Walker | Wheelchair profile | Why |
|---|---|---|---|
| Brooklyn Bridge (Manhattan end) to DUMBO | 2,258 m | 3,769 m, the step-free way along the promenade | |
| High Line, Gansevoort to 30th Street | 1,689 m on the High Line | 1,835 m on the Tenth Avenue sidewalks | High Line entrances read as near-vertical footways because this pipeline did not carry OSM node tags. At 30th Street, OSM maps the lift as `highway=elevator`, `wheelchair=yes` on node 2823833584. From v0.3.7 the elevator is carried and the edges at it have no incline: the profile finds a 1,706 m route between the two ends, on the Tenth Avenue sidewalks (68% sidewalk, 17% footway), where v0.3.6 found none; it still does not use the deck |
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

# 1. Build (city-wide: about 52 minutes; peak memory 35 GB, which ran with
#    swap on a 32 GB machine, 14 GB under data/ including 2.2 GB of LiDAR tiles
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

The v0.3.7 build ran from an empty `data/` at commit `070f35a` (the pinned OSM extract was placed in it first and checked against its SHA-256), failed in Stage 4's node merge, and was resumed with `--stage 4` on the same staged data after the fix, at commit `c442658` ([`evaluation/v0.3.7/build.json`](../evaluation/v0.3.7/build.json) records both runs). The tests (`tests/`) pass.

## Open work

- A jump in this pipeline's height estimate across one short edge at the foot of a ramp blocks three of the structure routes (Williamsburg, Queensboro, Pulaski). From v0.3.6 an edge whose grade comes out at 0.5 or more has no incline and is marked `ext:incline_unknown`, and the wheelchair profile refuses a marked edge, so the mark does not open these routes. The fix is still in the height method: the edge needs a height on the ramp's own surface at both ends. OSM is not wrong there.
- The node heights beside an edge marked `ext:incline_unknown` may themselves be wrong (terrain under a bridge OSM does not tag). Nothing corrects them yet.
- An edge with no incline and no mark (a tunnel edge, a structure edge with no deck height, an edge at a node with no height) passes the wheelchair profile unmeasured.
- Structures built after May 2017 carry whatever height the 2017 survey saw there.
- The ramp survey is mostly from 2018. DOT's per-corner progress data could mark ramps whose corner was rebuilt since.
- The largest single limit on routing is that much of the Bronx, Queens and Staten Island has streets with no sidewalk mapped in OSM in any form. The `sidewalk=*` tags are now read, untagged cycleways and tracks are kept under OSM's United States default, and OSM's kerb and elevator nodes are carried; none of these closes that gap. The 218 one-way cycleways left out have not been checked on a sample. A city-wide list of near-miss endpoints would be shared with the NYC OSM community for review, and no edit would be made from it automatically.
- Where a ramp moved onto a crossing end, a neighbouring crossing that had counted it within 5 m can lose it: the routing layer calls 387,802 of 437,940 crossing edges ramped against 387,442 of 437,510 in v0.3.6, and four of the 12,000 sampled pairs lost their wheelchair route that way while many more gained one.
