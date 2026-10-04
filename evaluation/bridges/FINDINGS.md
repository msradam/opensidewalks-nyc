# Five bridges not joined on pedestrian edges in v0.3.2

The v0.3.2 bridge test (`research_notes/release/city/bridges.py`) listed five crossings
as not joined end to end on pedestrian edges. This note gives the cause of each one.
All numbers come from `diagnose.py` and are in `findings.json`.

Note 2026-10-03. This diagnosis was written by an Anthropic Claude model instance run through Claude Code, working from the pinned extract and the v0.3.2 build. Where it says "I", that instance is writing. The exact model version was not recorded. No person checked any of it on the ground or in imagery. The author reviewed it. It describes v0.3.2. On 2026-10-03 the verdicts below were corrected where the first version blamed OpenStreetMap for this project's own modelling choices. This graph does not read `sidewalk=*` tags on street centrelines, and its wheelchair profile does not route along centrelines. Stage 3 drops cycleways that have no `foot` tag, which is stricter than the OSM default for the United States (`foot=yes`). A mapper note that proposed OSM edits from this diagnosis was removed from the repository. No OSM edit was made or proposed upstream.

| Bridge | Verdict | Cause in one line |
|---|---|---|
| 145th Street Bridge | Modelling | OSM maps the Bronx-side sidewalks of East 149th Street as `sidewalk=right` on the street, which this graph does not read. No separate sidewalk way leaves the intersection. |
| Broadway Bridge | OSM geometry, unconfirmed | No crossing way across 9th Avenue at Broadway, and a sidewalk corner at West 225th Street that is 12 m from the next sidewalk and not joined to it. Whether a crosswalk exists at 9th Avenue was not checked. The bridge sidewalks are joined. |
| Randall's Island Connector | None of the three. No defect. | The Connector is joined. The test's landfalls for this row are under the RFK Bronx span. |
| RFK Triborough Bridge, Bronx span | Pipeline | Stage 3 drops way 1414563386, an island cycleway with no `foot` tag, although the United States default for `highway=cycleway` is `foot=yes`. It is the only link from both ramps to the island. |
| Henry Hudson Bridge | None of the three. No defect. | The walkway is joined at both ends. The test's landfalls are 230 m off the walkway landings. |

No bridge is "not walkable in reality". No bridge is cut by the Stage 4 endpoint merge.

## Method

`osm_cache.py` reads the pinned extract once and caches every highway way with a node
inside lat 40.790 to 40.890, lon -73.945 to -73.900 (22,310 ways, 70,945 nodes).
`lib.py` classifies each way the way the pipeline does: kept as pedestrian, kept as
street, dropped by Stage 1, or dropped by Stage 3. It builds two undirected graphs with
one weight per node pair: the raw OSM ways that the rules keep as pedestrian, and the
pedestrian edges of the built v0.3.2 graph.

Three checks hold for the whole box. Every OSM way that the rules classify as pedestrian
(14,850 ways) is present as pedestrian edges in the built graph, matched by `ext:osm_id`.
For all five bridges the pedestrian shortest path in the built graph has the same length
as the path on the raw OSM ways the rules keep. So Stage 4 cuts nothing here, and each
cause is either in OSM or in the Stage 1 and Stage 3 rules, or in how this graph models
sidewalks that OSM records as street tags. The release test numbers are
reproduced inside the extract to within 2 m.

| Bridge (test anchors) | Straight line | Built pedestrian path | Built path with street edges | Pedestrian path with the proposed fix |
|---|---|---|---|---|
| 145th Street Bridge | 545 m | 2,476 m | 786 m | 804 m |
| Broadway Bridge | 232 m | 1,284 m | 579 m | 394 m |
| Randall's Island Connector | 446 m | 1,302 m | 1,019 m | 958 m (with the RFK fix) |
| RFK Bronx span | 597 m | 2,486 m | 2,478 m | 1,201 m |
| Henry Hudson Bridge | 535 m | 1,526 m | 1,519 m | no fix needed |

## 1. 145th Street Bridge: sidewalks mapped as street tags

### What OSM has

Both sidewalks are mapped as separate `highway=footway`, `footway=sidewalk`, `foot=yes`
ways. The roadway (ways 5670765, 799505593, 799505594) is `highway=secondary` with
`sidewalk=separate`.

| Side | Manhattan approach | Bridge and swing span | Bronx approach |
|---|---|---|---|
| North | 409222599 | 409222602, 799505589, 799505592 | 409222603 |
| South | 409222604 | 600283264, 799505590, 799505591 | 796690083 |

On the Manhattan side both sidewalks join the grid at nodes 9557088445
(40.8204701, -73.9360175) and 9166288593 (40.8202816, -73.9361492), through sidewalks
1037463244 and 1037463966 and crossing 1037463245.

On the Bronx side the north sidewalk ends at node 9166288599 (40.8194447, -73.9305506)
and the south sidewalk at node 9166288597 (40.8191739, -73.9303667). From there the only
pedestrian ways are crossings and traffic islands inside the intersection of Exterior
Street, East 149th Street and River Avenue: crossings 1443528565, 1443528570, 1443528569,
1244669272, 1244669273 and traffic islands 1244669274, 1443528567, 1443528568. A walk on
pedestrian ways from either sidewalk end reaches 36 nodes within 250 m and then stops.
The cluster ends at four places.

| Node | Lat, lon | What stops there |
|---|---|---|
| 9903127443 | 40.8194383, -73.9299701 | North end of crossing 1244669273. The node is on slip road 552614410 (`secondary_link`). No sidewalk continues on the north side of East 149th Street or the east side of River Avenue. |
| 11570004635 | 40.8190980, -73.9300820 | South-east corner, where crossings 1244669272 and 1244669273 meet. No sidewalk continues on the south side of East 149th Street. |
| 13246259572 | 40.8195078, -73.9301556 | Traffic island 1443528567 ends on slip road 1430145125. |
| 13246259574 | 40.8195455, -73.9302862 | Traffic island 1443528568 ends on slip road 1430145125. |

The nearest Bronx sidewalk ways are at Gerard Avenue, 64 to 65 m east (node 13329984702
at 40.8191991, -73.9292627 on way 1453525595, and node 13329984704 at
40.8189763, -73.9293457 on ways 1419059059 and 1453525601), and at East 150th Street,
83 to 86 m north (node 13337724672 at 40.8201829, -73.9296940 on sidewalk 1454347058,
east side of River Avenue, and node 13337724673 at 40.8202171, -73.9298525 on sidewalk
1454347072, west side). East 149th Street carries `sidewalk=right` on ways 5699290 and 1082179001,
so OSM records that the sidewalks exist. OSM has two documented ways to map a sidewalk:
as a separate way, or as a `sidewalk=*` tag on the street. Here it uses the second, and
this graph reads only the first.

The NYC DOT ramp survey has 16 kerb ramps within 70 m of the intersection, among them
ramp 201524 at 40.819473, -73.92995 (4 m from node 9903127443) and ramps 201530 and
201526 at the south-east corner. The survey's ramps there are consistent with the
sidewalks that the street tags record. Nobody checked them on the ground.

### What the built graph has

All ten sidewalk ways and all the crossings are pedestrian edges. Deck end to deck end
(node 7732264955 to node 4111221251) the path is 250 m for a 246 m straight line. From
the Bronx sidewalk ends to the Bronx anchor (node 13337724674, 27 m from the test
landfall) the pedestrian path is 2,755 to 2,862 m: back over the bridge and across the
Harlem River again further south. The path with street edges (786 m) leaves the south
sidewalk at node 9166288597 and runs on the centrelines of Exterior Street, East 149th
Street and River Avenue (ways 46625547, 1443528561, 1477007367, 992098822, 46595126).

Two side notes. The slip roads 552614410, 1430145125 and 1477007366 are
`highway=secondary_link`. They pass the Stage 1 filter, because the regex `secondary` is
unanchored, and Stage 3 drops them, because `secondary_link` is not in `STREET_TYPES`.
The one-way cycleways 1443528563 and 1443528564 have no `foot` tag and Stage 3 drops
them. Neither is a pedestrian way and neither would close the gap.

### Fix

The path exists along the street (786 m with street edges, against 2,476 m on
pedestrian edges alone). This project's own rules cut it: the graph does not read
`sidewalk=*` on the street, and the wheelchair profile refuses street centrelines. The fix
belongs in the pipeline, for example by deriving sidewalk edges from `sidewalk=*` tags.
With three sidewalk links added at the Bronx end the test path is 804 m, ratio 1.48. (The
first version of this section said no pipeline rule was involved and proposed that a
mapper draw the sidewalks. That was corrected on 2026-10-03.)

## 2. Broadway Bridge: OSM geometry, unconfirmed

### What OSM has

Both sidewalks are mapped and both are joined to sidewalks on both banks.

| Side | South approach | Bridge and lift span | North approach |
|---|---|---|---|
| West ("Broadway Sidewalk (west side)") | 833386640 | 833386642, 982319752, 982319751 | 833386641 |
| East | 1421992530 | 1421992527, 1421992529, 1421992528 | 1016658473 |

The roadway ways (126140593, 799505579, 799505574 and others) are `highway=primary` with
`sidewalk:right=separate`. Ways 191841937, 191841938, 191841939, 1016658469 and
1016658472 in the same box are short stairs and footbridges that look like station
access. They are not part of the crossing.

The detour comes from two missing links beside the bridge and one open question.

South bank. The east bridge sidewalk 1421992530 ends at node 9561125969
(40.8729311, -73.9116972), where sidewalk 1038066215 (east side of 9th Avenue) begins.
Sidewalk 1246891608 runs along the east side of Broadway and turns down the west side of
9th Avenue. Its corner node 9561125933 (40.8727630, -73.9118034) is 21 m from node
9561125969, across 9th Avenue. There is no crossing way. The pedestrian distance between
the two nodes is 1,039 m. Ramp 310604 of the DOT survey is on 9 Avenue at
40.872766, -73.911794, 1 m from node 9561125933.

North bank. The west bridge sidewalk 833386641 ends on the West 225th Street roadway
node 7779712806 (40.8743360, -73.9104341), where Broadway west sidewalk 1135504587
continues north. The sidewalk on the north side of West 225th Street, way 1168603228,
ends at node 7578364023 (40.8744626, -73.9105580), at the foot of station steps
1016658468. That node is 12 m west of way 1135504587 and is not joined to it. Way
1168603228 is 698 m long and reaches 1135504587 only at its far end, through 1135504588.
The pedestrian distance from the bridge sidewalk (node 7779712804) to node 7578364023 is
521 m, by way of the first mapped crossing of West 225th Street, 260 m to the west.

Open question. At the south end the two bridge sidewalks (nodes 7779712807 and
13068327320) are 26 m apart across Broadway and 460 m apart on pedestrian ways. The
first mapped crossing of Broadway is crossing 1246388777, 300 m south-west. Whether a
crosswalk exists at 9th Avenue was not determined.

### What the built graph has

All ten sidewalk ways are pedestrian edges. Deck end to deck end the west sidewalk is
146 m for a 146 m straight line, and the east sidewalk 165 m for 164 m. The test's south
anchor (node 9561125937, 40.8725112, -73.9115204) is on sidewalk 1246891608, on the
wrong side of the missing 9th Avenue crossing. Its north anchor is node 7578364023, the
unjoined corner. The 1,284 m path walks 300 m south-west to cross Broadway, back up the
west sidewalk, over the bridge, then 260 m west along West 225th Street and back.

### Fix

No pipeline rule is involved as far as this diagnosis found. With a crossing of 9th
Avenue and the corner at West 225th Street joined, the test path is 394 m, ratio 1.70.
A ramp 1 m from the corner does not show that a crosswalk exists, so the missing crossing
is unconfirmed. Nobody looked at imagery or the ground here, and no change to OSM is
proposed from this note.

## 3. Randall's Island Connector: no defect in the Connector

The Connector is three ways named "Randall's Island Connector", all `highway=cycleway`,
`foot=designated`, `bicycle=yes`: 892827324 (27 m), 892827325 (the bridge over the Bronx
Kill, 27 m) and 892827323 (344 m, under the Hell Gate viaduct). It runs from node
3843180753 (40.7975383, -73.9166611) on Randall's Island to node 42772993
(40.7999189, -73.9131540) on East 132nd Street, then crossing 1189211370 to sidewalk
1189211371 at node 11042720484. All four ways are pedestrian edges in the built graph.
End to end the path is 404 m for a 404 m straight line.

The release test's landfalls for this row are (40.7990, -73.9180) and (40.8025, -73.9160).
Those points are under the RFK Bronx span, 200 m west of the Connector. The south anchor
snapped to node 4159724355 (40.7985485, -73.918568) on footway 1287206021, beside the
RFK ramps. From there the RFK walkway is 100 m away and unreachable (section 4), so the
path goes east to the real Connector and back: 1,302 m. This row measured the RFK defect
a second time. With the RFK fix the same anchors give 958 m.

East 132nd Street (ways 440887346, 465321984) and Cypress Avenue are
`highway=unclassified`, which Stage 1 does not request, so the Bronx end has fewer
street edges than it should. That does not affect pedestrian edges. (Note 2026-10-03:
v0.3.3 requests `unclassified`; this paragraph describes v0.3.2.)

The fix is in the test: move the landfalls to about (40.7975, -73.9167) and
(40.7999, -73.9131).

## 4. RFK Triborough Bridge, Bronx span: pipeline rule

### What OSM has

Two walkways cross the Bronx Kill side by side.

| Walkway | Ways, Randall's Island to the Bronx | Tags |
|---|---|---|
| Shared path (new) | 1287205186, 1287205187, 46627101 (337 m bridge), 1432305134, 1432305133, 46663499, then crossing 1387307964 of Cypress Avenue | `highway=cycleway`, `foot=designated`, `bicycle=designated` |
| Older walkway | 46627099, 46627098, 46627100, 46627097 (338 m bridge), then steps 1559136285 | `highway=path` or `footway`, `bicycle=no` |

The roadway ways (5699313 and others) are `highway=motorway`, `foot=no`.

Bronx side. The shared path reaches crossing 1387307964 at node 9156976064
(40.8022639, -73.9168990) and sidewalk 1232018300 at node 12842351865. The older walkway
comes down steps 1559136285 at node 595800950 (40.8015835, -73.9168234) to footway
46626564 and sidewalk 1559136289. Both are joined to the Bronx grid.

Randall's Island side. Each ramp crosses one island path and then ends on the centreline
of Bronx Shore Road (way 375160033, `highway=unclassified`).

| Node | Lat, lon | What is there |
|---|---|---|
| 11938167518 | 40.7979764, -73.9197503 | Shared path 1287205186 meets way 1414563386. |
| 12715806360 | 40.7978933, -73.9198145 | Shared path 1287205186 ends on Bronx Shore Road. |
| 4186937127 | 40.7979535, -73.9196941 | Older walkway 46627099 meets way 1414563386. |
| 595806636 | 40.7978698, -73.9197578 | Older walkway 46627099 ends on Bronx Shore Road. |

Way 1414563386 is 368 m long, from node 3843180752 (40.7967687, -73.9178023, on the Hell
Gate Greenway Path) to node 12998503219 (40.7988880, -73.9211141). Its tags are
`highway=cycleway`, `bicycle=designated`. It has no `foot` tag. It also meets footway
1287206021 at node 4159724358 and cycleway 359260543 (`foot=yes`) at node 3640160377.
The next two segments of the same path, 1414563387 and 299906173, carry
`foot=designated`. In raw OSM the chain is connected by shared nodes from the deck to
the island paths.

### What the built graph has

Every walkway way above is a pedestrian edge. Way 1414563386 is absent: it passes the
Stage 1 filter and Stage 3 drops it. Way 375160033 is absent: Stage 1 does not request
`unclassified` (in v0.3.2; v0.3.3 requests it). So on the island both ramps are dead ends at the four nodes in the
table, and the deck is reachable from the Bronx only. From the island path junction
(node 3640160377) to the Bronx sidewalk (node 12842351865) the pedestrian path is
1,286 m for a 533 m straight line, by way of the Randall's Island Connector. With way
1414563386 kept it is 633 m, ratio 1.19. The test anchors give 2,486 m now and 1,201 m
with the way kept (ratio 2.01; the south anchor is 118 m from its landfall and 250 m
west of the ramps).

### The rule

Stage 3, `schema_map._classify_osm_edge`: a way with `highway` in `SHARED_TYPES`
(cycleway, track) is a pedestrian edge only when `foot` is in `FOOT_ALLOWED` (yes,
designated, permissive). Any other cycleway returns `None` and is dropped.

The OSM wiki's default access table for the United States gives `foot=yes` for
`highway=cycleway`. Its worldwide table gives `foot=no` and notes that every router on
osm.org walks untagged cycleways. So by the United States default the path is walkable
in OSM and the pipeline drops it. The pipeline's rule is stricter than the United States
default, so the cause is the pipeline. (The first version gave the verdict two parts and
counted the missing `foot` tag as an OSM fault. That was corrected on 2026-10-03: adding a
tag so that this pipeline keeps the way would be tagging for the router.)

The MTA states that the path is open to pedestrians (press release of May 12, 2025):
"The MTA replaced pedestrian-only paths on the RFK Manhattan and Bronx spans, both
connecting to Randalls Island with new bike/pedestrian paths that are fully compliant
with the Americans with Disabilities Act (ADA)."

### Cost of changing the rule

Counts are from the extract, for ways with a node inside lat 40.49 to 40.92,
lon -74.26 to -73.69 (`cycleway_rule_cost.py`). The box takes in parts of Nassau and
Westchester.

| Change | Ways added | Length | What comes with it |
|---|---|---|---|
| A. Keep every cycleway with no `foot` tag | 627 | 37.0 km | 218 are `oneway=yes`, which is how on-street bike lanes are drawn (Tillary Street 11, Broadway 9, Pike Street 9). 28 are named Hudson River Greenway and 20 Ocean Parkway Bike Path. |
| B. Keep an untagged cycleway only if it is two-way, is not a crossing, has both ends on kept pedestrian ways, and has no node on a street | 53 | 6.7 km | Includes 1414563386. 25 of the 53 are named Ocean Parkway Bike Path (18) or Hudson River Greenway (7). |
| C. Allow list with the one way ID 1414563386 | 1 | 0.4 km | Nothing else. |

Ocean Parkway and the Hudson River Greenway are, as far as I know, bike paths that run
beside a separate walkway. I did not check that here. If it is right, rule B is wrong
on about half of what it adds, and rule A on much more.

### Fix

The fix belongs in the pipeline. Rule A is the change that follows the United States default,
and the table above gives what comes with it, including on-street bike lanes. Rule B is
narrower. Whether way 1414563386
should carry `foot=designated` is for a local mapper to judge from signage, and this
project does not rely on it. (The first version preferred an OSM edit and an allow list
of one way ID. That was corrected on 2026-10-03.)

Adding `unclassified` to the Stage 1 filter is a separate change, and v0.3.3 made it.
In this diagnosis it adds 2,052 ways
(259 km) as street edges and no pedestrian edges. It would put Bronx Shore Road, East
132nd Street and Cypress Avenue in the street network. Stage 3 already lists
`unclassified` in `STREET_TYPES`, so the filter is the only thing keeping them out.

## 5. Henry Hudson Bridge: no defect

### What OSM has

The walkway is way 246164306: `highway=cycleway`, `foot=designated`,
`bicycle=designated`, `bridge=yes`, `layer=1` (the lower level). The roadways are
motorways 8119396 (`layer=1`) and 9764584 (`layer=2`).

South end. At node 1760596448 (40.8763656, -73.9248079) the walkway meets path 932513548
(`highway=path`, `bicycle=no`, 156 m), which joins footway 164422598 in Inwood Hill Park
at node 1760604368 (40.8764487, -73.9231731). The walkway continues 33 m to node
12063751144 and park path 164422607.

North end. From node 2532396929 (40.8794561, -73.9197580) the chain is 246164304,
246164303, 1544290735 (a short bridge) and 1544290734, all `highway=cycleway` with
`foot=designated`, to crossing 1347387854 at node 12321483875 (40.8807793, -73.9182113)
on Henry Hudson Parkway West, then sidewalk 1419179978.

The chain is connected by shared nodes at every step.

### What the built graph has

All eight ways are pedestrian edges. Deck end to deck end (node 1760596448 to node
2532396929) the path is 551 m for a 548 m straight line. From the park junction to the
Henry Hudson Parkway West crossing it is 910 m for 639 m, ratio 1.42.

The test's landfalls are (40.8755, -73.9225) and (40.8805, -73.9220), which assume a
bridge running due north. The walkway runs south-west to north-east. Its north landing
is 230 m east of the north landfall and its south landing is inside the park, west of
the south landfall. The 1,526 m test path is 392 m of park paths, the 551 m deck, and
583 m from the north landing back west to the anchor on sidewalk 1107174126. All of it
is on pedestrian edges. The ratio of 2.85 comes from where the anchors are.

### Is it open to walkers

Yes. MTA press release of May 12, 2025: "The MTA widened the lower-level sidewalk and
built ADA-compliant connections at both ends of the Henry Hudson Bridge, resulting in a
fully accessible bike/pedestrian path across the entire bridge, and improving
accessibility between Spuyten Duyvil and Inwood Hill Park at the northern end of
Manhattan. The new walkway, opened in December 2024, was completed on time and within
budget."

### Fix

In the test: move the landfalls to about (40.8764, -73.9232) and (40.8808, -73.9182).

A 12-node island of pedestrian ways sits under the north end (footway 75069699 with
`bridge=yes`, steps 75069691 and 75069692, footway 75069698 to Edsall Avenue). It is
attached to a street and to no other pedestrian way. It is not part of the bridge
walkway. I did not find out what it is.

## Not determined

- Whether marked crosswalks exist across 9th Avenue at Broadway and across Broadway at
  the south end of the Broadway Bridge. I had no imagery. The ramp survey shows one
  ramp at the 9th Avenue corner.
- The exact line of the three missing sidewalks at East 149th Street. The end nodes are
  given; the geometry between them needs imagery or a survey.
- Whether way 1414563386 itself is signed for pedestrians. The MTA sentence covers the
  bridge paths and their connection to the island, and the adjoining segments carry
  `foot=designated`.
- Whether the Ocean Parkway Bike Path and Hudson River Greenway ways that rules A and B
  would admit are closed to walkers.
- Whether a shorter stair or path links the Henry Hudson walkway's north end to
  Palisade Avenue.
- Anything outside the box. A pedestrian path that leaves lat 40.790 to 40.890,
  lon -73.945 to -73.900 is not seen. The five test paths reproduce to within 2 m, so
  none of them leaves it.

## Sources

- MTA, "MTA Celebrates New Bike and Pedestrian Paths on Robert F. Kennedy, Henry Hudson
  and Cross Bay Bridges", May 12, 2025.
  https://www.mta.info/press-release/mta-celebrates-new-bike-and-pedestrian-paths-robert-f-kennedy-henry-hudson-and-cross
  (mta.info returns 403 to scripts. Read from
  http://web.archive.org/web/20260518022516/https://www.mta.info/press-release/mta-celebrates-new-bike-and-pedestrian-paths-robert-f-kennedy-henry-hudson-and-cross)
- OpenStreetMap wiki, "OSM tags for routing/Access restrictions", worldwide and United
  States of America tables. https://wiki.openstreetmap.org/wiki/OSM_tags_for_routing/Access_restrictions
- NYC DOT Pedestrian Ramp Locations, dataset `ufzp-rrqu`, three `within_circle` queries
  (`ramps_near.py`, results in `ramps_near.json`).

## Files

These scripts and their outputs are in the project's working archive and are not published. `findings.json` and `bridges.json` in this folder are the published results.

| File | What it is |
|---|---|
| `osm_cache.py` | One pass over the extract. Writes `osm_cache.json` (ways, tags, nodes in the box) and `citywide_tag_counts.json`. |
| `cycleway_rule_cost.py` | One pass over the extract. Writes `cycleway_candidates.json`, the cycleways and tracks Stage 3 drops city-wide. |
| `ramps_near.py` | Three small queries to the DOT ramp survey. Writes `ramps_near.json`. |
| `lib.py` | Loading, the pipeline's classification rule, graphs, shortest path. `python lib.py` runs its self-check. |
| `explore.py` | `describe`, `reach`, `gaps`, `route`: the helpers used to follow each chain node by node. |
| `diagnose.py` | Computes every number in this note and writes `findings.json`. |

To repeat, with `PYTHONPATH` set to the repository root and the project's
`.venv/bin/python`: `osm_cache.py`, `cycleway_rule_cost.py cycleway_candidates.json`,
`ramps_near.py ramps_near.json`, `diagnose.py`.
