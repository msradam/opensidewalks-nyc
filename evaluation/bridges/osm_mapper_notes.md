# OSM mapper notes: pedestrian gaps at four Harlem River and Bronx Kill crossings

These notes come from a connectivity check of a Geofabrik New York extract dated
2026-10-01. Each item stands alone. IDs and coordinates are from that extract, so check
the current map first. Nothing here was verified on the ground or against imagery. The
evidence for each item is stated. Please confirm before editing.

## 1. Bronx: no sidewalks leave the east end of the 145th Street Bridge

Location: the intersection of Exterior Street, East 149th Street and River Avenue, at
the Bronx end of the 145th Street Bridge, about 40.8193, -73.9301.

What is there. The two bridge sidewalks (ways 409222603 and 796690083) end at nodes
9166288599 (40.8194447, -73.9305506) and 9166288597 (40.8191739, -73.9303667). They
join signal crossings 1443528565, 1443528570, 1443528569, 1244669272, 1244669273 and
traffic islands 1244669274, 1443528567, 1443528568.

What is missing. No sidewalk way leaves this cluster, so a pedestrian router cannot get
from the bridge to any Bronx street. Three sidewalks are not drawn:

| Missing sidewalk | From | To | Length |
|---|---|---|---|
| North side of East 149th Street | node 9903127443 (40.8194383, -73.9299701), the north end of crossing 1244669273 | node 13329984702 (40.8191991, -73.9292627), on sidewalk 1453525595 at Gerard Avenue | 65 m |
| South side of East 149th Street | node 11570004635 (40.8190980, -73.9300820), where crossings 1244669272 and 1244669273 meet | node 13329984704 (40.8189763, -73.9293457), on sidewalks 1419059059 and 1453525601 at Gerard Avenue | 64 m |
| East side of River Avenue | node 9903127443 | node 13337724672 (40.8201829, -73.9296940), on sidewalk 1454347058 and crossing 1454347067 at East 150th Street | 86 m |

Also check three crossing ends. Crossing 1244669273 ends at node 9903127443, which is a
node of slip road 552614410 (`highway=secondary_link`), so the crossing stops in the
carriageway and not at the kerb. Traffic islands 1443528567 and 1443528568 end at nodes
13246259572 (40.8195078, -73.9301556) and 13246259574 (40.8195455, -73.9302862), both
on slip road 1430145125. A sidewalk on the west side of River Avenue from there to node
13337724673 (40.8202171, -73.9298525, on sidewalk 1454347072), 83 m north, is also not
drawn.

How I know. From either bridge sidewalk end, pedestrian ways reach 36 nodes within
250 m and no more. The shortest pedestrian route from the bridge's Bronx end to the
sidewalk at River Avenue and East 150th Street, about 100 m away, is at least 2,755 m
and crosses the Harlem River twice. East 149th Street carries `sidewalk=right` on ways 5699290 and
1082179001. The NYC DOT Pedestrian Ramp Locations survey (dataset `ufzp-rrqu`) has 16
kerb ramps within 70 m, including ramp 201524 at 40.819473, -73.92995 on the north-east
corner and ramps 201530 and 201526 on the south-east corner.

Low priority, same place: footway 1498220979 (384 m, 51 nodes, with footway 1498220980)
touches Exterior Street at node 13720917648 (40.8195367, -73.9304215) and no pedestrian
way. It passes 11 m from traffic island 1443528568.

## 2. Manhattan (Inwood): no crossing of 9th Avenue at Broadway, south of the Broadway Bridge

Location: 9th Avenue at Broadway, about 40.8728, -73.9117.

What is there. The east sidewalk of the Broadway Bridge (ways 1421992527 and 1421992530)
comes off the bridge to node 9561125969 (40.8729311, -73.9116972), where sidewalk
1038066215 runs down the east side of 9th Avenue. On the other side of 9th Avenue,
sidewalk 1246891608 comes up the east side of Broadway and turns the corner at node
9561125933 (40.8727630, -73.9118034).

What is missing. A crossing way (`highway=footway`, `footway=crossing`) over 9th Avenue
between nodes 9561125933 and 9561125969, 21 m apart, sharing a node with the roadway
(way 5671259 or 141125504).

How I know. The pedestrian route between the two nodes is 1,039 m. The DOT ramp survey
has ramp 310604 on 9 Avenue at 40.872766, -73.911794, 1 m from node 9561125933. Check
on the ground whether the crosswalk is marked and signalled.

Question for someone who knows the place: is there a crosswalk over Broadway here? The
south ends of the two bridge sidewalks, node 7779712807 (40.8732943, -73.9117384) on
the west and node 13068327320 (40.8731451, -73.9115065) on the east, are 26 m apart and
460 m apart on pedestrian ways. The first mapped crossing of Broadway is way 1246388777,
300 m to the south-west. If there is none on the ground, nothing needs changing.

## 3. Manhattan (Marble Hill): open sidewalk corner at Broadway and West 225th Street

Location: north-west corner of Broadway and West 225th Street, under the elevated
station, about 40.8744, -73.9105.

What is there. Sidewalk 1168603228 (north side of West 225th Street, 698 m long) ends
at node 7578364023 (40.8744626, -73.9105580), where station steps 1016658468 begin.
Broadway west sidewalk 1135504587 runs from node 10584748985 (40.8746351, -73.9099323)
to node 7779712806 (40.8743360, -73.9104341), which is a node of the West 225th Street
roadway (ways 132500933 and 833386643). The Broadway Bridge west sidewalk 833386641
ends at the same roadway node.

What is missing. A join between node 7578364023 and sidewalk 1135504587, 12 m apart.
Sidewalk 1168603228 meets 1135504587 only at its far end, through way 1135504588 at
node 10584748986, after going round the block.

How I know. The pedestrian route from the bridge sidewalk (node 7779712804) to node
7578364023, 24 m away, is 521 m, by way of the first mapped crossing of West 225th
Street 260 m to the west. The DOT ramp survey has ramps 79304 and 79300 on West 225
Street at 40.874356, -73.910392 and 40.874293, -73.910497.

Also consider: sidewalks 833386641 and 1135504587 meet on the roadway node 7779712806
and so stand in for a crossing of West 225th Street. A `footway=crossing` way there
would say so.

## 4. Randall's Island: way 1414563386 has no foot tag

Location: the path along Bronx Shore Road at the foot of the RFK (Triborough) Bridge
Bronx span ramps, from node 3843180752 (40.7967687, -73.9178023) to node 12998503219
(40.7988880, -73.9211141). 368 m, 17 nodes.

What is there. Way 1414563386 is tagged `highway=cycleway`, `bicycle=designated`. Both
RFK Bronx span walkways join it: the shared path 1287205186 (`foot=designated`) at node
11938167518 (40.7979764, -73.9197503), and the older walkway 46627099 (`foot=yes`) at
node 4186937127 (40.7979535, -73.9196941). It also meets footway 1287206021 at node
4159724358 and cycleway 359260543 (`foot=yes`) at node 3640160377. The next segments of
the same path, ways 1414563387 and 299906173, are tagged `foot=designated`.

What is missing. A `foot` tag. A data user that needs an explicit `foot` value on a
cycleway drops this way, and it is the only pedestrian link between both bridge ramps
and the rest of the island. Past it, each ramp ends on the centreline of Bronx Shore
Road (way 375160033) at nodes 12715806360 and 595806636.

Proposed edit: add `foot=designated`, to match ways 1414563387 and 299906173, if
pedestrians share the path on the ground.

How I know. With the way excluded, the pedestrian route from node 3640160377 on the
island to the Bronx sidewalk at the far end of the bridge (node 12842351865) is 1,286 m
by way of the Randall's Island Connector. With it included the route is 633 m over the
bridge. The MTA wrote on May 12, 2025: "The MTA replaced pedestrian-only paths on the
RFK Manhattan and Bronx spans, both connecting to Randalls Island with new
bike/pedestrian paths that are fully compliant with the Americans with Disabilities Act
(ADA)."
(https://www.mta.info/press-release/mta-celebrates-new-bike-and-pedestrian-paths-robert-f-kennedy-henry-hudson-and-cross)

## No action needed

The Henry Hudson Bridge walkway (way 246164306) and the Randall's Island Connector
(ways 892827324, 892827325, 892827323) are mapped with `foot=designated` and are
connected at both ends.
