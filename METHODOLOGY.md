# Methodology

This document records the data sources, transformations and schema mapping decisions of the OpenSidewalks NYC pipeline, as of v0.3.6-nyc.1. OpenSidewalks NYC is an independent project by Adam Munawar Rahman. It is not made or endorsed by the Taskar Center for Accessible Technology, OpenSidewalks or the Transportation Data Equity Initiative (TDEI), nor by NYC DOT or the City of New York. No wheelchair user or disability organization has reviewed the data or the wheelchair profile. This project has not checked any of the data on the ground. Do not use it to tell anyone that a route is accessible, and do not rely on it, or on an app built from it, to plan a trip. The full [Disclaimer](README.md#disclaimer) is in the README.

---

## Data Sources

### 1. OpenStreetMap (dated Geofabrik extract, graph built with OSMnx)

#### What it is

The OpenStreetMap pedestrian walking network for NYC. Footways, paths, crossings, steps, and street edges where foot travel is permitted. Maintained by the OSM community.

#### Where it came from

One dated regional extract from Geofabrik, named by URL and SHA-256 in `config/sources.yaml`. Every build from v0.3.3 to v0.3.6 read `new-york-261001.osm.pbf` (data timestamp 2026-10-01T20:22:06Z). Geofabrik may remove daily files, so the release itself is the copy to rebuild from. Stage 1 downloads it once, refuses a file whose checksum differs from the pin, filters its ways with pyosmium, and hands the result to [OSMnx](https://github.com/gboeing/osmnx) to build the graph. No Overpass query is made.

#### License

[ODbL 1.0](https://www.openstreetmap.org/copyright). Data must be attributed.

#### Snapshot

The extract's own data timestamp (from the PBF header), its URL and its SHA-256 are written to `data/raw/manifest.json` and to `dataSource.osmExtract` in the root of every output file. From v0.3.6 (first built in v0.3.5, an internal build that was not released) the root `dataTimestamp` is that same OSM data timestamp, because the schema defines the field as how current the data is. The time of the build is `pipelineVersion.builtAt`. To move to newer OSM data, change the URL and the checksum together.

#### Borough seams

The city graph is built once and each borough is cut from it with `truncate_by_edge=True`, so the segments that cross a borough line are kept (both neighbouring boroughs hold them and Stage 4 removes the duplicate).

#### Reach of the extract

It covers New York State. A way that crosses into New Jersey is kept whole, but a study-area box that reaches across the state line gets no New Jersey streets.

#### Why explicit custom_filter, not `network_type='walk'`

OSMnx's `network_type='walk'` applies its own heuristics for what counts as walkable. The pipeline uses an explicit filter instead: it lists the `highway` values it keeps and excludes `foot=no`, `access=no` and `access=private`. Anyone can read the inclusion rule in one line of `config/sources.yaml`.

#### Custom filter used

```
["highway"~"footway|path|pedestrian|steps|residential|service|tertiary|secondary|primary|unclassified|cycleway|track|living_street"]["foot"!~"no"]["access"!~"no|private"]
```

The syntax is Overpass's and so are the semantics: each regex is an unanchored search, and a way with no `foot` or `access` tag passes. There is one departure. In OSM's access rules a tag for one mode is more specific than `access`, so a way that fails only the access clause is kept when it carries `foot=yes`, `designated` or `permissive`. The Queensboro Bridge walkway is tagged `access=no`, `foot=designated`. `highway=unclassified` is in the filter; earlier builds did not request it. The link roads (`primary_link` and so on) match the unanchored regex and are then dropped by Stage 3.

#### How it was transformed

OSM edges are classified into OSW entity types based on their `highway`, `footway` and `area` tag values:
- `highway=footway` + `footway=sidewalk` → Sidewalk Edge
- `highway=footway` + `footway=crossing` → Crossing Edge
- `highway=footway|path` (other) → Footway Edge. `path` has no schema entity, so an OSM path is written as `highway=footway` and keeps `ext:osm_highway=path`. In NYC parks a path is often an unpaved trail, and the extension lets a user tell it from a footway.
- `highway=pedestrian` on a linear way → Pedestrian Road Edge, unless it is tagged `footway=sidewalk` or `footway=crossing`, which makes it a Sidewalk or Crossing. Stage 4's endpoint merge counts it as pedestrian, and the routing layer walks it like a Footway.
- `highway=pedestrian`, `footway` or `path` with `area=yes` on a closed way → Pedestrian Zone, a Polygon (see "Pedestrian areas" below).
- `highway=steps` → Steps Edge, its own entity in the schema
- `highway=cycleway|track` with `foot=yes`, `designated` or `permissive` → the same classes, with `ext:osm_highway` keeping the OSM value. Without one of those foot values the way is dropped.
- `highway=residential|service|...` → the schema's motor vehicle road Edges (Residential Street, Service Road and so on). `service=*` subtags are not kept, so Driveway, Alley and Parking Aisle appear as Service Road.

The graph is built without OSMnx's walk mode, so a way tagged `oneway=yes` arrives in one direction. Stage 3 adds the missing reverse of every edge, street edges included, because OSM's `oneway` binds vehicles and bicycles, not people on foot. It also reverses steps, although `oneway` on `highway=steps` (for example an escalator tagged `conveying`) applies to pedestrians; that is a known limitation. The schema lets consumers infer reverse edges, so a consumer must not add them again.

#### Pedestrian areas

An OSM way tagged `area=yes` is a surface, such as a plaza, and people walk across it in any direction. Up to v0.3.4 the pipeline wrote the outline of each such way as Pedestrian Road or Footway Edges, so a route followed the edge of a plaza and the plaza was counted as a road. From v0.3.6 it writes each one as the schema's Pedestrian Zone (`pipeline/utils/zones.py`). The test is `area=yes` with `highway=pedestrian`, `footway` or `path`, without `footway=sidewalk` or `footway=crossing`, on a way whose edges join into one closed ring that is a valid polygon. A way that fails the ring test stays Edges.

A Pedestrian Zone is a Polygon with `highway=pedestrian` and `_w_id`, the list of the Nodes on its ring in ring order, so that `_w_id[i]` is ring vertex `i`. It keeps the way's `name`, `surface`, `foot` and `ext:osm_id`. A zone made from a footway or path area keeps `ext:osm_highway`. It carries no `width` and no `incline`. v0.3.6 has 2,201 zones. `python-osw-validation` 0.5.0 accepts the entity: a fixture with a zone passed, and a negative control failed.

An edge whose OSM way is a bridge or a tunnel is marked `ext:structure` (`bridge` or `tunnel`; a building passage is not marked), and a way with `layer` above 0 and no bridge tag is marked `elevated`. Stage 4 reads deck heights for bridge and elevated edges from LiDAR returns (section 5b) and writes no incline on tunnel edges.

OSM `surface` tags are mapped to the OSW surface enum (9 canonical values). Non-canonical OSM values are mapped to a canonical one: `tarmac` becomes `asphalt`; `sett`, `cobblestone`, `stone` and `brick` become `paving_stones`; `wood` and `metal` become `paved`. The OSM value is not kept, so cobbles and smooth pavers look alike.

`crossing:markings` comes from OSM's own `crossing:markings` way tag where present. A value in the schema's enum is kept. A variant the schema does not list becomes its base type (`zebra:skewed` becomes `zebra`, `lines:surface` becomes `lines`), and a list of several marking types (`zebra;lines;pictograms`) becomes `yes`. Without a usable tag, the value comes from `crossing=*`, read as the schema advises: `marked` and `zebra` give `yes`, `unmarked` gives `no`, and every other value gives none. `uncontrolled` and `traffic_signals` say nothing about paint, so a crossing tagged only that way carries no `crossing:markings`.

OSM `foot=*` is written as `foot` on any Edge where its value is one of the schema's seven (`yes`, `no`, `designated`, `permissive`, `private`, `use_sidepath`, `destination`). Other values, such as `customers`, are left off. OSM `width` tags of 0 or less are left off.

#### Known issues

OSM records sidewalks in two ways: as separate ways (`footway=sidewalk`) or as `sidewalk=both`, `left`, `right` or `separate` on the street. Both are valid OSM schemes. This pipeline reads only the first. Streets tagged the second way become road Edges with no Sidewalk Edge beside them, and the `sidewalk=*` value is not carried. Stage 3's planimetric gap-fill pass derives candidate centrelines for some of them; they ship as a sidecar file and are not part of the graph.

OSM node tags are not carried. Stage 1 requests way tags only, and Stage 4 keeps only `_id`, `barrier`, `kerb` and `tactile_paving` on nodes, which come from the DOT survey. The 105,016 OSM `kerb` nodes in the extract, OSM `tactile_paving`, `highway=elevator` and `highway=crossing` nodes are therefore absent.

The filter drops a cycleway with no `foot` tag. That is stricter than OSM's United States default, which treats `highway=cycleway` as `foot=yes`.

OSM nodes that belong only to ways Stage 3 drops (a cycleway or track with no `foot` tag) are dropped with them.

---

### 2. NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`)

#### What it is

A point dataset of 217,679 pedestrian curb ramp locations citywide. A contractor (Cyclomedia) collected it for the NYC Department of Transportation from vehicle-mounted street-level imagery and LiDAR; no one measured the ramps on site for this dataset. The capture dates in the `GeoCyclora` field run from March 2017 to January 2020, and 216,220 of the 217,679 ramps (99.3%) were captured in 2018. Records ramp location, geometry (running slope, cross slope, landing dimensions), and condition.

#### Where it came from

NYC Open Data Socrata API (`data.cityofnewyork.us/resource/ufzp-rrqu.json`), paginated in batches of 10,000 rows ordered by `:id`. Stage 1 counts distinct row IDs and compares the total with the dataset's own `count(*)`, and stops if they differ. Without `$order`, Socrata pages can overlap: one Staten Island pull of 23,326 rows held 16,664 distinct ramps.

#### License

NYC Open Data terms of use; no licence is attached (see `LICENSE-DATA.md`).

#### How it was transformed

Each ramp becomes a Curb Ramp Node:
```json
{
  "type": "Feature",
  "geometry": { "type": "Point", "coordinates": [...] },
  "properties": {
    "_id": "...",
    "barrier": "kerb",
    "kerb": "lowered",
    "tactile_paving": "yes"   // "no" when the survey says the surface is Missing
  }
}
```

`tactile_paving` comes from the survey's `DWS_CONDITIONS` field: Good Condition, Defective (2,021) and the two Off Ramp values (83) map to `yes`, Missing maps to `no`, and Not Applicable leaves the tag off. `yes` includes defective and misplaced surfaces, so DOT's raw value is also kept unchanged as `ext:dws_condition`.

#### Why `kerb=lowered` and not `kerb=flush`

OpenSidewalks defines a Curb Ramp as `kerb=lowered`, so every surveyed ramp gets that value. `ufzp-rrqu` has no ramp type column. Cut-through ramps, which have no ramp run, would be `kerb=flush` in OSM and may be flush curbs in schema terms.

#### Why Curb Nodes, not edge attributes

The OpenSidewalks schema treats curbs as Core Nodes at edge endpoints, not as attributes of the adjacent sidewalk or crossing edges. A routing engine can then apply a cost at the transition point itself, not along the whole edge.

#### Units and signs

Slopes are percentages, signed by direction (running and counter slope from the road to the landing, cross slope left to right facing the ramp from the road). Compare magnitudes, not signed values. The federal design limits for a curb ramp are 1:12 (8.33%) running slope and 1:48 (2.08%) cross slope; 5% is the limit for a walkway and for the counter slope. DOT says its measurements are not indicative of whether a ramp is compliant, and this project does not compare them with design limits.

#### Survey date

The records were captured from March 2017 to January 2020, mostly in 2018. DOT says the imagery was captured between March 2017 and October 2018 and that data collection was completed by October 2019. Record dates run to January 2020, later than that stated completion. This project uses the record dates and has not resolved the difference. The dataset has not changed since October 2021. A ramp rebuilt after its survey date keeps the survey's measurements here.

#### Sentinel value handling

The DOT data dictionary does not define `999`, `888`, `777` or `555`. This project reads them as no measurement because they fall far outside the physical range and recur together on the same rows (`999` on a running slope is probably a cut-through ramp, which has no ramp run). Stage 3 omits these from the artifact and does not carry them (the validator rejects null-valued `ext:*` tags, and the codes would poison any downstream statistics). A counter slope with a magnitude over 100% (steeper than 45 degrees) is not a gutter slope; Stage 3 leaves it off as unmeasured. Six survey values were dropped this way, from -300% to 473%.

#### Stage 4 snapping

In Stage 4, curb nodes are snapped to the nearest edge endpoint within 5 m. Ramp survey coordinates are not always exactly at the OSM edge endpoint. The snap step reconciles the ~meter-level discrepancy between survey coordinates and OSM node positions. The ring vertices of a Pedestrian Zone count as endpoints for this snap. A node holds one ramp's fields, so when several ramps land on the same node the first keeps it and the others stay in the file at their surveyed position, unattached.

A ramp that lands on an OSM vertex shares that vertex's Node. The Node's position is the OSM vertex, and every other value on it comes from the survey. From v0.3.6 such a Node says so: it carries `ext:source=nyc_dot_ramps` and the survey's `ext:source_timestamp`, and `ext:ramp_id` traces it to the survey record. Up to v0.3.4 it said `ext:source=osm_walk`.

---

### 3. NYC Planimetric Database: Sidewalks (`52n9-sdep`)

#### What it is

Sidewalk polygon features produced by the NYC Office of Technology and Innovation from aerial imagery. The polygons are the physical extent of sidewalk surfaces, not centerlines.

#### Where it came from

NYC Open Data Socrata API, paginated in batches of 5,000 rows ordered by `:id`, with the same completeness check as the ramps.

#### License

NYC Open Data terms of use; no licence is attached (see `LICENSE-DATA.md`).

#### How it was transformed

Two uses: sidewalk widths and gap-fill centerlines.

Widths: each OSM sidewalk edge whose centroid falls inside a planimetric polygon gets `width` = 2 × polygon area / perimeter (the mean width of an elongated strip). OSM-surveyed `width` tags take precedence, except a value of 0 or less, which is left off; the planimetric estimate only fills gaps. The value is the mean width of the whole polygon, not the clear width at the edge.

Gap-fill coverage check: for each planimetric polygon, check whether any existing OSM sidewalk edge is within 10 m of the polygon boundary. If covered, skip. If not covered (typically where OSM has only a `sidewalk=both` tag on the street centerline), extract a centerline from the polygon and write it to the sidecar file `nyc-gapfill-sidewalks.geojson` (not to the graph), unless at least half of that centerline lies within 1.5 m of an OSM crossing or footway. That last test removes the median refuges that a crossing already runs through and the paths OSM maps without `footway=sidewalk`.

#### Centerline extraction method (minimum rotated rectangle)

Implemented in `schema_map.py::_polygon_centerline()`. Compute the polygon's minimum rotated rectangle and return the straight line connecting the midpoints of its two short sides. This is O(1) per polygon and fits the elongated strip geometry typical of sidewalk polygons. The axis is only kept when at least 90% of it lies inside its own polygon. For a ring around a block, or an L-shaped polygon, the axis runs through the block interior and is rejected and counted as a failure. Each gap-fill centerline is emitted in both directions.

#### Known limitation

Planimetric-derived sidewalk edges have approximate centerline geometry only, and nearly all are unconnected to OSM nodes. They are not part of the graph and ship as `nyc-gapfill-sidewalks.geojson`.

---

### 4. Borough boundaries (OpenStreetMap via Nominatim)

#### What it is

The five NYC borough boundary polygons (Manhattan, Brooklyn, Queens, The Bronx, Staten Island).

#### Where it came from

The pipeline first asks NYC Open Data for dataset `7t3b-ywvw`, which has been withdrawn (HTTP 404), and then falls back to OSMnx geocoding, which returns the OpenStreetMap boundaries through Nominatim. The fallback is what every build since mid 2026 has used. NYC Open Data's current boundary datasets are `gthc-hcne` (clipped to the shoreline) and `wh2p-dxnf` (water included); if the ID is ever swapped, use the water-included one, because the shoreline version leaves the bridges outside every borough polygon.

#### License

ODbL-1.0 (OpenStreetMap).

#### How it was used

The boundaries have three uses. The five borough polygons are unioned into a single MultiPolygon and written to the root `region` field, which declares the geographic scope of the dataset. A spatial join assigns each feature to the borough whose polygon contains its centroid and writes it as `ext:borough`, for filtering and per-borough splits. Each borough polygon also cuts that borough's graph from the city graph in Stage 1.

---

### 5. NYC 2017 Topobathymetric LiDAR DTM

#### What it is

A bare-earth digital terrain model of NYC captured by LiDAR between May and July 2017 (buildings removed, hydro-flattened), served in metres by the NY State GIS Program Office ArcGIS ImageServer (`NYC_TopoBathymetric_2017_1_meter`; the model's native grid is 1 US foot, 0.305 m).

#### Where it came from

`elevation.its.ny.gov` ImageServer export. Each borough is fetched as a grid of GeoTIFF tiles at 2 m per pixel, 2,048 pixels a side, skipping tiles with no land (about 1.1 GB in a clean build); each tile reaches four pixels into its neighbours so a node near a seam is interpolated from real pixels on both sides.

#### License

Public data, no licence attached (NY State GIS Program Office; NYC OTI survey).

#### How it was used

Stage 4 samples the DTM at every node coordinate, interpolating bilinearly between pixel centres. Each node whose sample lands on valid data gets `ext:elevation_m`; each edge whose two endpoint heights are both known gets `incline` = rise / run, unless the two heights cannot be a slope along the edge. A staircase climbs at about 1 in 2, and nothing that is not steps is built steeper. So where the heights give an edge that is not steps a grade of 0.5 or more, they are on two surfaces: the ground at one end and a deck at the other, the foot and the top of a wall, a sidewalk on a bridge OSM does not tag beside the ground under it, steps OSM does not map. On an edge a few centimetres long they can also be survey noise. On steps the limit is the schema's own, 1.0. Such an edge gets no incline and is marked `ext:incline_unknown=yes`, so a consumer can tell "the heights say something changes level here" from "nothing was measured". Up to v0.3.5 only grades over 1.0 were left off, without a mark, and a grade between 0.5 and 1.0 was written as a slope.

The heights are smoothed over short edges. The graph keeps every OSM vertex as a node, so half its edges are shorter than 7.4 m, and a few decimetres of height error, or a kerb, between two nodes a metre apart reads as a 30% grade. Before the difference is taken, each node's height is averaged with its neighbours' along the path, weighted 1 minus length / 5 m, in two passes, so a node 1 m away counts almost as much as the node itself and one 5 m away not at all. A steady slope comes through unchanged, because the neighbours up and down the path cancel. A step of 0.5 m or more between two close nodes is a real change of level (a wall, untagged steps, a deck beside the ground) and is left alone; steps and tunnel edges take no part. The test of the setting is that real steepness does not depend on how finely a mapper cut a path: in a study area at 1 m resolution the share of footway edges over the wheelchair limits was 6.6% for edges under 2 m against 1.0% for edges of 10 to 25 m before smoothing, and about 1.3% in every length band after. The heights written on the nodes are not smoothed.

A node is only sampled from a tile whose extent contains it. The tiles have no nodata value, and a point outside a tile reads as 0.0.

Inside a tile the service writes "no data" as exactly 0.0 too. That is open water for the most part, 42% of all tile pixels, and no pixel of real ground is exactly 0.0 (27,875 pixels lie within 5 mm of zero without being zero). Stage 4 reads a pixel of exactly 0.0 as no height. A node whose four pixels all hold data is interpolated as before. Where some of the four have no data, on a shoreline or a pier's edge, the height is taken from the pixels that hold data if they carry at least half the interpolation weight; otherwise the node gets no `ext:elevation_m` and its edges no incline. Up to v0.3.5 these pixels were read as ground at 0 m.

A node all of whose edges are tunnel edges is underground, and the terrain model there is the ground above it. It gets no `ext:elevation_m`. A node where a tunnel edge meets any other edge, or the outline of a Pedestrian Zone, is the mouth of the tunnel, in the open, and keeps its terrain height. Of the 1,308 nodes that touch only tunnel edges, 8 are on a zone outline and keep a height; the other 1,300 have none.

An earlier build read the model at 5 to 12 m per pixel, and these figures are from that build. At that resolution, against that build's median edge length of 7 m, the two ends of a short edge usually fall in the same or neighbouring pixels. Reading the nearest pixel gave them either the same elevation or a whole pixel's step; interpolating between pixel centres removes that. In a Midtown window the share of sidewalk edges steeper than 8.33% fell from 4.8% to 2.5% at 7 m per pixel, and the edges over that limit at 7 m and at 10 m per pixel now largely agree. On Staten Island the agreement with the ramp survey's gutter slopes rose a little (rank correlation 0.38 to 0.42).

The model is bare earth and includes the river bed. On a bridge, a deck or a pier it describes what is underneath, so the pipeline reads deck heights from the point clouds instead (next section).

---

### 5b. LiDAR point clouds (2017 NYC, 2014 USGS) for deck heights

#### What it is

The classified point clouds behind the city's 2017 topobathymetric LiDAR (22.8 billion returns) and the 2014 USGS survey of the city (4.8 billion), served by NOAA's Digital Coast archive as Entwine Point Tiles: an octree of LAZ files over plain HTTPS, each node holding one return per voxel of its cube. At depth 9 a tile is a 93 m square with a return about every 0.7 m, 100 to 400 KB.

#### Where it came from

`noaa-nos-coastal-lidar-pds.s3.amazonaws.com/entwine/geoid18/9306` (2017) and `.../4920` (2014). Heights are NAVD88 metres. Only the tiles that contain a node on or beside a structure are fetched, about 2.2 GB city-wide, cached under `data/raw/lidar_points/`.

#### License

Public data on NOAA's Digital Coast archive, no licence attached (NYC OTI and USGS surveys).

#### How it was used

The code is in `pipeline/utils/ept.py`, `pipeline/utils/deck.py` and `assemble._structure_elevations`. Starting from the nodes of edges tagged `bridge` or `elevated`, plus two hops of neighbours, the returns within 2 m of each node are read from the 2017 survey, and from the 2014 survey where the 2017 one has none (it lacks the main spans over open water). Returns are grouped into surfaces by a 0.5 m height gap. A surface is classified if the survey's class says ground or bridge deck; it is solid if it is classified, or flat (interquartile range of 0.15 m) and populated by at least eight returns, which is how a plaza over a road or a deck on a building looks, since the bridge deck class does not cover them. Tree crowns, fences and cables are neither. The walking surface is then chosen by continuity: a node OSM does not put on a structure is on the ground wherever the survey classified ground at terrain height, whatever hangs above it, and from those nodes each next node along the path takes the solid surface nearest in height to the one before, within 0.6 m plus 12% of the edge length (plus the whole length on steps). A node OSM puts on a structure prefers a solid surface off the ground, so the ground seen past the edge of a deck does not pull the deck down. A deck that runs on past the tagged part claims untagged nodes until it meets the ground, and the region of nodes read grows with it. A structure no labelled node leads onto (its approaches are untagged, or it is reached by lift) is started from the node with the clearest solid surface, unless that node sits beside a labelled one whose surfaces were all out of reach, which is what a bridge tower looks like; such nodes, and covered spans, are interpolated between their labelled ends. Structure nodes with no surface at all get no height and their edges no incline.

#### Validation

Heights taken from the 2017 survey were compared with the 2014 survey, which the fix did not use for them: at the node, is there a surface in the 2014 returns at the same height? The numbers are in `validators/QUALITY_REPORT.md` and [`evaluation/structure_incline/`](evaluation/structure_incline/). The places where the surveys disagree by more than 2 m are mostly places rebuilt between the two flights (Hudson Yards, Empire Outlets, the Bayonne Bridge, LaGuardia). Nodes lifted 2 m or more above the terrain model were also checked against the city's planimetric transport structure polygons, which come from photogrammetry and know nothing of OSM tags or LiDAR.

#### Limits

A structure built after May 2017 (LaGuardia's new terminals, the new Kosciuszko span) is read as whatever the 2017 survey saw there. Covered walkways and lower decks under an upper deck are interpolated. A station entrance that OSM joins to the sidewalk by a plain edge, without steps, has the ground at one end and the deck at the other. From v0.3.6 such an edge has no incline and says `ext:incline_unknown`, by the stair-pitch rule of section 5; the pipeline does not carry OSM node tags such as elevators, so it cannot say what joins the two levels. Deck heights have about 5 cm of survey noise, so incline on a 3 m deck edge is still noisy.

---

### 6. MTA ADA Station List

#### What it is

Intended as a list of ADA-accessible NYC subway stations with geographic coordinates, to annotate transit-adjacent pedestrian nodes. It does not currently work and nothing from it ships.

#### Where it came from

The configured NYC Open Data ID (`drh3-e2fd`) is a planimetric hydrography layer, not a station list, so the pipeline falls back to the MTA GTFS static feed. That feed's `stops.txt` has no `wheelchair_boarding` column, so there is no accessibility flag to read and the stage now skips the index. The live station table with ADA fields is `39hk-dx4f` on data.ny.gov; wiring it in is open work.

#### License

MTA open data terms (no source is used at present).

#### How it was used

Not at all. The stage finds no usable source and is skipped: no station file is staged and nothing ships. The intent was a sidecar index that consumers could join to pedestrian nodes to mark transit-adjacent nodes with `ext:ada_accessible=yes`.

#### Why sidecar, not graph nodes

MTA subway station entrances are not pedestrian infrastructure in the OSW sense. They are destinations reachable via the pedestrian network. Including them as graph nodes would require modeling their internal geometry (the staircase or elevator leading underground), which this pipeline does not attempt.

---

## Pipeline Stages

### Stage 1: Acquire

Downloads raw data from five sources (the sixth, MTA, finds no usable source and is skipped) and records provenance (retrieval timestamp, content hash, row count) in `data/raw/manifest.json`. The OSM extract and the DEM tiles are reused when the file is already there; the extract is checked against its pinned SHA-256 every time. The Socrata sources are downloaded on every run.

Borough boundaries are acquired first because the OSM graph is cut by borough polygon.

OSM data is saved as merged nodes/edges GeoJSONs. There is no per-borough graph cache any more (a cache written before the borough seam fix used to be reused silently).

### Stage 2: Clean

Per source:
- Null/empty geometries are dropped
- Invalid geometries are repaired with `shapely.validation.make_valid()`
- CRS is normalized to EPSG:4326
- Column names are normalized to lower case with `_` between words
- Source-specific normalization (DOT sentinel values, planimetric slivers, OSM list-valued columns)

All decisions are recorded in `data/clean/cleaning_report.md`.

### Stage 3: Schema Map

Maps cleaned source data to schema entity types, and writes OSM pedestrian areas as Pedestrian Zones. Every transformation is also documented in a code comment beside it.

The most complex transformation is the planimetric gap-fill: deriving sidewalk centerlines from polygon geometry and filtering by OSM coverage. See the Planimetric section above for the method.

### Stage 4: Assemble

Builds the single canonical FeatureCollection from staged feature files:
1. Snap CurbRamp nodes to edge endpoints within 5 m (reconciles survey/OSM positional discrepancy)
   A Pedestrian Zone takes part in steps 1 to 7 through its ring, as one temporary edge per pair of consecutive ring Nodes. Those rows are dropped before the file is written, and the zone ships as a Polygon whose `_w_id` follows any Node the merge renamed.
2. Close near-miss gaps within 2 m. A node takes another node's ID only when that closes a gap: one of the two is a dead end, or the two are in different connected components of the whole graph or of the pedestrian graph. A dead end is not moved onto a neighbour or onto a node it already reaches within 10 m. Pairs are taken nearest first, a node that has moved is never a target and a target never moves, so no endpoint moves more than 2 m. Curb nodes are carried along with the endpoint they snapped to. The only edges that can collapse are street segments under 2 m whose two ends are in different pedestrian components; they are dropped. The measurements behind the change are in the git history of `release-notes/`.
3. Combine all nodes (OSM nodes + snapped curb nodes)
4. Inject bare nodes for any edge endpoint not yet in the node set
5. Deduplicate nodes by `_id`, preserving curb-ramp annotations when a ramp and an OSM node share a location. The merged Node takes the survey's `ext:source` and `ext:source_timestamp`
6. Compute per-edge `incline`: sample the LiDAR DTM at node coordinates, replace the heights of nodes on and beside bridges and elevated ways with deck heights from the LiDAR point clouds (section 5b), smooth over edges shorter than 5 m, then rise over run; an edge whose heights cannot be a slope (0.5 or more off steps, over 1.0 on steps) gets no incline and `ext:incline_unknown`
7. Write topology report (connected components, fragmentation)
8. Serialize to `data/staged/nyc-osw-unvalidated.geojson`

Root metadata (`$schema`, `dataSource`, `dataTimestamp`, `pipelineVersion`, `region`) is written here. `dataSource.name` names the sources. `dataTimestamp` is the OSM extract's data timestamp. `pipelineVersion` holds the software's `name`, `version`, `url`, `gitSHA` and the build time, `builtAt`.

### Stage 5: Validate (internal pre-check)

Two-layer internal check:
1. **Structural integrity**, over every feature: unique `_id`, correct geometry types, all `_u_id`/`_v_id` references resolve, WGS-84 coordinate bounds.
2. **JSON Schema**, over a 2,000-feature random sample against the OSW v0.3 JSON Schema, using `jsonschema.Draft7Validator`.

Results are written to `output/validation_report.md`.

This stage is a fast pre-check, not the conformance gate. The gate is the Taskar Center's `python-osw-validation` package run against the split ZIP after the endpoint snap (below); a release ships only when it returns `is_valid: True` with zero errors.

### Post-build: endpoint snap

`scripts/snap_endpoints.py` runs after the build. The Stage 4 endpoint merge remaps `_u_id`/`_v_id` without moving edge terminal vertices, which leaves a gap of up to 2 m between a merged edge end and its referenced node coordinate. `python-osw-validation` 0.4.0+ checks those coordinates exactly, so the snap moves every edge endpoint onto its node's coordinate and every Pedestrian Zone ring vertex onto the coordinate of the Node its `_w_id` names. It rounds every coordinate to 7 decimal places (the limit `python-osw-validation` 0.5.0 enforces), rewrites `output/nyc-osw.geojson` in place, and emits the split files (`nyc.nodes.geojson`, `nyc.edges.geojson` and, from v0.3.6, `nyc.zones.geojson`) plus `output/nyc-osw-osw-split.zip` for the validator.

### Stage 6: Export

Three output formats from the same staged FeatureCollection:
- `nyc-osw.geojson` is a copy of the canonical FeatureCollection (the OSW deliverable).
- `nyc.graphml` is a NetworkX MultiDiGraph with one edge per OSW edge from `_u_id` to `_v_id`.
- `nyc-routing.json` is a compact JSON with approximate edge lengths, for a routing engine to load.

A graph has no polygons, so the GraphML and the routing JSON hold each Pedestrian Zone as the edges a person can walk across it (see the routing layer below). Each of those edges carries `ext:zone`, the zone's `_id`.

Stage 6 runs before the endpoint snap, so its GraphML and routing JSON predate the snap and the coordinate rounding. Release assets are made afterwards from the snapped `output/nyc-osw.geojson` with the scripts in `scripts/` (see `scripts/README.md`), and every asset carries the licence, the attribution and the OSM snapshot.

---

## Routing layer: the 5 m ramp to crossing rule

**The rule.** `scripts/osw_to_unweaver.py` builds the layer the wheelchair profile reads. It counts a crossing as having curb ramps when a surveyed ramp lies within 5 m (`RAMP_REACH_M`) of each end of the crossing; the crossing edges of one street crossing are grouped through nodes no sidewalk reaches, and the rule is applied to the group's two ends. The profile in `unweaver-project/cost-wheelchair.py` is adapted from the example wheelchair profile of [Unweaver](https://github.com/nbolten/unweaver) (Nick Bolten, Apache-2.0), whose limits follow AccessMap's manual wheelchair profile; this project added the refusals for steps and street centrelines. It walks a Pedestrian Road like a Footway (`osw_to_unweaver.py` gives it `subclass=footway`). It walks a Pedestrian Zone as two kinds of edge, both made by `zone_edges` in `pipeline/utils/zones.py`: the ring, between consecutive `_w_id` Nodes, and a straight chord between every two ring Nodes that an Edge also touches (the zone's entrances), kept only when the whole chord lies inside the polygon. A route can then cross a plaza between any two of its entrances or follow its edge, and it never cuts across a courtyard the ring bends around. Each of these edges is walked like a Footway and carries `ext:zone`. Its incline is the rise over the run between the two Nodes' heights, and is left off for an edge shorter than 5 m unless its two ends differ by more than 0.5 m, which is a real change of level. A zone edge follows the same stair-pitch rule as an Edge of the file (section 5): where its two heights give a grade of 0.5 or more it has no incline and carries `ext:incline_unknown`. The profile refuses a crossing without ramps at both ends, refuses steps and street centrelines, and refuses an edge steeper than 8.3% up or 10% down.

**Unknown grades.** An edge marked `ext:incline_unknown` (`incline_unknown` on the layer) is read as steeper than any limit and refused, because measured heights say something changes level there. An edge with no incline and no mark (a tunnel edge, a structure edge with no deck height, an edge at a node with no height) was not measured. It passes, as before, because refusing those would cut every underpass on no evidence. That second group is a disclosed limit: the profile can route over an edge whose grade nobody measured. In v0.3.6 the layer has 2,478 marked edges, 2,068 from the file and 410 of the 96,650 zone edges, and 3,124 file edges with no incline and no mark: 2,762 tunnel edges and 362 with an end that has no height. Refusing the marked edges took 23 of the 10,000 trips sampled by borough from the wheelchair profile, 11 of 2,000 sampled city-wide, and the route between the two ends of the High Line, each of which had crossed a change of level as if it were flat (`evaluation/v0.3.6/`). A zone edge under 5 m whose ends are within 0.5 m of each other is a third case: it has no incline because the difference is within survey noise, carries no mark, and passes.

The strict reading of the ramp rule (a ramp on the crossing's own end node) was rejected because it left about 1% of Queens pairs routable.

**How it was checked.** 200 crossings were drawn at random, 40 per borough (seed 20261002). Each was drawn on a sheet over the city's 2018 orthoimagery, the main year of the ramp survey, with the survey's ramp positions marked and no rule's verdict shown. For each end a rater answered whether a surveyed ramp sits where the crossing meets the kerb (yes, no or unclear). The raters were language-model agents following a written protocol: four took 50 sheets each, and a fifth rated every third sheet again without seeing the other ratings. No person rated the sheets and no crossing was visited. The two ratings agreed on 96% of the 132 ends both rated (Cohen's kappa 0.81 over three classes), and on all 125 ends both called yes or no. This kappa is between instances of one language model, so it measures consistency, not accuracy.

**Result.** Of the 191 crossings with no unclear end, 164 were rated ramped at both ends. The 5 m rule calls 166 ramped, and 164 of those were rated ramped (precision 0.988). It misses none of the 164 (recall 1.0). The strict rule finds 56%. A 3 m survey rule passes no unramped crossing but misses 5. With 2 errors against 0, the sample cannot tell these rules apart. The 5 m rule was kept because it was fixed before the check and because the narrower rules leave fewer routes.

**Limits.** The existence of each ramp rests on DOT's survey, collected by a contractor from vehicle-mounted imagery and LiDAR, mostly in 2018: the imagery showed a ramp directly at 9 of the 400 ends. The rating tests which crossing a surveyed ramp serves, not whether the ramp is there today or usable. The false pass rate is 1.2% (2 of 166), with an exact 95% interval of 0.15% to 4.3%. Recall is measured only against ramps that are in the survey, so it cannot detect a crossing refused because the survey missed a ramp. Crossings with zero, one, or three or more ends (2.5%) were not sampled.

The protocol, the sample, every rating and the scores are in [`evaluation/crossing_rule/`](evaluation/crossing_rule/). All 200 rating sheets are in `evaluation-sheets.zip` on the release.

---

## Schema Mapping Decisions

### Sidewalk and plain Footway

OSM ways tagged `footway=sidewalk` become Sidewalks, which the schema defines as a designated pedestrian path to the side of a street. Other `highway=footway` ways become plain Footways. The pipeline does not add `footway=sidewalk` to any way. The difference matters for analysis, because a Sidewalk belongs to a street and a plain Footway does not.

### Why plazas are Pedestrian Zones, not Edges

The schema has a LineString entity for a pedestrian road and a Polygon entity, the Pedestrian Zone, which its JSON Schema describes as "an area where pedestrians can travel freely in all directions". An OSM `area=yes` way is the second. Written as Edges, its outline sends a route round the edge of a plaza and counts the plaza as a road. Written as a Polygon with `_w_id`, it says which Nodes the area joins and leaves the paths across it to the consumer. The routing layer of this project draws those paths as straight chords between entrances that stay inside the polygon. That is this project's choice, not part of the schema.

### Why Curb Ramps are Nodes, not edge attributes

The schema models a curb as a Core Node at an edge endpoint. A curb ramp has its own properties (slopes, warning surface) at one point, so a routing engine can apply a cost at that point instead of spreading it along an edge.

### Why crossings are structurally separated from sidewalks

The schema places Crossings on the road surface only. A plain Footway joins each Crossing to its Sidewalk, and the curb Node sits where that Footway meets the Crossing. The Crossing is split where it meets the street centreline. NYC's OSM data usually skips the joining Footway, and this pipeline does not add it, so many Crossings join Sidewalks directly (in Manhattan, 52% of the nodes on a Crossing also touch a Sidewalk, measured on v0.3.4). Curb Ramps are snapped to the nearest edge endpoint within 5 m, so many sit on that junction or on a Sidewalk vertex. Routing can still apply different costs to crossing and sidewalk edges.

### Handling OSM `sidewalk=both` on street centerlines

Where OSM has `sidewalk=both` or `sidewalk=left/right` tags on a street centerline and no separate sidewalk geometry, the OSM edge is classified as a road Edge (not a Sidewalk Edge) and the planimetric gap-fill pass derives a candidate sidewalk centreline from the planimetric polygon layer, shipped as a sidecar file. This is a simplification: the rectangle axis only stands for the sidewalk when the polygon is a strip, and it is not connected to the rest of the network.

---

## Known Limitations

1. **Incline is estimated, not measured.** It comes from the terrain model at 2 m, smoothed over short edges, and on structures from LiDAR deck heights; it is absent in tunnels and where the terrain model has no data. It has not been compared with surveyed street grades, and a kerb ramp a metre long is below what any airborne survey can resolve. Where two heights give a grade no path could have, the edge has no incline and says `ext:incline_unknown`; the node heights on either side of such an edge may themselves be wrong. The wheelchair profile refuses a marked edge. An edge with no incline and no mark was not measured, and the profile passes it.
2. **No APS (Accessible Pedestrian Signal) data.** It would need a separate NYC DOT dataset or a field survey.
3. **No sidewalk condition ratings.** The DOT ramp dataset has condition flags but there is no equivalent for sidewalk pavement quality citywide.
4. **Planimetric centerlines are unreliable.** The minimum-rotated-rectangle axis is not a centerline. About half of a sample lay on a sidewalk, and nearly all gap-fill segments are unconnected to the rest of the graph. They are not in the graph: they ship as `nyc-gapfill-sidewalks.geojson`, whose root says what they are. The polygons were selected by comparison with OpenStreetMap, so the file's root carries the graph's licence (ODbL-1.0) and attribution.
5. **No live feeds.** The pipeline is a point-in-time snapshot. Rerun to refresh.
6. **MTA ADA annotation not implemented.** No station index is produced (see the MTA section above).
7. **OSM node tags and `sidewalk=*` are not read.** OSM kerbs, elevators and crossing nodes are absent, and streets whose sidewalks OSM maps as tags have no Sidewalk Edge.
8. **36,180 Curb Ramp Nodes are on no Edge and no Pedestrian Zone ring.** The schema maps curbs at edge endpoints.
9. **A Pedestrian Zone has no interior detail.** The file does not know about planters, fountains, steps or level changes inside a plaza, and the routing layer's chords are straight lines between entrances. A zone has an outer ring only. An OSM pedestrian area that does not close into one valid ring stays Pedestrian Road or Footway Edges along its outline.

---

## Roadmap

### Next
- APS signal data from NYC DOT
- MTA ADA station → pedestrian node annotation
- Content-hash-based caching in Stage 1
- Broader tests

### Later
- Sidewalk condition from 311 sidewalk violation data
- Live feed support (rolling updates in place of full rebuilds)
- Vector tiles export for web visualization
