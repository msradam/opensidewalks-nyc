# Methodology

This document records every data source, transformation, and schema mapping decision in the opensidewalks-nyc pipeline. It is updated as the pipeline evolves, not written after the fact.

---

## Data Sources

### 1. OpenStreetMap (dated Geofabrik extract, graph built with OSMnx)

**What it is:** The OpenStreetMap pedestrian walking network for NYC. Footways, paths, crossings, steps, and street edges where foot travel is permitted. Maintained by the OSM community.

**Where it came from:** One dated regional extract from Geofabrik (`new-york-YYMMDD.osm.pbf`), named by URL and SHA-256 in `config/sources.yaml`. Stage 1 downloads it once, refuses a file whose checksum differs from the pin, filters its ways with pyosmium, and hands the result to [OSMnx](https://github.com/gboeing/osmnx) to build the graph. No Overpass query is made.

**License:** [ODbL 1.0](https://www.openstreetmap.org/copyright). Data must be attributed.

**Snapshot:** The extract's own data timestamp (from the PBF header), its URL and its SHA-256 are written to `data/raw/manifest.json` and to `dataSource.osmExtract` in the root of every output file. To move to newer OSM data, change the URL and the checksum together. Releases up to v0.3.1-nyc.1 queried the public Overpass API at build time, so their OSM snapshot is not recorded anywhere. On Staten Island the two sources give the same graph: every finished edge has the same ID, geometry and properties.

**Borough seams:** The city graph is built once and each borough is cut from it with `truncate_by_edge=True`, so the segments that cross a borough line are kept (both neighbouring boroughs hold them and Stage 4 removes the duplicate). The v0.3.1-nyc.1 release was built without this, so its boroughs are joined at only 13 nodes and nearly every bridge is cut mid-span.

**Reach of the extract:** It covers New York State. A way that crosses into New Jersey is kept whole, but a study-area box that reaches across the state line gets no New Jersey streets.

**Why explicit custom_filter, not `network_type='walk'`:** OSMnx's `network_type='walk'` applies its own undocumented heuristics for what counts as walkable. For a standards-conformant pipeline, we prefer explicit control: we whitelist specific `highway` tag values and exclude `foot=no` and `access=no`. This makes the inclusion criteria auditable.

**Custom filter used:**
```
["highway"~"footway|path|pedestrian|steps|residential|service|tertiary|secondary|primary|cycleway|track|living_street"]["foot"!~"no"]["access"!~"no|private"]
```

The syntax is Overpass's and so are the semantics: each regex is an unanchored search, and a way with no `foot` or `access` tag passes. There is one departure. In OSM's access rules a tag for one mode is more specific than `access`, so a way that fails only the access clause is kept when it carries `foot=yes`, `designated` or `permissive`. The Queensboro Bridge walkway is tagged `access=no`, `foot=designated`. Up to v0.3.1-nyc.1 the filter was folded over two lines in the YAML, which put a space before `secondary`, so no secondary road was ever requested and none is in those releases. `highway=unclassified` is not in the filter, although Stage 3 would keep it. The link roads (`primary_link` and so on) match the unanchored regex and are then dropped by Stage 3.

**How it was transformed:** OSM edges are classified into four OSW feature types based on `highway` and `footway` tag values:
- `highway=footway` + `footway=sidewalk` → Sidewalk Edge
- `highway=footway` + `footway=crossing` → Crossing Edge
- `highway=footway|path|pedestrian|steps` (other) → Footway Edge
- `highway=cycleway|track` with `foot=yes`, `designated` or `permissive` → the same three classes, with `ext:osm_highway` keeping the OSM value. Without one of those foot values the way is dropped. Many bridge paths and greenways are mapped this way; v0.3.1-nyc.1 dropped them all.
- `highway=residential|service|...` → Street Edge

The graph is built without OSMnx's walk mode, so a way tagged `oneway=yes` arrives in one direction. Stage 3 adds the missing reverse of every pedestrian edge (OSM's `oneway` binds vehicles and bicycles, not people on foot). Streets keep their direction.

An edge whose OSM way is a bridge or a tunnel is marked `ext:structure` (`bridge` or `tunnel`; a building passage is not marked). Stage 4 writes no incline on those edges.

OSM `surface` tags are mapped to the OSW surface enum (9 canonical values). Non-canonical OSM surface values (e.g. `tarmac`, `cobblestone`) are mapped to the nearest canonical equivalent.

OSM `crossing` tags are mapped to `crossing:markings`. Non-canonical values (e.g. `marked`, `traffic_signals`) are mapped to the nearest canonical equivalent.

**Known issues:** OSM coverage of NYC sidewalks is incomplete. Many streets have `sidewalk=both` or `sidewalk=left/right` tags on the street centerline rather than separate sidewalk geometry. These are handled by Stage 3's planimetric gap-fill pass.

---

### 2. NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`)

**What it is:** A point dataset of 217,000+ pedestrian curb ramp locations citywide, surveyed by the NYC Department of Transportation 2017-2020. Records ramp location, geometry (running slope, cross slope, landing dimensions), and condition.

**Where it came from:** NYC Open Data Socrata API (`data.cityofnewyork.us/resource/ufzp-rrqu.json`), paginated in batches of 10,000 rows ordered by `:id`. Stage 1 counts distinct row IDs and compares the total with the dataset's own `count(*)`, and stops if they differ. Without `$order`, Socrata pages can overlap: one Staten Island pull of 23,326 rows held 16,664 distinct ramps.

**License:** Public Domain (NYC Open Data).

**How it was transformed:** Each ramp point becomes an OSW CurbRamp Point Node:
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

`tactile_paving` comes from the survey's `DWS_CONDITIONS` field: Good Condition, Defective and the two Off Ramp values map to `yes`, Missing maps to `no`, and Not Applicable leaves the tag off. The v0.3.1-nyc.1 release set `yes` for every non-empty value, including Missing (59% of ramps).

**Why `kerb=lowered` and not `kerb=flush`:** The DOT dataset records all ramps as lowered-curb ramps. There is no distinction between lowered and flush in the source data. `kerb=lowered` is the correct value for a curb ramp that transitions from sidewalk level to road level via a slope.

**Why Curb Nodes, not edge attributes:** The OpenSidewalks spec treats curb interfaces as first-class Point Nodes, not as attributes of the adjacent sidewalk or crossing edges. This enables routing engines to impose cost penalties at the transition point itself. Not on the entire edge.

**Units and signs:** Slopes are percentages, signed by direction (running and counter slope from the road to the landing, cross slope left to right facing the ramp from the road). Compare magnitudes, not signed values. The federal design limits for a curb ramp are 1:12 (8.33%) running slope and 1:48 (2.08%) cross slope; 5% is the limit for a walkway and for the counter slope. The survey's own description says its measurements do not by themselves establish ADA compliance.

**Survey date:** The survey was captured almost entirely in 2018 and the dataset has not changed since October 2021. Ramps rebuilt since then keep their old measurements here.

**Sentinel value handling:** The DOT dataset uses `999`, `888`, `777` and `555` where there is no measurement (`999` is mostly cut-through ramps, which have no ramp run). Stage 3 omits these from the artifact rather than carrying them (the validator rejects null-valued `ext:*` tags, and the codes would poison any downstream statistics). The v0.3.1-nyc.1 release filtered only `999`.

**Stage 4 snapping:** In Stage 4, curb nodes are snapped to the nearest edge endpoint within 5 m. Ramp survey coordinates are not always exactly at the OSM edge endpoint. The snap step reconciles the ~meter-level discrepancy between survey coordinates and OSM node positions. A node holds one ramp's fields, so when several ramps land on the same node the first keeps it and the others stay in the file at their surveyed position, unattached.

---

### 3. NYC Planimetric Database: Sidewalks (`52n9-sdep`)

**What it is:** Sidewalk polygon features produced by the NYC Office of Technology and Innovation from aerial imagery. The polygons represent the physical extent of sidewalk surfaces, not centerlines.

**Where it came from:** NYC Open Data Socrata API, paginated in batches of 5,000 rows ordered by `:id`, with the same completeness check as the ramps.

**License:** Public Domain (NYC Open Data).

**How it was transformed:** Two uses: sidewalk widths and gap-fill centerlines.

Widths: each OSM sidewalk edge whose centroid falls inside a planimetric polygon gets `width` = 2 × polygon area / perimeter (the mean width of an elongated strip). The perimeter counts interior rings, because many polygons are rings around a whole block; the v0.3.1-nyc.1 release used the outer ring only, which about doubles the width on those. OSM-surveyed `width` tags take precedence; the planimetric estimate only fills gaps. The value is the mean width of the whole polygon, not the clear width at the edge.

Gap-fill coverage check: for each planimetric polygon, check whether any existing OSM sidewalk edge is within 10 m of the polygon boundary. If covered, skip. If not covered (typically where OSM has only a `sidewalk=both` tag on the street centerline), extract a centerline from the polygon and emit it as a Sidewalk Edge, unless at least half of that centerline lies within 1.5 m of an OSM crossing or footway. That last test removes the median refuges that a crossing already runs through and the paths OSM maps without `footway=sidewalk`.

**Centerline extraction method (minimum rotated rectangle):** Implemented in `schema_map.py::_polygon_centerline()`. Compute the polygon's minimum rotated rectangle and return the straight line connecting the midpoints of its two short sides. This is O(1) per polygon and fits the elongated strip geometry typical of sidewalk polygons. The axis is only kept when at least 90% of it lies inside its own polygon. For a ring around a block, or an L-shaped polygon, the axis runs through the block interior and is rejected and counted as a failure. The v0.3.1-nyc.1 release had no such check, and in an imagery sample 13 of 15 of its gap-fill edges were not on a sidewalk. Each gap-fill centerline is emitted in both directions.

**Known limitation:** Planimetric-derived sidewalk edges have approximate centerline geometry only. They may not connect cleanly to adjacent OSM nodes. The Stage 4 assemble step injects bare nodes at their endpoints to satisfy the OSW structural requirement that all `_u_id`/`_v_id` references resolve to Node features.

---

### 4. Borough boundaries (OpenStreetMap via Nominatim)

**What it is:** The five NYC borough boundary polygons (Manhattan, Brooklyn, Queens, The Bronx, Staten Island).

**Where it came from:** The pipeline first asks NYC Open Data for dataset `7t3b-ywvw`, which has been withdrawn (HTTP 404), and then falls back to OSMnx geocoding, which returns the OpenStreetMap boundaries through Nominatim. The fallback is what every build since mid 2026 has used. NYC Open Data's current boundary datasets are `gthc-hcne` (clipped to the shoreline) and `wh2p-dxnf` (water included); if the ID is ever swapped, use the water-included one, because the shoreline version leaves the bridges outside every borough polygon.

**License:** ODbL-1.0 (OpenStreetMap).

**How it was used:**
1. **Root metadata `region`:** The five borough polygons are unioned into a single MultiPolygon and written to the OSW root-level `region` field. This is the geographic scope declaration of the dataset.
2. **Per-feature `ext:borough`:** A spatial join assigns each feature to the borough whose polygon contains its centroid. Used for downstream filtering and analysis.
3. **OSM cut:** Each borough polygon is used to cut that borough's graph from the city graph in Stage 1.

---

### 5. NYC 2017 Topobathymetric LiDAR DTM

**What it is:** A bare-earth digital terrain model of NYC captured by LiDAR between May and July 2017 (buildings removed, hydro-flattened), served in metres on a 1 m grid by the NY State GIS Program Office ArcGIS ImageServer (`NYC_TopoBathymetric_2017_1_meter`).

**Where it came from:** `elevation.its.ny.gov` ImageServer export, one GeoTIFF tile per borough. The pipeline caps each tile at 3,000 pixels a side, so a borough tile is resampled to 5 to 12 m per pixel (Bronx 5.2 m, Staten Island 6.5 m, Brooklyn 7.0 m, Manhattan 7.5 m, Queens 11.9 m).

**License:** Public Domain (NY State).

**How it was used:** Stage 4 samples the DTM at every node coordinate, interpolating bilinearly between pixel centres. Each node whose sample lands on valid data gets `ext:elevation_m`; each edge whose two endpoint elevations are both known gets `incline` = rise / run, clamped to the OSW range of ±1.0. Values outside that range are DEM noise on very short edges and are dropped rather than clamped into pseudo-plausibility.

A node is only sampled from a tile whose extent contains it. The tiles have no nodata value, and a point outside a tile reads as 0.0; the v0.3.1-nyc.1 release sampled every node from the first tile (the Bronx), so 80% of its nodes have an elevation of exactly 0.0 and their edges an incline of 0.

At 5 to 12 m per pixel against a median edge length of 7 m, the two ends of a short edge usually fall in the same or neighbouring pixels. Reading the nearest pixel gave them either the same elevation or a whole pixel's step; interpolating between pixel centres removes that. In a Midtown window the share of sidewalk edges steeper than 8.33% fell from 4.8% to 2.5% at 7 m per pixel, and the edges over that limit at 7 m and at 10 m per pixel now largely agree. On Staten Island the agreement with the ramp survey's gutter slopes rose a little (rank correlation 0.38 to 0.42).

The model is bare earth and includes the river bed. On a bridge, a deck or a pier it describes what is underneath, so edges marked `ext:structure` get no incline. Node elevations on those structures are still the ground or water below.

---

### 6. MTA ADA Station List

**What it is:** Intended as a list of ADA-accessible NYC subway stations with geographic coordinates, to annotate transit-adjacent pedestrian nodes. It does not currently work and nothing from it ships.

**Where it came from:** The configured NYC Open Data ID (`drh3-e2fd`) is a planimetric hydrography layer, not a station list, so the pipeline falls back to the MTA GTFS static feed. That feed's `stops.txt` has no `wheelchair_boarding` column, so there is no accessibility flag to read and the stage now skips the index. The live station table with ADA fields is `39hk-dx4f` on data.ny.gov; wiring it in is open work.

**License:** Public Domain (MTA).

**How it was used:** Sidecar annotation only. MTA station points are indexed in the staged data (`data/staged/mta_ada_stations.geojson`). Downstream consumers can spatially join this index to pedestrian nodes to identify transit-adjacent nodes and annotate them with `ext:ada_accessible=yes`. This join is not currently implemented in the pipeline. V1.1 scope.

**Why sidecar, not graph nodes:** MTA subway station entrances are not pedestrian infrastructure in the OSW sense. They are destinations reachable via the pedestrian network. Including them as graph nodes would require modeling their internal geometry (the staircase/elevator leading underground), which is out of V1 scope.

---

## Pipeline Stages

### Stage 1: Acquire

Downloads raw data from all six sources and records provenance (retrieval timestamp, content hash, row count) in `data/raw/manifest.json`. The OSM extract and the DEM tiles are reused when the file is already there; the extract is checked against its pinned SHA-256 every time. The Socrata sources are downloaded on every run.

Borough boundaries are acquired first because the OSM graph is cut by borough polygon.

OSM data is saved as merged nodes/edges GeoJSONs. There is no per-borough graph cache any more (a cache written before the borough seam fix used to be reused silently).

### Stage 2: Clean

Per source:
- Null/empty geometries are dropped
- Invalid geometries are repaired with `shapely.validation.make_valid()`
- CRS is normalized to EPSG:4326
- Column names are normalized to lowercase/underscore
- Source-specific normalization (DOT sentinel values, planimetric slivers, OSM list-valued columns)

All decisions are recorded in `data/clean/cleaning_report.md`.

### Stage 3: Schema Map

Maps cleaned source data to OSW-conformant feature types. Every transformation is documented in code comments adjacent to the transformation itself (not just here).

The most complex transformation is the planimetric gap-fill: deriving sidewalk centerlines from polygon geometry and filtering by OSM coverage. See the Planimetric section above for the method.

### Stage 4: Assemble

Builds the single canonical FeatureCollection from staged feature files:
1. Snap CurbRamp nodes to edge endpoints within 5 m (reconciles survey/OSM positional discrepancy)
2. Close near-miss gaps within 2 m. A node takes another node's ID only when that closes a gap: one of the two is a dead end, or the two are in different connected components of the whole graph or of the pedestrian graph. A dead end is not moved onto a neighbour or onto a node it already reaches within 10 m. Pairs are taken nearest first, a node that has moved is never a target and a target never moves, so no endpoint moves more than 2 m. Curb nodes are carried along with the endpoint they snapped to. The only edges that can collapse are street segments under 2 m whose two ends are in different pedestrian components; they are dropped. Up to v0.3.1-nyc.1 the merge united every pair of endpoints within 2 m with union-find, which chains along closely spaced vertices: on Staten Island it dropped a quarter of all edges and moved endpoints up to 33 m. The comparison behind the change is in the v0.3.2 release notes.
3. Combine all nodes (OSM nodes + snapped curb nodes)
4. Inject bare nodes for any edge endpoint not yet in the node set
5. Deduplicate nodes by `_id`, preserving curb-ramp annotations when a ramp and an OSM node share a location
6. Compute per-edge `incline` by sampling the LiDAR DTM at node coordinates (rise over run, clamped to the OSW range of ±1.0)
7. Write topology report (connected components, fragmentation)
8. Serialize to `data/staged/nyc-osw-unvalidated.geojson`

Root metadata (`$schema`, `dataSource`, `dataTimestamp`, `pipelineVersion`, `region`) is written here.

### Stage 5: Validate (internal pre-check)

Two-layer internal check:
1. **Structural integrity**, over every feature: unique `_id`, correct geometry types, all `_u_id`/`_v_id` references resolve, WGS-84 coordinate bounds.
2. **JSON Schema**, over a 2,000-feature random sample against the OSW v0.3 JSON Schema, using `jsonschema.Draft7Validator`.

Results are written to `output/validation_report.md`.

This stage is a fast pre-check, not the conformance gate. The gate is the official `python-osw-validation` package run against the split ZIP after the endpoint snap (below); a release ships only when it returns `is_valid: True` with zero errors.

### Post-build: endpoint snap

`scripts/snap_endpoints.py` runs after the build. The Stage 4 endpoint merge remaps `_u_id`/`_v_id` without moving edge terminal vertices, which leaves a gap of up to 2 m between a merged edge end and its referenced node coordinate. `python-osw-validation` 0.4.0+ checks those coordinates exactly, so the snap moves every edge endpoint onto its node's coordinate, rounds every coordinate to 7 decimal places (the limit `python-osw-validation` 0.5.0 enforces), rewrites `output/nyc-osw.geojson` in place, and emits the split node/edge files plus `output/nyc-osw-osw-split.zip` for the validator.

### Stage 6: Export

Three output formats from the same staged FeatureCollection:
- **nyc-osw.geojson**. Copy of the canonical FeatureCollection (the OSW deliverable)
- **nyc.graphml**. NetworkX MultiDiGraph, one edge per OSW edge from `_u_id` to `_v_id`
- **nyc-routing.json**. Compact JSON with approximate edge lengths, intended for downstream routing engine consumption

Stage 6 runs before the endpoint snap, so its GraphML and routing JSON predate the snap and the coordinate rounding. Release assets are made afterwards from the snapped `output/nyc-osw.geojson` with the scripts in `scripts/` (see `scripts/README.md`), and every asset carries the licence, the attribution and the OSM snapshot.

---

## Schema Mapping Decisions

### Why `footway=sidewalk` on all OSM sidewalk-type edges

The OpenSidewalks spec treats sidewalks as distinct from generic footways: a `footway=sidewalk` edge represents a pedestrian path that runs parallel to a road, physically separated from it. OSM edges tagged `highway=footway` without a `footway` sub-tag are mapped to the `footway` Edge type (generic pedestrian path), not the `sidewalk` type. This distinction matters for accessibility analysis: sidewalks have a known relationship to the adjacent road, which enables inferring crossing locations and street-side context.

### Why CurbRamp nodes are Point Nodes, not edge attributes

The OSW spec explicitly models curb interfaces as Point Nodes rather than attributes of adjacent edges. This design choice reflects the physical reality: the curb ramp is a discrete feature with its own accessibility properties (slope, width, tactile paving) located at a specific point in space. By making it a Node, routing engines can apply cost penalties at the exact transition point between sidewalk and road surfaces. Not amortized across an entire edge.

### Why crossings are structurally separated from sidewalks

The OSW spec requires crossings to be modeled as separate Edge features that exist on the road surface, connecting curb nodes on opposite sides of the street. This separation (sidewalk → curb node → crossing edge → curb node → sidewalk) enables accessibility-aware routing to apply different cost functions to crossing and sidewalk segments. A wheelchair user, a stroller pusher, and a sighted walker all have different costs for an unmarked crossing, a zebra crossing with a curb ramp, and a signalized crossing with APS.

### Handling OSM `sidewalk=both` on street centerlines

Where OSM has `sidewalk=both` or `sidewalk=left/right` tags on a street centerline rather than separate sidewalk geometry, the OSM edge is classified as a Street Edge (not a Sidewalk Edge) and the planimetric gap-fill pass derives the sidewalk geometry from the planimetric polygon layer. This is a simplification: the rectangle axis only stands for the sidewalk when the polygon is a strip, and it is not connected to the rest of the network unless an endpoint falls within 2 m of another.

---

## Known Limitations

1. **Incline is DEM-derived, not surveyed.** It is an estimate of the terrain from a 5 to 12 m grid, absent on bridges and tunnels, and it agrees with surveyed street grades in the large, not edge by edge. Values outside the OSW ±1.0 range are dropped.
2. **No APS (Accessible Pedestrian Signal) data.** Would require a separate NYC DOT dataset or field survey.
3. **No sidewalk condition ratings.** The DOT ramp dataset has condition flags but there is no equivalent for sidewalk pavement quality citywide.
4. **Planimetric centerlines are unreliable.** The minimum-rotated-rectangle axis is not a centerline. About half of a v0.3.2 sample lay on a sidewalk, and nearly all gap-fill segments are unconnected to the rest of the graph.
5. **No live feeds.** The pipeline is a point-in-time snapshot. Rerun to refresh.
6. **MTA ADA annotation not implemented.** No station index is produced (see the MTA section above).

---

## Roadmap

### V1.1
- APS signal data from NYC DOT
- MTA ADA station → pedestrian node annotation
- Content-hash-based caching in Stage 1
- Comprehensive test suite

### V1.2
- Sidewalk condition from 311 sidewalk violation data
- Live feed support (rolling updates rather than full rebuilds)
- Vector tiles export for web visualization
