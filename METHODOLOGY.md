# Methodology

This document records every data source, transformation, and schema mapping decision in the ariadne-nyc pipeline. It is updated as the pipeline is built. Not written after the fact.

---

## Data Sources

### 1. OpenStreetMap (via OSMnx)

**What it is:** The OpenStreetMap pedestrian walking network for NYC. Footways, paths, crossings, steps, and street edges where foot travel is permitted. Maintained by the OSM community.

**Where it came from:** Fetched via [OSMnx](https://github.com/gboeing/osmnx) using the Overpass API. Queried borough-by-borough (five separate queries) to manage memory.

**License:** [ODbL 1.0](https://www.openstreetmap.org/copyright). Data must be attributed.

**Why borough-by-borough:** NYC is large. A single city-wide query would time out or exhaust memory on the Overpass API. Querying by borough polygon produces manageable payloads and allows partial reruns if one borough fails.

**Why explicit custom_filter, not `network_type='walk'`:** OSMnx's `network_type='walk'` applies its own undocumented heuristics for what counts as walkable. For a standards-conformant pipeline, we prefer explicit control: we whitelist specific `highway` tag values and exclude `foot=no` and `access=no`. This makes the inclusion criteria auditable.

**Custom filter used:**
```
["highway"~"footway|path|pedestrian|steps|residential|service|tertiary|secondary|primary|cycleway|track|living_street"]["foot"!~"no"]["access"!~"no|private"]
```

**How it was transformed:** OSM edges are classified into four OSW feature types based on `highway` and `footway` tag values:
- `highway=footway` + `footway=sidewalk` → Sidewalk Edge
- `highway=footway` + `footway=crossing` → Crossing Edge
- `highway=footway|path|pedestrian|steps` (other) → Footway Edge
- `highway=residential|service|...` → Street Edge

OSM `surface` tags are mapped to the OSW surface enum (9 canonical values). Non-canonical OSM surface values (e.g. `tarmac`, `cobblestone`) are mapped to the nearest canonical equivalent.

OSM `crossing` tags are mapped to `crossing:markings`. Non-canonical values (e.g. `marked`, `traffic_signals`) are mapped to the nearest canonical equivalent.

**Known issues:** OSM coverage of NYC sidewalks is incomplete. Many streets have `sidewalk=both` or `sidewalk=left/right` tags on the street centerline rather than separate sidewalk geometry. These are handled by Stage 3's planimetric gap-fill pass.

---

### 2. NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`)

**What it is:** A point dataset of 217,000+ pedestrian curb ramp locations citywide, surveyed by the NYC Department of Transportation 2017-2020. Records ramp location, geometry (running slope, cross slope, landing dimensions), and condition.

**Where it came from:** NYC Open Data Socrata API (`data.cityofnewyork.us/resource/ufzp-rrqu.json`), paginated in batches of 10,000 rows.

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
    "tactile_paving": "yes"   // if dws_conditions is non-empty
  }
}
```

**Why `kerb=lowered` and not `kerb=flush`:** The DOT dataset records all ramps as lowered-curb ramps. There is no distinction between lowered and flush in the source data. `kerb=lowered` is the correct value for a curb ramp that transitions from sidewalk level to road level via a slope.

**Why Curb Nodes, not edge attributes:** The OpenSidewalks spec treats curb interfaces as first-class Point Nodes, not as attributes of the adjacent sidewalk or crossing edges. This enables routing engines to impose cost penalties at the transition point itself. Not on the entire edge.

**Sentinel value handling:** The DOT dataset uses `999.0` to represent unmeasurable or missing measurements. These are replaced with `null` in Stage 2 (clean).

**Stage 4 snapping:** In Stage 4, curb nodes are snapped to the nearest edge endpoint within 5 m. Ramp survey coordinates are not always exactly at the OSM edge endpoint. The snap step reconciles the ~meter-level discrepancy between survey coordinates and OSM node positions.

---

### 3. NYC Planimetric Database: Sidewalks (`vfx9-tbb6`)

**What it is:** Sidewalk polygon features produced by the NYC Office of Technology and Innovation from aerial imagery. The polygons represent the physical extent of sidewalk surfaces, not centerlines.

**Where it came from:** NYC Open Data Socrata API, paginated in batches of 5,000 rows.

**License:** Public Domain (NYC Open Data).

**How it was transformed:** Used as a gap-fill source for OSM sidewalk coverage.

The coverage check: for each planimetric polygon, check whether any existing OSM sidewalk edge is within 10 m of the polygon boundary. If covered, skip. If not covered (typically where OSM has only a `sidewalk=both` tag on the street centerline), extract a centerline from the polygon and emit it as a Sidewalk Edge.

**Centerline extraction method (Voronoi skeleton):** The `centerline` Python package was not used because recent versions require GDAL as a system dependency, which complicates reproducibility across environments. Instead, a custom Voronoi-based skeleton is implemented in `schema_map.py::_polygon_centerline()`:

1. Densify the polygon boundary by interpolating points at ~0.5 m intervals (in EPSG:32618, UTM Zone 18N for metric accuracy).
2. Compute a Voronoi diagram over the boundary points.
3. Retain only Voronoi ridge line segments that lie strictly inside the polygon.
4. Return the longest connected component as the centerline.

This approach works well for elongated strip geometry (typical of sidewalk polygons). It fails for irregular or L-shaped polygons. Those failures are counted and reported, not silently dropped.

**Known limitation:** Planimetric-derived sidewalk edges have approximate centerline geometry only. They may not connect cleanly to adjacent OSM nodes. The Stage 4 assemble step injects bare nodes at their endpoints to satisfy the OSW structural requirement that all `_u_id`/`_v_id` references resolve to Node features.

---

### 4. NYC Borough Boundaries (`7t3b-ywvw`)

**What it is:** The five NYC borough boundary polygons (Manhattan, Brooklyn, Queens, The Bronx, Staten Island) from NYC Open Data.

**Where it came from:** NYC Open Data Socrata API. Falls back to OSMnx geocoding if the Socrata endpoint is unavailable.

**License:** Public Domain (NYC Open Data).

**How it was used:**
1. **Root metadata `region`:** The five borough polygons are unioned into a single MultiPolygon and written to the OSW root-level `region` field. This is the geographic scope declaration of the dataset.
2. **Per-feature `ext:borough`:** A spatial join assigns each feature to the borough whose polygon contains its centroid. Used for downstream filtering and analysis.
3. **OSM query bounds:** Each borough polygon is passed to OSMnx as the query boundary in Stage 1.

---

### 5. MTA ADA Station List

**What it is:** A list of ADA-accessible NYC subway stations with geographic coordinates. Used to annotate transit-adjacent pedestrian nodes, not as part of the pedestrian graph topology.

**Where it came from:** NYC Open Data subway stations dataset (`drh3-e2fd`), falling back to MTA GTFS static feed (`stops.txt` with `wheelchair_boarding` column) if unavailable.

**License:** Public Domain (MTA).

**How it was used:** Sidecar annotation only. MTA station points are indexed in the staged data (`data/staged/mta_ada_stations.geojson`). Downstream consumers can spatially join this index to pedestrian nodes to identify transit-adjacent nodes and annotate them with `ext:ada_accessible=yes`. This join is not currently implemented in the pipeline. V1.1 scope.

**Why sidecar, not graph nodes:** MTA subway station entrances are not pedestrian infrastructure in the OSW sense. They are destinations reachable via the pedestrian network. Including them as graph nodes would require modeling their internal geometry (the staircase/elevator leading underground), which is out of V1 scope.

---

## Pipeline Stages

### Stage 1: Acquire

Downloads raw data from all five sources and records provenance (retrieval timestamp, content hash, row count) in `data/raw/manifest.json`. Caches by file existence. A content-hash cache-busting mechanism will be added in V1.1.

Borough boundaries are acquired first because OSM borough queries require the polygon bounds.

OSM data is saved as per-borough GraphML files plus merged nodes/edges GeoJSONs. The GraphML files enable re-loading without re-querying OSM if a later stage needs to restart.

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
2. Combine all nodes (OSM nodes + snapped curb nodes)
3. Inject bare nodes for any edge endpoint not yet in the node set
4. Deduplicate nodes by `_id`
5. Write topology report (connected components, fragmentation)
6. Serialize to `data/staged/nyc-osw-unvalidated.geojson`

Root metadata (`$schema`, `dataSource`, `dataTimestamp`, `pipelineVersion`, `region`) is written here.

### Stage 5: Validate

Two-layer validation:
1. **JSON Schema**. Validates every feature against the OSW v0.3 JSON Schema downloaded from `sidewalks.washington.edu`. Implemented using `jsonschema.Draft7Validator`.
2. **Structural integrity**. Checks that are independent of the schema spec: unique `_id`, correct geometry types, all `_u_id`/`_v_id` references resolve, WGS-84 coordinate bounds.

Results are written to `output/validation_report.md`.

### Stage 6: Export

Three output formats from the same staged FeatureCollection:
- **nyc-osw.geojson**. Copy of the canonical FeatureCollection (the OSW deliverable)
- **nyc.graphml**. NetworkX DiGraph with nodes/edges, suitable for academic analysis
- **nyc-routing.json**. Compact JSON with approximate edge lengths, intended for downstream routing engine consumption

---

## Schema Mapping Decisions

### Why `footway=sidewalk` on all OSM sidewalk-type edges

The OpenSidewalks spec treats sidewalks as distinct from generic footways: a `footway=sidewalk` edge represents a pedestrian path that runs parallel to a road, physically separated from it. OSM edges tagged `highway=footway` without a `footway` sub-tag are mapped to the `footway` Edge type (generic pedestrian path), not the `sidewalk` type. This distinction matters for accessibility analysis: sidewalks have a known relationship to the adjacent road, which enables inferring crossing locations and street-side context.

### Why CurbRamp nodes are Point Nodes, not edge attributes

The OSW spec explicitly models curb interfaces as Point Nodes rather than attributes of adjacent edges. This design choice reflects the physical reality: the curb ramp is a discrete feature with its own accessibility properties (slope, width, tactile paving) located at a specific point in space. By making it a Node, routing engines can apply cost penalties at the exact transition point between sidewalk and road surfaces. Not amortized across an entire edge.

### Why crossings are structurally separated from sidewalks

The OSW spec requires crossings to be modeled as separate Edge features that exist on the road surface, connecting curb nodes on opposite sides of the street. This separation (sidewalk → curb node → crossing edge → curb node → sidewalk) enables accessibility-aware routing to apply different cost functions to crossing vs. Sidewalk segments. A wheelchair user, a stroller pusher, and a sighted walker all have different costs for an unmarked crossing vs. A zebra crossing with a curb ramp vs. A traffic signal crossing with APS signals.

### Handling OSM `sidewalk=both` on street centerlines

Where OSM has `sidewalk=both` or `sidewalk=left/right` tags on a street centerline rather than separate sidewalk geometry, the OSM edge is classified as a Street Edge (not a Sidewalk Edge) and the planimetric gap-fill pass derives the sidewalk geometry from the planimetric polygon layer. This is a simplification: the derived centerline may not be geometrically precise to within <1 m, but it is directionally correct and connects to the rest of the network.

---

## Known Limitations (V1)

1. **No incline/slope data.** The OSW schema supports `incline` on edges (% grade) but deriving it requires a DEM and spatial interpolation. V1.1 scope.
2. **No APS (Accessible Pedestrian Signal) data.** APS signals at crossings are V1.1 scope (requires a separate NYC DOT dataset or field survey).
3. **No sidewalk condition ratings.** The DOT ramp dataset has condition flags but there is no equivalent for sidewalk pavement quality citywide. V1.1 scope.
4. **Planimetric centerlines are approximate.** The Voronoi skeleton method produces geometrically correct but not survey-accurate centerlines.
5. **No live feeds.** The pipeline is a point-in-time snapshot. Rerun to refresh.
6. **MTA ADA annotation not implemented.** The MTA ADA station index is acquired and staged but the spatial join to pedestrian nodes is not yet implemented. V1.1 scope.

---

## Roadmap

### V1.1
- Incline data from USGS 3DEP DEM
- APS signal data from NYC DOT
- MTA ADA station → pedestrian node annotation
- Content-hash-based caching in Stage 1
- Comprehensive test suite

### V1.2
- Sidewalk condition from 311 sidewalk violation data
- Live feed support (rolling updates rather than full rebuilds)
- Vector tiles export for web visualization
