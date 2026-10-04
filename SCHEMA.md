# Schema reference

OpenSidewalks NYC v0.3.5-nyc.1 passes `python-osw-validation` 0.5.0 against the **[OpenSidewalks Schema v0.3](https://github.com/OpenSidewalks/OpenSidewalks-Schema)** JSON Schema. That validator checks form, not the schema's topology rules, and the data departs from the schema in the ways listed under [Known deviations from the schema](#known-deviations-from-the-schema). This document is a quick reference for consumers describing the properties actually present in the artifact. Read the upstream spec for the authoritative type definitions.

## Top-level

A single GeoJSON `FeatureCollection` (`output/nyc-osw.geojson`). Root metadata:

| Field | Value |
|---|---|
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` (the schema's `$id`; the readable schema is in the [schema repository](https://github.com/OpenSidewalks/OpenSidewalks-Schema/blob/main/opensidewalks.schema.json)) |
| `dataSource` | `name` lists the sources the data came from (OpenStreetMap, NYC DOT Pedestrian Ramp Locations, NYC Planimetric Sidewalks, NYC 2017 LiDAR). `url` is this repository. Also `license` (`ODbL-1.0`), `licenseUrl`, `attribution` (the credit line to reuse) and `osmExtract` (`url`, `sha256` and `dataTimestamp` of the OSM extract the build read). |
| `dataTimestamp` | How current the data is: the OSM extract's data timestamp, `2026-10-01T20:22:06Z`. The curb ramp survey is older, mostly 2018, and its capture dates are not in the file; `ext:source_timestamp` on a ramp says when the survey was read. Up to v0.3.4 this field held the build time. |
| `pipelineVersion` | The software that made the file: `name` (`opensidewalks-nyc`), `version`, `url` (the repository at the build commit), `gitSHA` and `builtAt`, the UTC time of the build |
| `region` | MultiPolygon: union of the five NYC borough boundaries |

The validator input is the split form of the same data: `nyc.nodes.geojson`, `nyc.edges.geojson` and, from v0.3.5, `nyc.zones.geojson`, zipped as `nyc-osw-osw-split.zip`.

## Feature types

The schema infers each entity type from its geometry type and its identifying fields, such as `highway`, `footway`, `service`, `barrier` and `kerb`.

### Edges (LineStrings)

| OSW type | `highway` | `footway` |
|---|---|---|
| **Sidewalk** | `footway` | `sidewalk` |
| **Crossing** | `footway` | `crossing` |
| **Footway** | `footway` | (absent) |
| **Pedestrian Road** | `pedestrian` | (absent) |
| **Steps** | `steps` | (absent) |
| **Motor vehicle roads and Living Street** | `residential`, `service`, `tertiary`, `secondary`, `primary`, `unclassified`, `living_street` | (absent) |

Steps is its own entity in the schema, identified by `highway=steps`. OSM `path` ways, which have no schema entity, are written as `highway=footway` with `ext:osm_highway=path`. Linear OSM `highway=pedestrian` ways (pedestrian streets) keep `highway=pedestrian` and are the schema's Pedestrian Road. v0.3.5 has 11,402 Pedestrian Road Edges. Plazas and other pedestrian areas are Pedestrian Zones, not Edges (see [Zones](#zones-polygons)). A pedestrian way tagged `footway=sidewalk` or `footway=crossing` stays a Sidewalk or Crossing. The routing layer walks a Pedestrian Road like a Footway. `cycleway` and `track` ways that OSM tags as open to walkers (`foot=yes`, `designated` or `permissive`) are written as `highway=footway` and keep `ext:osm_highway`. `service=*` subtags are not kept, so Driveway, Alley and Parking Aisle appear as Service Road.

Every OSW edge is directional. The schema lets a consumer infer reverse edges, but this dataset already stores every segment as two edges, one per direction, each with its own `incline` sign. Do not add reverse edges when you load it. Unweaver adds them anyway, which leaves parallel duplicates that do not change route lengths. Street edges are stored both ways too, because OSM's `oneway` binds vehicles and a person walks along a one-way street either way.

Edge properties:

| Property | Type | Presence |
|---|---|---|
| `_id` | string | All edges. Stable coordinate-derived ID. |
| `_u_id` / `_v_id` | string | All edges. Endpoint node `_id`s; both resolve, and the edge's terminal coordinates equal the node coordinates exactly. |
| `highway` | enum | All edges |
| `footway` | enum | Sidewalks and crossings |
| `surface` | enum | Where OSM tags it. Non-canonical OSM values are mapped to the schema enum (`sett`, `cobblestone`, `stone` and `brick` become `paving_stones`; `wood` and `metal` become `paved`), and the OSM value is not kept. |
| `width` | float (m) | Sidewalks: from the OSM `width` tag where present, otherwise the mean width of the planimetric polygon (2 × area / perimeter). Other edges: from OSM where tagged. It is the mean width of the whole polygon, not the clear width at that spot. An OSM width of 0 or less is left off.
| `incline` | float | Where both endpoint heights are known. Signed rise/run in the direction u→v; values outside the OSW range [-1.0, 1.0] are dropped. The heights come from the LiDAR terrain model at 2 m, or on a bridge or elevated way from the LiDAR returns on the deck, and each node's height is averaged with its neighbours' along the path over edges shorter than 5 m before the difference is taken (see METHODOLOGY.md). Tunnel edges, and structure edges whose deck height could not be read, carry none. It is the mean grade between the two endpoints after smoothing, not the steepest point on the edge, and like other DEM-derived incline it can understate it. It is an estimate, not a measurement. It is not OSM's `incline` tag, which this pipeline does not read. Steps edges carry no `climb`.
| `name` | string | Where named in OSM |
| `crossing:markings` | enum | Crossings. From OSM's own `crossing:markings` tag where present: a value in the schema's enum is kept, a variant the schema does not list (`zebra:skewed`) becomes its base type, and a list of several marking types becomes `yes`. Otherwise from `crossing=*`, as the schema advises: `marked` and `zebra` give `yes`, `unmarked` gives `no`, and other values (`uncontrolled`, `traffic_signals`) give none. |
| `foot` | enum | Any edge where OSM's `foot` tag has one of the schema's values (`yes`, `no`, `designated`, `permissive`, `private`, `use_sidepath`, `destination`). Other values, such as `customers`, are left off. |

The schema adds `foot` so applications can warn before routing someone along a road. Most road edges have no `foot` tag. OSM's United States defaults allow walking on these road classes. A missing tag says nothing about whether the road has a sidewalk.

### Zones (Polygons)

A Pedestrian Zone is the schema's entity for "an area where pedestrians can travel freely in all directions". This dataset writes one for each OSM closed way tagged `area=yes` with `highway=pedestrian`, `footway` or `path`: a plaza, a square, a paved forecourt. v0.3.5 has 2,201 (1,936 from pedestrian areas, 262 from footway areas and 3 from path areas) zones. Up to v0.3.4 the outlines of these areas were Pedestrian Road and Footway Edges.

| Property | Notes |
|---|---|
| geometry | Polygon with one ring, the outline of the OSM way |
| `_id` | Stable ID derived from the geometry |
| `_w_id` | The `_id`s of the Nodes on the ring, in ring order, without the closing repeat. `_w_id[i]` is ring vertex `i`, and the vertex's coordinates equal that Node's coordinates exactly. Every listed Node is in the file. In the GeoJSON files it is an array of strings. FlatGeobuf has no list type, so in `nyc-osw.fgb` it is that array as JSON text in a string field; parse it with a JSON reader. |
| `highway` | `pedestrian` on every zone |
| `name`, `surface`, `foot` | From the OSM way where tagged, mapped as for Edges |
| `ext:osm_highway` | `footway` or `path` when the OSM area was tagged that way; absent for `highway=pedestrian` areas |
| `ext:osm_id`, `ext:borough`, `ext:source`, `ext:source_timestamp`, `ext:pipeline_version`, `ext:structure` | As on Edges |

A zone has no `_u_id` or `_v_id`, no `width` and no `incline`. In the schema, `_w_id` says that every pair of its Nodes is joined across the area. A consumer that builds a graph must add edges for the zone itself; reading only the LineStrings leaves each plaza as a gap. The Nodes on a ring are ordinary Nodes: some are also the ends of Edges (the zone's entrances) and some are on the ring only.

The GraphML files, the routing JSON and this project's routing layer already hold each zone as edges: the ring, between consecutive `_w_id` Nodes, and a straight chord between every two entrances whose chord stays inside the polygon. Each of those edges is stored in both directions and carries `ext:zone`, the zone's `_id`. `ext:zone` does not appear in the GeoJSON or the FlatGeobuf. The chords are this project's reading of a zone, not part of the schema.

An OSM pedestrian area whose way does not close into one valid ring stays Edges along its outline. A pedestrian area tagged `footway=sidewalk` or `footway=crossing` stays a Sidewalk or Crossing.

### Nodes (Points)

All nodes carry `_id`. A node with nothing else is a Bare Node. OSM node tags are not carried: OSM `kerb`, `tactile_paving`, `highway=elevator` and crossing nodes are absent, and every Curb Ramp comes from the DOT survey. Curb Ramp nodes additionally carry:

| Property | Notes |
|---|---|
| `barrier` | `kerb` |
| `kerb` | `lowered` for every surveyed ramp. `ufzp-rrqu` has no ramp type column. DOT's cut-through ramps, which have no ramp run, may be flush curbs in schema terms and are left as `lowered`. |
| `tactile_paving` | `yes` when the DOT survey recorded a detectable warning surface in any condition (Good Condition, and also the 2,021 Defective and 83 Off Ramp records), `no` when it recorded the surface as missing. `yes` does not mean the surface works; `ext:dws_condition` has DOT's value. |
| `ext:dws_condition` | DOT's raw `DWS_CONDITIONS` value, unchanged: `Missing`, `Good Condition`, `Defective`, `Not Applicable`, `Off Ramp - Good` or `Off Ramp-Defective` |
| `ext:running_slope_pct` | Measured ramp running slope, percent, signed from the road to the landing |
| `ext:cross_slope_pct` | Measured ramp cross slope, percent, signed left to right facing the ramp from the road |
| `ext:counter_slope_pct` | Measured slope of the street at the ramp, percent, signed from the road to the landing |
| `ext:ramp_id`, `ext:corner_id` | DOT survey identifiers |
| `ext:street_1`, `ext:street_2` | Cross streets at the ramp corner |

The slopes are signed, so compare magnitudes. They come from DOT's survey, which a contractor (Cyclomedia) collected for DOT from vehicle-mounted street-level imagery and LiDAR. The records were captured from March 2017 to January 2020, 216,220 of the 217,679 (99.3%) in 2018, and are not updated when a ramp is rebuilt. DOT says the measurements do not establish ADA compliance. A counter slope with a magnitude over 100% is not a gutter slope and is left off as unmeasured (six survey values, from -300% to 473%).

A ramp is attached to the graph when its node is an edge endpoint. The schema maps curbs at edge endpoints and expects a Footway between a Sidewalk and a Crossing, with the curb where that Footway meets the Crossing. This dataset follows OSM's geometry, which joins many Crossings directly to Sidewalks (in Manhattan, 52% of the nodes on a Crossing also touch a Sidewalk, measured on v0.3.4), so many ramps sit on that junction or on a Sidewalk vertex. A node holds one ramp's fields, so where several surveyed ramps land on one node the others are kept as separate nodes at their surveyed position, on no edge. 36,180 Curb Ramp nodes are on no edge and no zone ring, either for that reason or because no pedestrian vertex lies within 5 m.

Nodes carry `ext:elevation_m` (metres NAVD88, rounded to 0.1 m): the LiDAR terrain model interpolated between pixel centres of a 2 m tile, or on a bridge or elevated way the height of the deck read from LiDAR returns, in which case `ext:elevation_source` says which survey (`lidar_2017`, `lidar_2014`) or `interpolated` for a covered span. A node on a structure whose deck could not be read has no elevation.

## Extensions: `ext:*`

The OSW v0.3 schema allows arbitrary `ext:`-prefixed properties on every feature type (`patternProperties: ^ext:.*$`). Consumers that don't recognize an `ext:*` field can ignore it.

| Extension | On | Why |
|---|---|---|
| `ext:source` / `ext:pipeline_version` | every feature | Provenance, required by repo policy |
| `ext:source_timestamp` | every edge and zone; every Curb Ramp node | Retrieval time of the source. OSM-derived nodes without a ramp do not carry it. A ramp that sits on an edge shares its node with an OSM vertex. From v0.3.5 that node carries `ext:source=nyc_dot_ramps` and the survey's `ext:source_timestamp`, because every value on it except its position comes from the DOT survey. Its position is the OSM vertex, and `ext:ramp_id` traces it to the survey record. Up to v0.3.4 such a node said `ext:source=osm_walk` and had no timestamp. |
| `ext:osm_id` | OSM-derived edges and zones | Provenance back to the OSM way (a stringified ID, or list of IDs for merged ways) |
| `ext:osm_highway` | edges from an OSM `path`, or from a `cycleway` or `track` open to walkers; zones from a `footway` or `path` area | What OSM called the way. The edge itself is `highway=footway`, and the zone is `highway=pedestrian`. In NYC parks a `path` is often an unpaved trail. |
| `ext:borough` | every feature | `MN` / `BK` / `QN` / `BX` / `SI`, for filtering and per-borough splits. An OSM edge or node carries the borough whose cut of the graph it came from, so a segment on a bridge belongs to one of its two boroughs. |
| `ext:elevation_m` | nodes | Absolute elevation for accessibility analysis |
| `ext:elevation_source` | nodes on or beside a structure | `lidar_2017`, `lidar_2014` or `interpolated`: the height is the deck's, not the terrain model's. Absent where the terrain model stands. |
| `ext:structure` | edges | `bridge` or `tunnel` from the OSM tags; `elevated` for a way with `layer` above 0 and no bridge tag (a plaza over a road, a deck on a building). Bridge and elevated edges take their incline from deck heights; tunnel edges have none. |
| `ext:running_slope_pct`, `ext:cross_slope_pct`, `ext:counter_slope_pct` | curb nodes | Surveyed slope from NYC DOT, in percent (DOT's native unit) |
| `ext:dws_condition` | curb nodes | DOT's raw detectable warning surface condition, kept beside `tactile_paving` because `yes` covers defective and misplaced surfaces |
| `ext:ramp_id`, `ext:corner_id`, `ext:street_1`, `ext:street_2` | curb nodes | Traceability back to the DOT survey record |

## Sentinel values

The DOT data dictionary does not define `999`, `888`, `777` or `555`. This project reads them as no measurement because they fall far outside the physical range and recur together on the same rows. Sentinel values are omitted from the artifact, not carried or nulled (the validator rejects null-valued `ext:*` tags).

## Known deviations from the schema

- Many Crossings join Sidewalks directly, with no Footway between them (see Nodes).
- OSM node tags (`kerb`, elevators) are not carried.
- Both directions of every segment are stored; do not add reverse edges.
- 36,180 Curb Ramp nodes are on no edge and no zone ring.
- A Pedestrian Zone has one outer ring and no interior detail, and an OSM pedestrian area that does not close into one valid ring stays Edges along its outline.

## Validation

The pipeline's Stage 5 is an internal pre-check (structural integrity over all features, JSON Schema over a sample). The release gate is `python-osw-validation`, from the Taskar Center, run against the split ZIP:

```python
from python_osw_validation import OSWValidation
r = OSWValidation("output/nyc-osw-osw-split.zip").validate()
assert r.is_valid and not (r.errors or [])
```

Run it in its own environment (`uv run --no-project --isolated --with python-osw-validation ...`): the validator pins `geopandas==0.14.4`, which the pipeline cannot run on.

A release ships only when this returns zero errors. That is a check of form (schema, IDs, references, geometry validity, endpoint coordinates, coordinate precision), not of whether the attributes are true. See `validators/QUALITY_REPORT.md` for the current result and the audit behind it.

## Cross-references

- OpenSidewalks Schema repo: https://github.com/OpenSidewalks/OpenSidewalks-Schema
- AccessMap, the Taskar Center's routing application, which reads OSW data: https://github.com/TaskarCenterAtUW/AccessMap
- Unweaver (Nick Bolten, Apache-2.0), the routing engine behind AccessMap; this project's wheelchair profile is adapted from its example: https://github.com/nbolten/unweaver
- OSW spec rationale (Bolten et al., 2022): https://escholarship.org/uc/item/9920w8j7
