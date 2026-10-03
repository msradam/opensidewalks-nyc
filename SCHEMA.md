# Schema reference

This dataset conforms to **[OpenSidewalks Schema v0.3](https://sidewalks.washington.edu/opensidewalks/0.3/schema.json)**. This document is a quick reference for consumers describing the properties actually present in the artifact. Read the upstream spec for the authoritative type definitions.

## Top-level

A single GeoJSON `FeatureCollection` (`output/nyc-osw.geojson`). Root metadata:

| Field | Value |
|---|---|
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` |
| `dataSource` | Name and URL of this pipeline, plus `license` (`ODbL-1.0`), `licenseUrl`, `attribution` (the credit line to reuse) and `osmExtract` (`url`, `sha256` and `dataTimestamp` of the OSM extract the build read). Absent before v0.3.2. |
| `dataTimestamp` | Build-time UTC timestamp |
| `pipelineVersion` | `{version, gitSHA, builtAt}` for the producing build |
| `region` | MultiPolygon: union of the five NYC borough boundaries |

The validator input is the split form of the same data: `nyc.nodes.geojson` + `nyc.edges.geojson` zipped as `nyc-osw-osw-split.zip`.

## Feature types

OSW discriminates types by `geometry.type` plus `properties.highway` (and `properties.footway`).

### Edges (LineStrings)

| OSW type | `highway` | `footway` |
|---|---|---|
| **Sidewalk** | `footway` | `sidewalk` |
| **Crossing** | `footway` | `crossing` |
| **Footway** (other) | `footway`, `steps` | (absent) |
| **Street** | `residential`, `service`, `tertiary`, `secondary`, `primary`, `unclassified`, `living_street` | (absent) |

OSM `path` and `pedestrian` ways are written as `highway=footway`. So are `cycleway` and `track` ways that OSM tags as open to walkers (`foot=yes`, `designated` or `permissive`); those keep `ext:osm_highway`. Secondary roads are present from v0.3.2 (a filter typo left them out before).

Every edge is directed. A segment is two edges, one per direction, each with its own `incline` sign. From v0.3.3 that holds for street edges too: OSM's `oneway` binds vehicles, and a person walks along a one-way street either way. In v0.3.2 a one-way street was a single edge; before v0.3.2 so was a one-way pedestrian way.

Edge properties:

| Property | Type | Presence |
|---|---|---|
| `_id` | string | All edges. Stable coordinate-derived ID. |
| `_u_id` / `_v_id` | string | All edges. Endpoint node `_id`s; both resolve, and the edge's terminal coordinates equal the node coordinates exactly. |
| `highway` | enum | All edges |
| `footway` | enum | Sidewalks and crossings |
| `surface` | enum | Where OSM tags it. OSW canonical values; non-canonical OSM values are mapped. |
| `width` | float (m) | Sidewalks: from the OSM `width` tag where present, otherwise the mean width of the planimetric polygon (2 × area / perimeter). Other edges: from OSM where tagged. It is the mean width of the whole polygon, not the clear width at that spot. Overstated in v0.3.1-nyc.1. |
| `incline` | float | Where both endpoint heights are known. Signed rise/run in the direction u→v; values outside the OSW range [-1.0, 1.0] are dropped. From v0.3.3 the heights come from the LiDAR terrain model at 2 m, or on a bridge or elevated way from the LiDAR returns on the deck, and each node's height is averaged with its neighbours' along the path over edges shorter than 5 m before the difference is taken (see METHODOLOGY.md). Tunnel edges, and structure edges whose deck height could not be read, carry none. In v0.3.2 bridge edges carried none; mostly a spurious 0 in v0.3.1-nyc.1. |
| `name` | string | Where named in OSM |
| `crossing:markings` | enum | Crossings, where OSM has a `crossing=*` tag. Mapped to the OSW canonical set. |

### Nodes (Points)

All nodes carry `_id`. Curb-ramp nodes (from the NYC DOT survey) additionally carry:

| Property | Notes |
|---|---|
| `barrier` | `kerb` |
| `kerb` | `lowered` (the DOT survey does not distinguish flush ramps) |
| `tactile_paving` | `yes` when the DOT survey recorded a detectable warning surface (good, defective or off the ramp), `no` when it recorded the surface as missing. In v0.3.1-nyc.1 the value is `yes` on every curb node and cannot be trusted. |
| `ext:running_slope_pct` | Measured ramp running slope, percent, signed from the road to the landing |
| `ext:cross_slope_pct` | Measured ramp cross slope, percent, signed left to right facing the ramp from the road |
| `ext:counter_slope_pct` | Measured slope of the street at the ramp, percent, signed from the road to the landing |
| `ext:ramp_id`, `ext:corner_id` | DOT survey identifiers |
| `ext:street_1`, `ext:street_2` | Cross streets at the ramp corner |

The slopes are signed, so compare magnitudes. They were measured in 2017 to 2020 and are not updated when a ramp is rebuilt.

A ramp is attached to the graph when its node is an edge endpoint. A node holds one ramp's fields, so where several surveyed ramps land on one node the others are kept as separate nodes at their surveyed position, on no edge. Every surveyed ramp is in the file from v0.3.2.

Nodes carry `ext:elevation_m` (metres NAVD88, rounded to 0.1 m): the LiDAR terrain model interpolated between pixel centres of a 2 m tile (5 to 12 m in v0.3.2), or on a bridge or elevated way the height of the deck read from LiDAR returns, in which case `ext:elevation_source` says which survey (`lidar_2017`, `lidar_2014`) or `interpolated` for a covered span. A node on a structure whose deck could not be read has no elevation. In v0.3.1-nyc.1 a value of exactly 0.0 outside the Bronx is a sampling error, not an elevation; in v0.3.2 a node on a bridge has the height of the ground or water below.

## Extensions: `ext:*`

The OSW v0.3 schema allows arbitrary `ext:`-prefixed properties on every feature type (`patternProperties: ^ext:.*$`). Consumers that don't recognize an `ext:*` field can ignore it.

| Extension | On | Why |
|---|---|---|
| `ext:source` / `ext:pipeline_version` | every feature | Provenance, required by repo policy |
| `ext:source_timestamp` | every edge; nodes from the DOT survey and injected nodes | Retrieval time of the source. OSM-derived nodes do not carry it. |
| `ext:osm_id` | OSM-derived edges | Provenance back to the OSM way (a stringified ID, or list of IDs for merged ways) |
| `ext:osm_highway` | edges from an OSM `cycleway` or `track` open to walkers | What OSM called the way; the edge itself is `highway=footway` |
| `ext:borough` | nearly every feature | `MN` / `BK` / `QN` / `BX` / `SI`, for filtering and per-borough splits. An OSM edge or node carries the borough whose cut of the graph it came from, so a segment on a bridge belongs to one of its two boroughs. Nodes injected at the end of a gap-fill edge have none. In v0.3.1-nyc.1 the gap-fill edges and their nodes lack it. |
| `ext:elevation_m` | nodes | Absolute elevation for accessibility analysis |
| `ext:elevation_source` | nodes on or beside a structure | `lidar_2017`, `lidar_2014` or `interpolated`: the height is the deck's, not the terrain model's. Absent where the terrain model stands. |
| `ext:structure` | edges | `bridge` or `tunnel` from the OSM tags; `elevated` for a way with `layer` above 0 and no bridge tag (a plaza over a road, a deck on a building). Bridge and elevated edges take their incline from deck heights; tunnel edges have none. |
| `ext:running_slope_pct`, `ext:cross_slope_pct`, `ext:counter_slope_pct` | curb nodes | Surveyed slope from NYC DOT, in percent (DOT's native unit) |
| `ext:ramp_id`, `ext:corner_id`, `ext:street_1`, `ext:street_2` | curb nodes | Traceability back to the DOT survey record |

## Sentinel values

The DOT survey uses `999`, `888`, `777` and `555` where there is no measurement. Sentinel values are omitted from the artifact rather than carried or nulled (the validator rejects null-valued `ext:*` tags). v0.3.1-nyc.1 omits only `999`, so 829 curb nodes carry `888` or `777` as a slope.

## Validation

The pipeline's Stage 5 is an internal pre-check (structural integrity over all features, JSON Schema over a sample). The conformance gate is the official validator run against the split ZIP:

```python
from python_osw_validation import OSWValidation
r = OSWValidation("output/nyc-osw-osw-split.zip").validate()
assert r.is_valid and not (r.errors or [])
```

Run it in its own environment (`uv run --no-project --isolated --with python-osw-validation ...`): the validator pins `geopandas==0.14.4`, which the pipeline cannot run on.

A release ships only when this returns zero errors. That is a check of form (schema, IDs, references, geometry validity, endpoint coordinates, coordinate precision), not of whether the attributes are true. See `validators/QUALITY_REPORT.md` for the current result and the audit behind it.

## Cross-references

- OpenSidewalks Schema repo: https://github.com/OpenSidewalks/OpenSidewalks-Schema
- AccessMap (canonical OSW consumer): https://github.com/TaskarCenterAtUW/AccessMap
- OSW spec rationale (Bolten et al., 2022): https://escholarship.org/uc/item/9920w8j7
