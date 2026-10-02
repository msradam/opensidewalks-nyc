# Schema reference

This dataset conforms to **[OpenSidewalks Schema v0.3](https://sidewalks.washington.edu/opensidewalks/0.3/schema.json)**. This document is a quick reference for consumers describing the properties actually present in the artifact. Read the upstream spec for the authoritative type definitions.

## Top-level

A single GeoJSON `FeatureCollection` (`output/nyc-osw.geojson`). Root metadata:

| Field | Value |
|---|---|
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` |
| `dataSource` | Name and URL of this pipeline |
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
| **Footway** (other) | `footway`, `pedestrian`, `steps` | (absent) |
| **Street** | `residential`, `service`, `tertiary`, `secondary`, `primary`, `unclassified`, etc. | (absent) |

Edge properties:

| Property | Type | Presence |
|---|---|---|
| `_id` | string | All edges. Stable coordinate-derived ID. |
| `_u_id` / `_v_id` | string | All edges. Endpoint node `_id`s; both resolve, and the edge's terminal coordinates equal the node coordinates exactly. |
| `highway` | enum | All edges |
| `footway` | enum | Sidewalks and crossings |
| `surface` | enum | Where OSM tags it. OSW canonical values; non-canonical OSM values are mapped. |
| `width` | float (m) | Sidewalks: from the OSM `width` tag where present, otherwise the mean width of the planimetric polygon (2 × area / perimeter). Other edges: from OSM where tagged. Overstated in v0.3.1-nyc.1. |
| `incline` | float | Where both endpoint elevations were sampled from the LiDAR DTM. Signed rise/run in the direction u→v; values outside the OSW range [-1.0, 1.0] are dropped. Mostly a spurious 0 in v0.3.1-nyc.1. |
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

Nodes whose elevation was sampled from the LiDAR DTM carry `ext:elevation_m` (metres, rounded to 0.1 m). In v0.3.1-nyc.1 a value of exactly 0.0 outside the Bronx is a sampling error, not an elevation.

## Extensions: `ext:*`

The OSW v0.3 schema allows arbitrary `ext:`-prefixed properties on every feature type (`patternProperties: ^ext:.*$`). Consumers that don't recognize an `ext:*` field can ignore it.

| Extension | On | Why |
|---|---|---|
| `ext:source` / `ext:pipeline_version` | every feature | Provenance, required by repo policy |
| `ext:source_timestamp` | every edge; nodes from the DOT survey and injected nodes | Retrieval time of the source. OSM-derived nodes do not carry it. |
| `ext:osm_id` | OSM-derived edges | Provenance back to the OSM way (a stringified ID, or list of IDs for merged ways) |
| `ext:borough` | nearly every feature | `MN` / `BK` / `QN` / `BX` / `SI`, for filtering and per-borough splits. In v0.3.1-nyc.1 the gap-fill edges and their nodes lack it. |
| `ext:elevation_m` | nodes | Absolute elevation for accessibility analysis |
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
