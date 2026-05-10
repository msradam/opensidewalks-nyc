# Schema reference

This dataset conforms to **[OpenSidewalks Schema v0.3](https://sidewalks.washington.edu/opensidewalks/0.3/schema.json)**. This document is a quick reference for consumers — read the upstream spec for the authoritative definitions.

## Top-level

A single GeoJSON `FeatureCollection`. Root metadata:

| Field | Value |
|---|---|
| `region` | MultiPolygon: union of the five NYC borough boundaries |
| `ext:pipeline_version` | Set per-build (matches the git tag of `opensidewalks-nyc`) |
| `ext:source_timestamp_utc` | Build-time UTC timestamp |

## Feature types

OSW uses `geometry.type` + `properties.highway` (+ `properties.footway`) to discriminate types.

### Edges (LineStrings)

| OSW type | `highway` | `footway` |
|---|---|---|
| **Sidewalk** | `footway` | `sidewalk` |
| **Crossing** | `footway` | `crossing` |
| **Footway** (other) | `footway`, `path`, `pedestrian`, `steps` | (absent) |
| **Street** | `residential`, `service`, `tertiary`, `secondary`, `primary`, `unclassified`, etc. | (absent) |

All edges carry:

| Property | Type | Notes |
|---|---|---|
| `_id` | string | Stable feature ID, namespaced (e.g. `e_12345`) |
| `_u_id` / `_v_id` | string | Endpoint Node `_id`s. **Both must resolve.** |
| `highway` | enum | OSW canonical |
| `footway` | enum | Sidewalk/crossing only |
| `surface` | enum | OSW 9-value canonical (paved, asphalt, concrete, paving_stones, gravel, dirt, grass, wood, metal). Non-canonical OSM values mapped. |
| `incline` | float | Signed grade (rise/run). Sourced from USGS 3DEP 10 m DEM. |
| `width` | float (m) | Where surveyed |
| `name` | string | Where named |
| `ext:osm_id` | string | Provenance |
| `ext:borough` | enum | `MN` / `BK` / `QN` / `BX` / `SI` |
| `ext:source` | string | `osm` / `nyc_planimetric` / `nyc_dot` |
| `ext:source_timestamp` | ISO-8601 | Source-row timestamp |
| `ext:incline_source` | string | `usgs_3dep_10m` |

Sidewalk/Footway/Street additionally:

| Property | Type |
|---|---|
| `ext:running_slope_pct` | float |
| `ext:cross_slope_pct` | float |
| `ext:counter_slope_pct` | float |
| `ext:lit` | bool |
| `ext:ada_violations` | string \| array |

Crossing additionally:

| Property | Notes |
|---|---|
| `crossing:markings` | OSW canonical (`zebra`, `lines`, `dashes`, `surface`, `no`) |
| `ext:crossing` | OSM crossing context |
| `ext:tactile_paving` | bool |
| `ext:kerb` | enum: `lowered` / `flush` / `raised` |

### Nodes (Points)

All nodes carry `_id` and `geometry` (Point). Optional:

| Property | Notes |
|---|---|
| `ext:elevation_m` | USGS 3DEP 10 m, present on all OSM-derived nodes |
| `barrier` | `kerb` for curb interface nodes |
| `kerb` | `lowered` (DOT ramps) / `flush` / `raised` |
| `tactile_paving` | bool, set when DOT `dws_conditions` is non-empty |

## Extensions: `ext:*`

Anything prefixed `ext:` is an opensidewalks-nyc extension beyond OSW v0.3 core. The OSW spec explicitly allows arbitrary extension namespaces. Consumers that don't recognize an `ext:*` field can safely ignore it.

| Extension | Why |
|---|---|
| `ext:elevation_m` | Curb cut and crossing accessibility analysis often needs absolute elevation, not just per-edge incline. |
| `ext:running_slope_pct`, `ext:cross_slope_pct`, `ext:counter_slope_pct` | Surveyed slope from NYC DOT, in percent (DOT's native unit). |
| `ext:ada_violations` | Free-text or array of ADA-rule codes from DOT inspections. |
| `ext:borough` | NYC convenience for filtering / spatial joins. |
| `ext:osm_id` | Provenance back to OSM. |
| `ext:source` / `ext:source_timestamp` / `ext:pipeline_version` | Provenance triple — required by repo policy on every feature. |
| `ext:lit` | Lighting; OSM `lit=*` mapped to bool. |
| `ext:kerb` | Where OSW core distinguishes interfaces, this carries the surveyed kerb height class. |
| `ext:tactile_paving` | DWS (detectable warning surface) presence. |
| `ext:crossing` | Original OSM `crossing=*` value, retained alongside the canonical `crossing:markings`. |
| `ext:incline_source` | How `incline` was derived. |
| `ext:source_type` | For OSM-derived features: e.g. `link` for connecting footways without survey data. |

## Sentinel values

| Source | Sentinel | Replaced with |
|---|---|---|
| NYC DOT | `999.0` (unmeasurable) | `null` |
| OSM | empty string | `null` |

## Validation

The pipeline runs an OSW v0.3 schema validation step (`python -m pipeline validate`). The release ships with the validator output as `validators/validation-report.json`. Strict-mode failures block release; warnings are recorded.

## Cross-references

- OpenSidewalks Schema repo: https://github.com/taskar-center/opensidewalks-schema
- AccessMap (canonical OSW consumer): https://github.com/TaskarCenterAtUW/AccessMap
- OSW spec rationale (Bolten et al., 2022): https://escholarship.org/uc/item/9920w8j7
