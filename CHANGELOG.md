# Changelog

Every published release of the dataset, newest first. Each version has a full note in [`release-notes/`](release-notes/) and its evidence in [`evaluation/`](evaluation/). The version number is this dataset's own; every release uses the OpenSidewalks Schema v0.3. v0.3.2 and v0.3.5 were internal builds and were not released.

## v0.3.7-nyc.1 (2026-10-06)

- A street edge carries what OpenStreetMap says about its sidewalks in `ext:sidewalk` (`both`, `left`, `right`, `yes`, `no` or `separate`), folded from `sidewalk=*` or the per-side `sidewalk:left`, `sidewalk:right` and `sidewalk:both` tags, whose raw values are kept beside it. The wheelchair profile walks a street tagged as having a sidewalk. The street stays a street edge, not a sidewalk.
- A surveyed curb ramp snaps to the end of the crossing it serves when one is within 5 m, instead of to the nearest vertex of any kind.
- OpenStreetMap's own kerb nodes are carried: `barrier=kerb` with `kerb=lowered`, `raised`, `flush` or `rolled`, and `tactile_paving`. Where a surveyed ramp sits on one, the node carries the survey's values and OpenStreetMap's stay beside them in `ext:osm_kerb` and `ext:osm_tactile_paving`. Elevator nodes carry `ext:osm_highway=elevator`, and the edges at one have no incline and no mark, so a route can change level there.
- A cycleway or track with no `foot` tag is kept, as OpenStreetMap's access defaults for the United States say, unless it is a one-way cycleway (a bike lane beside the roadway). Up to v0.3.6 such ways were left out, among them several greenways.
- The GraphML root holds the OpenStreetMap extract's details as JSON, and the routing JSON's description names the project.
- CONTRIBUTING, an issue template for data errors and this changelog.

## v0.3.6-nyc.1 (2026-10-05)

- Plazas and other pedestrian areas (2,201) are Pedestrian Zones, Polygons with `_w_id`, instead of Edges along their outlines. The GraphML, the routing JSON and the routing layer expand each zone into its ring and the chords between its entrances.
- The root `dataTimestamp` is the OpenStreetMap extract's data time; the build time is `pipelineVersion.builtAt`.
- A curb ramp on an OpenStreetMap vertex says `ext:source=nyc_dot_ramps`. An OpenStreetMap `path` keeps `ext:osm_highway=path`.
- A terrain pixel that holds no data is no longer read as ground at 0 m, and a node inside a tunnel has no height.
- An edge whose two end heights give a grade at stair pitch or steeper has no `incline` and carries `ext:incline_unknown=yes`; the wheelchair profile refuses it.
- Every GraphML edge carries `length_m`. `evaluation-sheets.zip` is listed in `SHA256SUMS`.
- Carries the changes of the internal v0.3.5 build.

## v0.3.4-nyc.1 (2026-10-04)

- `crossing:markings` from OpenStreetMap's own tag, falling back to `crossing=*` as the schema advises.
- Linear `highway=pedestrian` ways are Pedestrian Road Edges.
- `foot` on any edge where OpenStreetMap's tag has one of the schema's values, and `ext:dws_condition` with DOT's raw warning surface value on every curb ramp.
- Geometry and incline unchanged from v0.3.3.

## v0.3.3-nyc.1 (2026-10-03)

- First public release: sidewalks, crossings, footways, steps and streets from a dated Geofabrik extract of OpenStreetMap, 217,679 curb ramps from the NYC DOT survey, incline from the 2017 LiDAR terrain model with deck heights from the point clouds on bridges, and sidewalk widths from the city's planimetric polygons.
- Passes `python-osw-validation` 0.5.0 with zero errors.
