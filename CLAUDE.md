# CLAUDE.md: opensidewalks-nyc

Guidance for an agent opening this repo fresh. Goal: reproduce, from scratch, an
OpenSidewalks (OSW) v0.3 pedestrian graph of NYC that passes the **current**
official validator.

## The acceptance gate (this is "the current requirement")

The artifact is conformant if, and only if, the OSW split ZIP passes
`python-osw-validation` at its latest release with zero errors:

```python
from python_osw_validation import OSWValidation
r = OSWValidation("output/nyc-osw-osw-split.zip").validate()
assert r.is_valid and not (r.errors or []), r.errors[:20]
```

- Latest validator as of 2026-10-02 is **0.5.0** (August 5, 2026). Confirm the
  current version on PyPI; 0.4.0 added the endpoint-coordinate check and 0.5.0
  added a 7-decimal coordinate limit. The published v0.3.1-nyc.1 files pass
  0.4.4 and 0.4.5 and fail 0.5.0; a build from current code passes 0.5.0. The
  validator caps reported errors at 20, so "20 errors" means "at least 20", not
  "almost done".
- Passing the validator is a check of form. It does not look at connectivity or
  at whether attribute values are true; v0.3.1-nyc.1 passed 0.4.4 with 80% of
  its elevations wrong. Check the things in "Checks the validator does not do".
- The validator pins `geopandas==0.14.4`. Never install it into the pipeline's
  environment (Stage 1 then fails on `union_all`); run it with
  `uv run --no-project --isolated --with python-osw-validation`.
- The bundled schema is still **OSW v0.3** (`OpenSidewalks/OpenSidewalks-Schema`
  latest tag `0.3`, Jan 2026; no v0.4 exists). The schema target is current.
- The validator input is a ZIP of `nyc.nodes.geojson` + `nyc.edges.geojson`, not
  the merged FeatureCollection.

## Reproduce from scratch → conformant

```bash
# 0. Environment (Python >= 3.11). rasterio (incline) is a normal project dep.
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
export SOCRATA_APP_TOKEN=...                     # optional: NYC Open Data 1 req/s -> 1000 req/s

# 1. Build. Re-acquires OSM + NYC Open Data and assembles the graph.
#    City-wide is ~60-90 min and ~10 GB scratch. For fast iteration, uncomment
#    the study_area bbox block in config/build.yaml first (then `pipeline clean`).
python -m pipeline build
#   -> output/nyc-osw.geojson  (+ nyc.graphml, nyc-routing.json)

# 2. Snap edge endpoints to their referenced node coordinates. REQUIRED for
#    conformance (see "Why build alone is not conformant" below). Stdlib only,
#    idempotent. Rewrites the GeoJSON and emits the validator inputs.
python scripts/snap_endpoints.py --input output/nyc-osw.geojson
#   -> output/osw-split/nyc.nodes.geojson, output/osw-split/nyc.edges.geojson,
#      output/nyc-osw-osw-split.zip

# 3. Validate against the real validator, in its own environment. THIS is the
#    conformance gate.
uv run --no-project --isolated --with python-osw-validation python -c "from python_osw_validation import OSWValidation as V; \
r=V('output/nyc-osw-osw-split.zip').validate(); \
print('is_valid', r.is_valid, 'errors', len(r.errors or []))"
#   REQUIRE: is_valid True, errors 0

# 4. The checks the validator does not do. Read the JSON; see the section below
#    for what each number should look like.
python validators/post_build_checks.py output/nyc-osw.geojson output/post_build_checks.json
```

## Why `python -m pipeline build` alone is not conformant

Two gaps mean the built-in pipeline reports success on an artifact the official
validator rejects. Know them before trusting the pipeline's own output.

1. **Stage 4 (`pipeline/stages/assemble.py`) does not snap edge vertices.** It
   merges near-coincident endpoints by remapping each edge's `_u_id`/`_v_id` to a
   canonical node ID (`_merge_near_endpoints`), but leaves the edge's terminal
   vertex at its original coordinate. `python-osw-validation` 0.4.0+ checks that
   every edge start/end coordinate equals the coordinate of the node it
   references, so those gaps (up to 2 m, on the few hundred endpoints the merge moves) fail. `scripts/snap_endpoints.py` (step 2
   above) closes them by moving each edge endpoint onto its node's coordinate.

2. **Stage 5 (`pipeline/stages/validate.py`) is not the official validator.** It
   is a home-grown `jsonschema` Draft7 check plus structural checks, running on a
   2,000-feature sample. It does not implement the geometry-to-node coordinate
   check, so `python -m pipeline validate` can report conformance while the
   artifact fails `python-osw-validation`. Use the PyPI validator as the gate.

**Durable fix (recommended, if hardening the pipeline):** fold the
`snap_endpoints.py` logic into Stage 4 so `assemble.py` writes coordinate-identical
edge endpoints and nodes, and replace Stage 5 with a call to
`python-osw-validation` against the split ZIP. Then `build` alone is conformant.
`validators/QUALITY_REPORT.md` records the same recommendation.

## Repo map

- `pipeline/`: the six-stage build. Entry: `python -m pipeline {build,validate,clean}`.
  Stages: `acquire` (1), `clean` (2), `schema_map` (3), `assemble` (4),
  `validate` (5, home-grown), `export` (6).
- `config/build.yaml`: tunable thresholds: `osw_schema_version: 0.3`,
  `snap_tolerance_meters`, `endpoint_merge_tolerance_meters`, the optional
  `study_area` bbox, output toggles.
- `config/sources.yaml`: declarative source manifest (IDs, licenses, retrieval).
- `scripts/snap_endpoints.py`: the mandatory post-build endpoint snap + ZIP emit.
- `scripts/restore_artifact.py`: older one-shot post-processor that produced the
  shipped artifact from an external source FeatureCollection. That source
  (`macadam-nyc/opensidewalks_nyc.geojson`) is gone, so it is not the from-scratch
  path; use the pipeline + `snap_endpoints.py` instead.
- `scripts/{to_flatgeobuf,to_graphml,to_routing_json,split_by_borough}.py`:
  release asset makers. Run them on the snapped GeoJSON; `scripts/README.md`
  has the exact commands. Stage 6's own GraphML and routing JSON predate the
  snap and are not the release assets.
- `scripts/{osw_to_unweaver,route_test}.py`: routing layer (with the 5 m ramp
  rule, `crossings_with_ramps`) and route test (17 landmark pairs, 7 across
  structures).
- `pipeline/utils/ept.py`, `pipeline/utils/deck.py`: the LiDAR point cloud
  reader and the deck height rules; `assemble._structure_elevations` wires them
  in, `assemble._smoothed_for_incline` smooths short edges.
- `validators/post_build_checks.py`: the checks the validator does not do.
- `tests/`: `test_validity_fixes.py`, `test_structure_incline.py`,
  `test_crossing_rule.py`; run each with `python tests/<file>`.
- `validators/QUALITY_REPORT.md`: conformance + quality writeup for the current
  artifact. Its numbers come from `post_build_checks.py`.
- `release-notes/`: one file per release, what changed and why.
- `release-assets/`: GitHub release asset staging (gitignored).

## Data sources (all re-acquired by Stage 1)

OSM walk network from one dated Geofabrik extract, pinned by URL and SHA-256 in
`config/sources.yaml` and built into a graph with OSMnx (ODbL-1.0; no Overpass
query); NYC DOT Pedestrian Ramps (`ufzp-rrqu`);
NYC Planimetric Sidewalks (`52n9-sdep`); Borough Boundaries (`7t3b-ywvw`); NYC
2017 LiDAR DEM (NY State ArcGIS ImageServer, 2 m tiles, for incline); LiDAR
point clouds (NOAA Digital Coast Entwine tiles, 2017 NYC and 2014 USGS, fetched
by Stage 4 around structures into `data/raw/lidar_points/`); NYC Address Points
(`g6pj-hd8k`); MTA ADA stations (`drh3-e2fd` / GTFS fallback).

## Known state and gotchas (2026-10-03, v0.3.3-nyc.1)

- **The from-scratch path works city-wide in one run.** Build + snap + validate
  produces `is_valid True, errors 0` under 0.5.0 on 4,068,058 features. It
  takes about 48 min and peaks at 35 GB of memory footprint on a
  32 GB machine, so close other heavy programs first.
- **Stage 4 downloads during the build:** the LiDAR tiles around structures
  (about 2 GB from a public S3 bucket, with retries). Offline, structure edges
  get no incline and the build still finishes.
- **The terrain tiles come as 2,048 px squares.** Larger requests come back as
  HTML error pages with status 200; Stage 1 opens each download with rasterio
  before keeping it.
- **Socrata pages can come back 500;** Stage 1 retries a page five times.
- **Moving to newer OSM data** means changing `extract_url` and
  `extract_sha256` together. Stage 1 refuses a file whose checksum differs.
  The extract covers New York State, so a study-area box that reaches into New
  Jersey gets no New Jersey streets.
- **The OSM tag filter is one line in `sources.yaml` on purpose.** Folded over
  two lines, YAML put a space in the regex and no secondary road matched.
  `foot=yes/designated/permissive` overrides `access=no/private` (the
  Queensboro walkway needs it), and a cycleway or track is kept only with one
  of those foot values (bridge paths need it).
- **Socrata paging must stay ordered.** `_socrata_fetch_all` orders by `:id`
  and compares the distinct row count with `count(*)`. Without `$order` a
  Staten Island pull returned 6,662 ramps twice and missed 6,662.
- **The endpoint merge moves only dead ends and component bridges,** nearest
  first, never more than the tolerance. Do not go back to uniting every pair
  within tolerance: the graph keeps every OSM vertex as a node, so that chains
  (33 m moves, a quarter of edges dropped). The decision and its measurements
  are in the v0.3.2 release notes.
- **Borough codes are `MN`/`BK`/`QN`/`BX`/`SI`** on staged features (end of
  Stage 3) and on OSM nodes (Stage 4, `borough_code`).
- **pandas 3 hazard:** `groupby(...).apply()` excludes the grouping column from
  the groups. The node dedup in `assemble.py` restores `_id` via a plain
  `reset_index()`; a `drop=True` there silently produces a node-less artifact.
- **Incline needs `rasterio`** (and `laspy[lazrs]` for the point clouds). If
  rasterio is missing, Stage 4 skips incline with a warning instead of failing.
  Elevation is interpolated between pixel centres; the tiles have no nodata
  value, so a node is only sampled from a tile whose extent contains it. Bridge
  and elevated edges take deck heights from the point clouds; tunnel edges get
  no incline. Node heights are smoothed along the path over edges under 5 m
  before incline is taken (the written `ext:elevation_m` is not smoothed).
- **The deck rules have corner cases.** Plazas over roads are unclassified in
  the survey (flat and dense counts as solid); sidewalks under elevated
  railways stay on the ground; a bridge tower top is not a deck; a ramp on an
  embankment is followed down until LiDAR and the terrain model agree. Each has
  a test in `tests/test_structure_incline.py`; add one before changing a rule.
- **Socrata borough-boundaries dataset `7t3b-ywvw` returns 404;** Stage 1 falls
  back to OSMnx geocoding (Nominatim). The replacement IDs are `gthc-hcne`
  (shoreline) and `wh2p-dxnf` (water included); only the water-included
  polygons contain the bridges.
- **`drh3-e2fd` is not an MTA dataset** (it is planimetric hydrography) and the
  GTFS fallback has no `wheelchair_boarding` column, so no ADA station index is
  produced. The live table is `39hk-dx4f` on data.ny.gov.
- **The DOT ramp survey is signed and has four sentinel codes** (555, 777, 888,
  999). Compare slope magnitudes. The curb ramp running-slope limit is 1:12
  (8.33%), not 5%. `DWS_CONDITIONS` is "Missing" on 59% of ramps.
- **A scipy sparse matrix adds duplicate entries.** Building a graph from this
  file's edges (one per direction, some parallel) without reducing to one
  weight per node pair doubles every distance.
- **`SOCRATA_APP_TOKEN`** is optional (anonymous access was fast enough for the
  city-wide build: 217,679 ramps in under 2 minutes).

## Checks the validator does not do

`validators/post_build_checks.py` runs these and writes one JSON. Each caught a
defect in v0.3.1-nyc.1 that passed the validator. What v0.3.3 looks like:

- `elevation`: nodes at exactly 0.0 are 0.04% (at most 0.12% in a borough), and
  the Staten Island maximum is 122.5 m. A large zero share means the tile
  sampling broke.
- `components`: Brooklyn, Manhattan, Queens and the Bronx each have 84% to 96%
  of their pedestrian nodes in the largest component; Staten Island has none
  (no walkable link). A borough at 0% that is not Staten Island is cut off.
- `curb`: every surveyed ramp is in the file, `tactile_paving` agrees with
  `DWS_CONDITIONS` on all of them, 83.5% are an edge endpoint, no sentinel
  slopes.
- `width`: sidewalk median 3.1 m; 0.94 of a polygon transect at the median.
  v0.3.1's 5.65 m was the ring-polygon bug.
- `gap_fill`: 0 edges in the graph; the sidecar has about 2,320.
- `structure`: 14,402 nodes with an `ext:elevation_source`, 48
  structure nodes without a height, 1,708 edges touching a deck node
  that read steeper than 15% over 3 m or more (most are station entrances OSM
  joins to the sidewalk without steps).
- `nodes_not_on_any_edge_by_source`: only `nyc_dot_ramps`.
- `one_direction_only`: empty.
- `form`: no coordinate over 7 decimals, no edge end off its node.
- A component count cannot show a cut bridge. Test bridges end to end with
  `research_notes/next/routing/bridges.py` (landfalls on the walkways' own
  landings); 20 of 23 are joined on pedestrian edges in v0.3.3.
- Deck heights: `research_notes/next/structure/validate.py` against the 2014
  survey, `where_check.py` against the planimetric structure polygons.

## Conventions

- Python env is `uv` only (`uv venv`, `uv pip install`, `uv run`). Do not use
  `pip`/`venv` directly. Ask before creating a `.venv` if one is absent.
- Run the deterministic quality passes on changed code: `ruff format .`,
  `ruff check --fix .`, then `python tests/test_validity_fixes.py` and any
  configured type-check. (The tree is not ruff-clean yet: as of 2026-10-02
  `ruff format .` would rewrite about 1,300 lines in the pipeline files, so
  check the files you touch and do not let a reformat bury a fix.) Do not report a
  task done with a failing pass.
- Do not add AI co-authorship. No `Co-Authored-By: Claude` trailers, no
  "Generated with Claude Code" footers, no AI listed as author/contributor in
  commits, PRs, or release notes. The LLM-assisted disclosure lives once in
  `NOTICE`; leave it there.
- Commit or push only when asked. If on `main`, branch first.
