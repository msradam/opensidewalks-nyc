# CLAUDE.md — opensidewalks-nyc

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

- Latest validator as of 2026-07-03 is **0.4.4** (July 1, 2026). Confirm the
  current version on PyPI and pin it; the 0.4.x line is where the strict checks
  live. The validator caps reported errors at 20, so "20 errors" means "at least
  20", not "almost done".
- The bundled schema is still **OSW v0.3** (`OpenSidewalks/OpenSidewalks-Schema`
  latest tag `0.3`, Jan 2026; no v0.4 exists). The schema target is current.
- The validator input is a ZIP of `nyc.nodes.geojson` + `nyc.edges.geojson`, not
  the merged FeatureCollection.

## Reproduce from scratch → conformant

```bash
# 0. Environment (Python >= 3.11). rasterio (incline) is a normal project dep.
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
uv pip install "python-osw-validation==0.4.4"   # not a project dep; install explicitly
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

# 3. Validate against the real validator. THIS is the conformance gate.
python -c "from python_osw_validation import OSWValidation as V; \
r=V('output/nyc-osw-osw-split.zip').validate(); \
print('is_valid', r.is_valid, 'errors', len(r.errors or []))"
#   REQUIRE: is_valid True, errors 0
```

## Why `python -m pipeline build` alone is not conformant

Two gaps mean the built-in pipeline reports success on an artifact the official
validator rejects. Know them before trusting the pipeline's own output.

1. **Stage 4 (`pipeline/stages/assemble.py`) does not snap edge vertices.** It
   merges near-coincident endpoints by remapping each edge's `_u_id`/`_v_id` to a
   canonical node ID (`_merge_near_endpoints`), but leaves the edge's terminal
   vertex at its original coordinate. `python-osw-validation` 0.4.0+ checks that
   every edge start/end coordinate equals the coordinate of the node it
   references, so those sub-metre gaps fail. `scripts/snap_endpoints.py` (step 2
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

- `pipeline/` — the six-stage build. Entry: `python -m pipeline {build,validate,clean}`.
  Stages: `acquire` (1), `clean` (2), `schema_map` (3), `assemble` (4),
  `validate` (5, home-grown), `export` (6).
- `config/build.yaml` — tunable thresholds: `osw_schema_version: 0.3`,
  `snap_tolerance_meters`, `endpoint_merge_tolerance_meters`, the optional
  `study_area` bbox, output toggles.
- `config/sources.yaml` — declarative source manifest (IDs, licenses, retrieval).
- `scripts/snap_endpoints.py` — the mandatory post-build endpoint snap + ZIP emit.
- `scripts/restore_artifact.py` — older one-shot post-processor that produced the
  shipped artifact from an external source FeatureCollection. That source
  (`macadam-nyc/opensidewalks_nyc.geojson`) is gone, so it is not the from-scratch
  path; use the pipeline + `snap_endpoints.py` instead.
- `scripts/{osw_to_unweaver,route_test,to_flatgeobuf,to_graphml,split_by_borough}.py`
  — export/routing helpers.
- `validators/QUALITY_REPORT.md` — conformance + quality writeup for the current
  artifact; regenerate its numbers with `validators/quality_audit.py`.
- `release-assets/` — GitHub release asset staging (gitignored; rebuild with
  to_flatgeobuf / split_by_borough / gzip from `output/`).

## Data sources (all re-acquired by Stage 1)

OSM walk network via OSMnx (ODbL-1.0); NYC DOT Pedestrian Ramps (`ufzp-rrqu`);
NYC Planimetric Sidewalks (`52n9-sdep`); Borough Boundaries (`7t3b-ywvw`); NYC
2017 LiDAR DEM (NY State ArcGIS ImageServer, for incline); NYC Address Points
(`g6pj-hd8k`); MTA ADA stations (`drh3-e2fd` / GTFS fallback).

## Known state and gotchas (2026-07-03, v0.3.1-nyc.1)

- **The from-scratch path works.** Build + snap + validate produces
  `is_valid True, errors 0` under 0.4.4. Stage 4 drops edges the 2 m endpoint
  merge collapses into zero-length self-loops (they fail shapely validity in
  0.4.x); do not "fix" that drop away.
- **Borough codes are normalized to `MN`/`BK`/`QN`/`BX`/`SI`** at the end of
  Stage 3. Upstream sources tag boroughs three different ways; consumers
  (`split_by_borough.py`, `quality_audit.py`) expect the codes.
- **pandas 3 hazard:** `groupby(...).apply()` excludes the grouping column from
  the groups. The node dedup in `assemble.py` restores `_id` via a plain
  `reset_index()`; a `drop=True` there silently produces a node-less artifact.
- **Incline needs `rasterio`** (a project dep since v0.3.1). If it is missing,
  Stage 4 skips incline with a warning instead of failing; the artifact still
  validates but loses grade data.
- **Socrata borough-boundaries dataset `7t3b-ywvw` returns 404** (since ~mid
  2026); Stage 1 falls back to OSMnx geocoding automatically.
- **`SOCRATA_APP_TOKEN`** is optional but strongly recommended for city-wide
  builds (anonymous access is rate-limited to ~1 req/s).

## Conventions

- Python env is `uv` only (`uv venv`, `uv pip install`, `uv run`). Do not use
  `pip`/`venv` directly. Ask before creating a `.venv` if one is absent.
- Run the deterministic quality passes on changed code: `ruff format .`,
  `ruff check --fix .`, then any configured type-check/tests. Do not report a
  task done with a failing pass.
- Do not add AI co-authorship. No `Co-Authored-By: Claude` trailers, no
  "Generated with Claude Code" footers, no AI listed as author/contributor in
  commits, PRs, or release notes. The LLM-assisted disclosure lives once in
  `NOTICE`; leave it there.
- Commit or push only when asked. If on `main`, branch first.
