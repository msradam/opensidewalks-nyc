# Unweaver / OpenSidewalks Ecosystem Research

Researched 2026-05-10 against live GitHub APIs and PyPI. URLs and SHAs verified at time of writing.

## TL;DR

- **Canonical Unweaver repo is `nbolten/unweaver`** (not under `AccessMap` or `TaskarCenterAtUW` as primaries). The TaskarCenterAtUW org has a fork (`TaskarCenterAtUW/unweaver`) but it is exactly the same commit set; AccessMap's own README explicitly links to `nbolten/unweaver` as "our flexible routing engine".
- **Unweaver is effectively unmaintained.** Last commit `66352c1` on 2022-11-02. 13 open issues (mostly Dependabot bumps). Python 3.8+, Apache-2.0, never released to PyPI; install is `pip install git+https://github.com/nbolten/unweaver.git@<sha>`.
- **Unweaver has no awareness of OSW v0.3.** It predates the v0.3 spec (released 2026-01-27) by 3+ years. It does not validate against an OSW schema at all; it ingests any LineString GeoJSON via Fiona/GDAL and copies properties verbatim onto edge columns. Cost functions are user-supplied Python that read OSM-style keys (`footway`, `curbramps`, `incline`, `length`).
- **The published canonical schema URL `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` is broken (HTTP 404).** That URL is the literal `$id` inside the schema, but the host returns a WordPress 404. Use `https://raw.githubusercontent.com/OpenSidewalks/OpenSidewalks-Schema/main/opensidewalks.schema.json` instead (or pin to tag `0.3`, commit `975b1e9e156ac2ebdf2a9422f7f4dce5bef158ae`).
- **Authoritative validator is `python-osw-validation` on PyPI (latest 0.3.7).** It validates a ZIP of split files (`*.edges.geojson`, `*.nodes.geojson`, `*.points.geojson`, `*.lines.geojson`, `*.polygons.geojson`, `*.zones.geojson`) against per-entity sub-schemas, not the monolithic FeatureCollection schema.

## Unweaver: install, input, query, cost function

### Repository facts

- URL: https://github.com/nbolten/unweaver
- Mirror/fork: https://github.com/TaskarCenterAtUW/unweaver (no divergent commits)
- Language: Python (Poetry, declares `python = "^3.8"` in `pyproject.toml` — `^3.8` will exclude Python 3.12+ unless you relax it)
- License: Apache-2.0 (LICENSE file says Apache-2.0; GitHub API reports "NOASSERTION" because the file lacks a SPDX header)
- Last commit: `66352c1` 2022-11-02 ("Added stricter mypy settings")
- Status: not archived, but stale — 4 years no commits, 13 open issues, no PyPI release. Treat as a research artifact, not a supported product.
- Stars/forks: 8 / 4

### Native dependencies (macOS / Apple Silicon)

Unweaver requires SQLite with extension loading enabled, SpatiaLite, GDAL, and proj. The macOS system Python's stdlib `sqlite3` is built **without** extension support, so SpatiaLite cannot load. You must use a Python rebuilt against Homebrew SQLite.

```bash
brew install sqlite libspatialite gdal proj
# Rebuild a Python that links Homebrew sqlite (pyenv example):
LDFLAGS="-L$(brew --prefix sqlite)/lib" \
CPPFLAGS="-I$(brew --prefix sqlite)/include" \
PYTHON_CONFIGURE_OPTS="--enable-loadable-sqlite-extensions" \
pyenv install 3.11.9
```

Then in your project:

```bash
uv venv --python 3.11    # must be the pyenv build above
uv pip install "git+https://github.com/nbolten/unweaver.git@66352c1#egg=unweaver"
```

Pitfalls:
- `Fiona = "^1.8.20"` and `shapely = "^1.6"` in `pyproject.toml` are old; on Apple Silicon you may need `uv pip install fiona shapely --no-binary :all:` linked against Homebrew GDAL/GEOS. Or relax pins via a fork.
- `osm-humanized-opening-hours = "^0.6.2"` may not have wheels for 3.12; stay on 3.11.
- The `pyproject.toml` constraint `python = "^3.8"` means **`<4,>=3.8` per Poetry semantics, but actually `>=3.8,<4`** — works for 3.11. If you need 3.12, fork and bump.

### Input format

Unweaver does **not** consume OSW directly as a graph. Pipeline:

1. Place LineString GeoJSON files in `<project>/layers/`.
2. `unweaver build <project>` reads them with Fiona and writes a routable SpatiaLite GeoPackage at `<project>/graph.gpkg`. Every property on input features becomes a column on the `edges` table. Geometry is decomposed into `_u`/`_v` node IDs (digraph; no multidigraph).
3. `unweaver weight <project>` precomputes static edge weights for each profile that has one.
4. `unweaver serve <project>` launches a Flask server with endpoints `/shortest_path/<profile>.json`, `/shortest_path_tree/<profile>.json`, `/reachable_tree/<profile>.json`.

Implication for OSW v0.3: **you must split or flatten the FeatureCollection before feeding Unweaver.** Unweaver wants a *transportation network* layer of LineStrings (sidewalks, crossings, footways). The OSW v0.3 mixed FeatureCollection (Nodes + Edges + Zones + Points + Lines + Polygons) will not work as-is. AccessMap's deployment expects a `transportation.geojson` (edges-only) plus a `regions.geojson` — confirmed in `TaskarCenterAtUW/AccessMap` README.

### Cost function and wheelchair profile

Profiles live in the project directory, not in the package. Example `example/profile-wheelchair.json`:

```json
{
  "id": "wheelchair",
  "args": [
    {"name": "avoidCurbs", "type": "fields.Boolean()"},
    {"name": "uphill",     "type": "fields.Number(validate=validate.Range(0, 10))"},
    {"name": "downhill",   "type": "fields.Number(validate=validate.Range(-15, 0))"}
  ],
  "cost_function": "cost-wheelchair.py",
  "shortest_path": "shortest-path-wheelchair.py"
}
```

The cost function reads OSM-flavored keys directly off edge properties:

```python
def cost_fun_generator(G, avoidCurbs=True, uphill=0.083, downhill=-0.1):
    def cost_fun(u, v, d):
        if d["footway"] == "crossing" and not d["curbramps"]:
            return None
        if d["incline"] is not None and (d["incline"] > uphill or d["incline"] < downhill):
            return None
        return d["length"] if d["length"] is not None else 0
    return cost_fun
```

So Unweaver consumes whatever attributes you put on your input GeoJSON. To run the stock wheelchair profile against OSW v0.3 data you need each edge to carry `footway`, `curbramps` (boolean), `incline` (numeric), `length` (numeric). In OSW v0.3 some of these are differently named (e.g., curb ramp presence is modeled as a connected `CurbRamp` node, not an edge boolean) — see Compatibility Verdict.

### Query API

```bash
curl "http://localhost:5000/shortest_path/wheelchair.json?lon1=-122.33&lat1=47.60&lon2=-122.31&lat2=47.61&avoidCurbs=true&uphill=0.083&downhill=-0.1"
```

Response is GeoJSON. There is also a Python library API; entry points in `unweaver/cli.py`, `unweaver/shortest_paths/`.

## AccessMap: relationship, repos, reference datasets

- `https://github.com/TaskarCenterAtUW/AccessMap` — orchestration repo (docker-compose, env config). Confirms Unweaver as the routing engine and points to `opensidewalks-data` for input data generation.
- `https://github.com/AccessMap/accessmap-incremental` — full bottom-up pipeline (OSM → DEM → OSW → routable) using a `osm_osw` Python package and Snakemake. This is the closest thing to a "build OSW for a new region" tutorial. Note: README says "this project is a tech demo and reproducing its functionality is difficult at this time."
- `https://github.com/OpenSidewalks/opensidewalks-data` — the canonical multi-city builder. Last push 2023-12-29; per-city Snakefiles under `cities/<region>/`. Probable templates: Seattle and others. Outputs `transportation.geojson` + `regions.geojson`.
- `https://github.com/AccessMap/accessmap` — the deployment repo (web app + routing wiring). Independent of `TaskarCenterAtUW/AccessMap` despite the name overlap.

No public CDN of pre-built OSW v0.3 reference datasets was found via the GitHub search; releases are empty on `opensidewalks-data`. AccessMap's production data is generated at deploy time. The closest thing to a reference is whatever ships from `accessmap-incremental` if you run its docker-compose pipeline against Seattle.

## OSW v0.3 schema: official URL, structural requirements, validator

### The schema file

- Repo: `https://github.com/OpenSidewalks/OpenSidewalks-Schema`
- Tag `0.3`: commit `975b1e9e156ac2ebdf2a9422f7f4dce5bef158ae`, released 2026-01-27
- Authoritative file path in repo: `opensidewalks.schema.json` (root, not `schemas/`)
- Stable raw URL: `https://raw.githubusercontent.com/OpenSidewalks/OpenSidewalks-Schema/main/opensidewalks.schema.json`
- Internal `$id`: `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` — **THIS HOST RETURNS HTTP 404**, do not depend on it for `$ref` resolution.
- Meta-schema: `http://json-schema.org/draft-07/schema#`
- Size: 278,540 bytes
- SHA-256 (main, today): `0bd9b2f70ff42c5cd49d35d7a3efff238b0fa5a7a72fe4f8bce5c1376cd442e2`
- File blob SHA reported by GitHub contents API: `8109310900abbb058dd62f7052c019efb99b659d`

### Top-level structural requirements

```
type: object
required: ["$schema", "features", "type"]
properties: $schema, dataSource, dataTimestamp, features, pipelineVersion, region, type
```

Notable: **`region` is NOT in `required`.** It is declared as `GeoJSON.MultiPolygon` if present, but the schema does not force it. AccessMap's deployment wants a separate `regions.geojson` file regardless, so functionally you need it — but a strictly-valid OSW v0.3 file can omit it.

### Canonical enums (verified by inspecting `definitions/`)

- `surface` (e.g., on `AlleyFields`, similarly on Sidewalk/Footway): `["asphalt", "concrete", "dirt", "grass", "grass_paver", "gravel", "paved", "paving_stones", "unpaved"]`
- `crossing:markings` (on `CrossingFields`): `["dashes", "dots", "ladder", "ladder:paired", "ladder:skewed", "lines", "lines:paired", "lines:rainbow", "no", "pictograms", "rainbow", "skewed", "surface", "yes", "zebra", "zebra:bicolour", "zebra:double", "zebra:paired", "zebra:rainbow"]`
- `kerb`: split per node type — `CurbRampFields.kerb = ["lowered"]`, `FlushCurbFields.kerb = ["flush"]`, `RaisedCurbFields.kerb = ["raised"]`, `RolledCurbFields.kerb = ["rolled"]`. There is no single `kerb` enum across all node types; the value is bound to the specific subtype.
- `tactile_paving` (on `CurbRampFields`): `["contrasted", "no", "primitive", "yes"]`

The `definitions` block has 90 entries. The schema is composed of `<Entity>` + `<Entity>Fields` pairs (e.g., `Crossing` + `CrossingFields`), with `Custom*` entities new in v0.3.

### Validator

- PyPI: `python-osw-validation`, latest `0.3.7`, requires Python >=3.10. Install:
  ```
  uv pip install "python-osw-validation==0.3.7"
  ```
- API: `OSWValidation(zipfile_path=...).validate()`
- **Important:** the validator does NOT accept the monolithic FeatureCollection. It expects a ZIP containing files whose names end in `.edges.geojson`, `.nodes.geojson`, `.points.geojson`, `.lines.geojson`, `.polygons.geojson`, `.zones.geojson`. This is the TDEI dataset packaging convention. Plan to emit your data both as a unified FeatureCollection (for Unweaver/AccessMap) and as the split-file ZIP (for the validator).
- Per-file schemas live under `schemas/jsonschema/` in the schema repo and can be passed via `schema_paths={...}` to override.

## Compatibility verdict

**Can a v0.3-conformant OSW artifact be loaded by current Unweaver? Partially, with a translation step. Not natively.**

- Unweaver does no schema enforcement. If you hand it a LineString-only GeoJSON with the right property names, it routes. So you can write a thin adapter in your pipeline that, from the v0.3 dataset, emits `transportation.geojson` containing only Edges (Sidewalks, Crossings, Footways, etc.) with **denormalized** OSM-like keys: `footway`, `length` (meters), `incline` (rise/run), `surface`, and a synthesized `curbramps` boolean per edge derived from connected CurbRamp nodes.
- The mismatch list:
  1. v0.3 models curb ramps as Nodes connected to Edge endpoints; Unweaver's stock wheelchair cost expects a per-edge `curbramps` bool. You must walk the topology and project node-attribute presence onto edges.
  2. v0.3 splits `kerb` into 4 node subtypes; Unweaver doesn't read it. Either add a derived edge property or extend the cost function.
  3. v0.3 `surface` enums and OSM `surface` enums are similar but not identical (v0.3 adds `grass_paver`; missing some OSM values like `metal`, `wood`). If your cost function whitelists OSM surfaces, audit it.
  4. v0.3 root requires `$schema`, `features`, `type`; Unweaver doesn't care, but the validator will reject if `$schema` is missing.
- Net: keep your v0.3 artifact as the source of truth. Generate two derived outputs: (a) split-file ZIP for `python-osw-validation`, (b) flattened `transportation.geojson` for Unweaver.

## Pitfalls + workarounds

1. **`$id` URL is dead.** Don't use `https://sidewalks.washington.edu/...` in tooling. Resolve `$ref`s locally or rewrite them. The TCAT WordPress site returns 404 for that path. Cache the schema in the repo (already in `validators/schema-cache/`).
2. **Unweaver Python pin (`^3.8`) and Fiona `^1.8.20` will fight modern toolchains.** Pin your venv to Python 3.11 with `--enable-loadable-sqlite-extensions`. Apple Silicon stock `python.org` builds and `pyenv install` defaults will silently ship without sqlite extension support; SpatiaLite will fail to load at `unweaver build` time with an opaque "not authorized" error. The README's macOS troubleshooting section is correct but easy to skip.
3. **Unweaver expects a digraph.** If your OSW v0.3 graph has parallel edges between the same node pair (rare but possible at multi-track crossings), Unweaver will silently drop one. Multidigraph support is open issue #10, never implemented.
4. **Unweaver's `weight` step bakes in static costs per profile.** For a wheelchair profile parameterized by user (`uphill`, `downhill`, `avoidCurbs`), costs are recomputed per-request — fine, but the static `weight` pass uses defaults; verify your route times.
5. **Validator wants a ZIP of split files, not a FeatureCollection.** This is the most common build mistake. Write a splitter early. The naming convention is suffix-based (`*.edges.geojson` etc.).
6. **`python-osw-validation` truncates errors to first 20 by default.** Pass `max_errors=10000` while iterating, or you'll fix one bug and not see the next 19,000.
7. **AccessMap deployment wants `transportation.geojson` + `regions.geojson` separately.** The OSW `region` MultiPolygon is optional in the schema but required by the AccessMap docker-compose. Emit both.
8. **No PyPI release of Unweaver.** Pin a commit SHA in your `pyproject.toml` (`66352c1` is the current tip). Reproducible builds will break if you use a branch name and someone force-pushes (low risk on a stale repo, but pin anyway).
9. **License ambiguity on Unweaver.** GitHub reports "NOASSERTION" because the LICENSE file lacks SPDX headers, but the file itself is verbatim Apache-2.0. Document which version you vendor.
10. **AccessMap's `accessmap-incremental` README explicitly warns it is hard to reproduce.** Don't assume you can lift its docker-compose wholesale; expect to rebuild the OSM → OSW pipeline yourself for NYC.

## Key URLs

- Unweaver: https://github.com/nbolten/unweaver  (commit `66352c1`)
- Unweaver docs site: https://unweaver.org (mkdocs build, may be stale)
- OSW Schema repo: https://github.com/OpenSidewalks/OpenSidewalks-Schema  (tag `0.3` = `975b1e9`)
- OSW Schema raw: https://raw.githubusercontent.com/OpenSidewalks/OpenSidewalks-Schema/main/opensidewalks.schema.json
- Validator repo: https://github.com/TaskarCenterAtUW/TDEI-python-lib-osw-validation
- Validator on PyPI: https://pypi.org/project/python-osw-validation/  (0.3.7)
- AccessMap deployment: https://github.com/TaskarCenterAtUW/AccessMap
- AccessMap incremental: https://github.com/AccessMap/accessmap-incremental
- OSW data builder (multi-city): https://github.com/OpenSidewalks/opensidewalks-data
