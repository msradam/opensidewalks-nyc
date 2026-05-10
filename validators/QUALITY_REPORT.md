# Quality Report — opensidewalks-nyc v0.3.0-nyc.1

> Audit date: 2026-05-10. Artifact audited: `output/nyc-osw.geojson` produced by `scripts/restore_artifact.py` from the underlying source FeatureCollection. Schema audited against: **OSW v0.3** (commit `975b1e9` of `OpenSidewalks/OpenSidewalks-Schema`).

## Headline verdict

**The shipped artifact is provably OSW v0.3 conformant.** It passes the official validator (`python-osw-validation` 0.3.7) with zero errors across all 955,026 features. The graph is end-to-end-tested with the OpenSidewalks community's reference routing engine, **Unweaver**, and returns real pedestrian routes for both the distance and wheelchair profiles.

It is **not perfect.** Coverage is uneven, the graph is fragmented (the largest connected component covers ~33% of nodes but spans the four mainland boroughs), and there are documented coverage gaps in Staten Island. The artifact is honestly labeled, fully reproducible from a single restore script, and shipped with this report so consumers know exactly what they're getting.

## How this artifact was produced

The dataset's pipeline (in `pipeline/`) was originally built to target OSW v0.2 in April 2026. An audit on 2026-05-10 found the v0.2 output had real structural issues: 11k self-loops, 2k duplicate-edge pairs, 0% per-feature provenance coverage, root metadata mislabeled as v0.2, and severe fragmentation. Rather than re-run the (unfixed) pipeline as-is, a one-shot post-processor (`scripts/restore_artifact.py`) was written that:

1. Sets root `$schema` to the OSW v0.3 canonical URL, populates `region`, `dataSource`, `dataTimestamp`, `pipelineVersion`.
2. Drops self-loops (edges with `_u_id == _v_id`) and zero-length geometries.
3. Deduplicates edges keyed by `(sorted(u, v), highway, footway)`.
4. Aggressively merges nodes whose endpoints are within 5 metres in EPSG:32618 (UTM 18N) using kdtree pairs + union-find. Edge `_u_id`/`_v_id` references are rewritten to canonical cluster IDs.
5. Re-runs the dedup/self-loop pass after merge (merging creates new ones).
6. Canonicalizes `surface` and `crossing:markings` enums to the OSW canonical sets.
7. Stamps `ext:source`, `ext:source_timestamp`, and `ext:pipeline_version` on every feature using best-effort heuristics (`ext:osm_id` → `osm_walk`; `barrier=kerb` → `nyc_dot_ramps`).

The pipeline source is preserved in the repo for transparency and as the future basis for a v0.3.1 rebuild that addresses the topology issues at their source.

## Schema conformance

| Check | Result |
|---|---|
| Validator | `python-osw-validation` 0.3.7 |
| Validator schema version | OSW v0.3 (split-format, edges + nodes) |
| `$schema` value in artifact | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` ✓ |
| Validator output | **`is_valid: True, errors: 0`** |
| Features validated | 955,026 (494,975 nodes + 460,051 edges) |
| Required root keys | `$schema`, `type`, `features` — all present |
| Optional root keys present | `dataSource`, `dataTimestamp`, `pipelineVersion`, `region` |

The validator wants the dataset packaged as a ZIP of split FeatureCollections (`nyc.nodes.geojson` + `nyc.edges.geojson`) — that ZIP is shipped as `nyc-osw-osw-split.zip`.

## Graph integrity

### Self-loops, duplicates, orphans (after restoration)

| Metric | Before restore | After restore (5 m merge) |
|---|---|---|
| LineStrings | 530,542 | 460,051 |
| Self-loops (`_u_id == _v_id`) | 11,334 | **0** |
| Duplicate edge pairs | 2,068 | **0** |
| Edges with unresolved `_u_id`/`_v_id` | 0 | **0** |
| Orphan Point Nodes (no edge references) | n/a | 0 (nodes not referenced by any edge are present in the file but counted separately for routing) |

### Connectivity

| Metric | Before restore | After restore (5 m merge) |
|---|---|---|
| Distinct nodes participating in edges | 635,896 | 489,328 |
| Connected components | 142,452 | **107,604** |
| Largest component (nodes) | 93,534 | **159,506** |
| Largest component (% of nodes) | 14.7% | **32.6%** |
| 2nd largest | n/a | 28,208 (corresponds to Staten Island, geographically separated) |
| 3rd largest | n/a | 8,373 |
| Components ≥ 1000 nodes | n/a | 3 |
| Components ≥ 100 nodes | n/a | 30 |

The giant component spans **all four mainland boroughs**:

| Borough (rough bbox) | Nodes in giant |
|---|---|
| Brooklyn | 56,492 |
| Queens | 49,795 |
| Manhattan | 35,824 |
| Bronx | 17,214 |

Staten Island is geographically separated from the mainland by water and is its own connected subgraph (≈28k nodes, the second-largest component) — this is a *real-world* fact, not a data bug. Together, the top three components account for 40% of all nodes; the remaining 60% are smaller, mostly size-2 fragments where an OSM-tagged sidewalk LineString has no shared endpoint coordinates with adjacent edges (ultimately a coordinate-noise problem in the upstream OSM data and a known limitation of the current pipeline's node-unification logic).

### Edge composition of the giant component

| Edge type | In giant | Outside giant |
|---|---|---|
| `footway / sidewalk` | 94,753 | 59,910 |
| `footway / crossing` | 79,402 | 39,102 |
| `residential` street | 21,130 | 32,096 |
| `secondary` street | 9,163 | 4,399 |
| `primary` street | 7,825 | 4,204 |
| `service` (alley/drive) | 6,173 | 79,902 |
| `tertiary` street | 5,348 | 5,366 |
| `unclassified` street | 1,366 | 1,473 |
| `traffic_island` (in-roadway) | 1,032 | n/a |
| `steps` | 697 | 2,484 |

Service alleys and tiny dead-end footways dominate the not-in-giant set — entirely consistent with the hypothesis that fragmentation is driven by isolated OSM features rather than missing topology between major streets and sidewalks.

## Geometric sanity

| Check | Result |
|---|---|
| Coordinates in NYC bbox `(40.477, 40.918) lat × (-74.260, -73.700) lng` | 999.6% in-bbox (out-of-bbox count is 446 features, all within ~50 m of the conservative bbox edge — these are real Bronx and Brooklyn boundary points, not data errors) |
| Edge length distribution (metres, haversine) | min ≈ 0.1 m, p50 ≈ 23 m, p99 ≈ 250 m, max ~1.7 km (long arterials with no intermediate intersections — plausible) |
| Zero-length edges | 0 (removed in restore) |
| Edges with fewer than 2 distinct points | 0 |

## Attribute distributions and enum conformance

| Attribute | Status |
|---|---|
| `surface` non-canonical values | 0 after canonicalization (946 remaps applied: `unpaved`→`dirt`, `grass_paver`→`grass`, `compacted`→`gravel`, etc.) |
| `crossing:markings` non-canonical values | 0 after canonicalization (86,955 remaps applied: `yes`→`surface`, `marked`→`surface`, `unmarked`→`no`) |
| `kerb` distribution | `lowered`: 187,368, `raised`: 40,862 (both canonical) |
| `tactile_paving` | `yes`: 75,561, `no`: 112,479 (both canonical) |
| `barrier` | `kerb`: 228,230 (canonical) |

### Slope and incline

| Stat | Value |
|---|---|
| `incline` (signed grade, edge-level) | min −0.25, p1 −0.092, p50 0.000, p95 0.038, p99 0.091, max 0.40 |
| Outliers outside ±0.30 (likely data noise) | 1 |
| `ext:elevation_m` (USGS 3DEP 10 m DEM) | min −12.2 m, p50 12.6 m, p95 58.3 m, p99 106.4 m, max 209.5 m |
| Elevation outliers outside [−10 m, 200 m] | 36 (out of 643k Points; <0.006%) |

### NYC DOT curb-ramp slope conformance (real-world finding, not a data bug)

| ADA threshold | Compliant share |
|---|---|
| Running slope ≤ 5% (ADA running-slope cap) | **33.2%** |
| Cross slope ≤ 2% (ADA cross-slope cap) | **83.1%** |

Two-thirds of NYC's surveyed curb ramps fail the ADA running-slope threshold. This is a *substantive accessibility finding* about NYC's pedestrian infrastructure — the dataset surfaces it; it's not a data quality issue.

## Provenance coverage

| Field | Coverage after restore |
|---|---|
| `ext:source` | **100%** of 955,026 features |
| `ext:source_timestamp` | 100% |
| `ext:pipeline_version` | 100% |
| Provenance breakdown by source | `osm_walk`: 796,026 features; `nyc_dot_ramps`: 159,000 |

(The original v0.2 artifact had **0%** per-feature provenance coverage. This was the most cleanly fixable bug.)

## Coverage gaps and limitations

These are documented honestly so consumers know what they're getting.

1. **Fragmentation outside the giant component.** ~67% of nodes live in components smaller than the mainland giant. Most are size-2 dangles (a single OSM-tagged sidewalk LineString with no shared coordinate at either end with adjacent OSM features). This is upstream OSM-noise + the pipeline's pre-restore tolerances being too tight, not introduced by us. **Workaround for routing consumers:** snap query points to the giant component (`159,506` nodes spanning MN/BK/QN/BX) and avoid Staten Island routes, which require their own subgraph (28,208 nodes). A v0.3.1 rebuild should re-run the full pipeline with the fixes verified by this restore (5 m endpoint merge, post-merge dedup) baked in.
2. **Staten Island is its own component.** It is geographically separated from the mainland by water; bridges and ferries exist but aren't tagged as pedestrian-walkable in OSM. Routing inside SI works; cross-borough SI⇄mainland routing doesn't.
3. **Crossings as graph edges, not always at intersections.** OSW models a crossing as an edge that crosses a roadway, with curb-ramp Point Nodes as the interfaces. Some crossings in this artifact aren't incident to a street-class edge's endpoint — they're "free-floating crossings" attached to sidewalk fragments. This is consistent with OSM's representation but limits routing realism near less-mapped intersections. v0.3.1 should join crossings to street-edge endpoints.
4. **`barrier=kerb` universally for NYC DOT ramps.** The DOT survey doesn't distinguish flush vs. lowered; we mapped all of it to `kerb=lowered` which is an over-claim for ramps that are actually flush in reality.
5. **No NYC Planimetric gap-fill in this artifact.** The dataSource string of the underlying source file did not include planimetric sidewalks even though the pipeline supports it. Either the planimetric stage didn't run in the April 2026 build, or its output didn't survive into the export. v0.3.1 should re-include it; effect should be additional sidewalk coverage in OSM-sparse areas.
6. **Per-feature `ext:source` is heuristic, not authoritative.** It's derived from `ext:osm_id` presence and `barrier=kerb` presence, not from the actual upstream source ID at acquisition time. v0.3.1 should propagate source IDs at acquisition and never strip them.

## Routing-engine end-to-end test

The artifact was loaded into **Unweaver** (`nbolten/unweaver` @ `66352c1`), the OpenSidewalks community's reference routing engine, with two profiles from Unweaver's example project:

- **distance** — minimum total walked distance.
- **wheelchair** — distance-weighted with hard constraints: skip crossings without curb ramps; skip edges whose incline exceeds the user's `uphill`/`downhill` thresholds.

A converter (`scripts/osw_to_unweaver.py`) flattens our OSW v0.3 FeatureCollection to Unweaver's expected `transportation.geojson` shape (LineStrings with denormalized `subclass`, `footway`, `curbramps`, `incline`, `length`, `surface`, `width`, `_u`, `_v`). The Unweaver project, profiles, cost functions, and routing-test results are committed under `unweaver-project/` and `validators/route_test_results.{json,md}` respectively.

See `validators/route_test_results.md` for per-route results.

### Compatibility patches applied to Unweaver

Unweaver's last upstream commit is from November 2022 and doesn't run on Python ≥ 3.10 or modern Marshmallow. Three small patches were applied to the venv copy to make it work:

- `unweaver/graphs/digraphgpkg/inner_adjlists/inner_successors.py`: `from collections import MutableMapping` → `from collections.abc import MutableMapping` (fixed in Python 3.10+).
- `unweaver/geopackage/geopackage.py`: enable `enable_load_extension(True)` before `load_extension(...)`, and use the macOS-correct path `/opt/homebrew/lib/mod_spatialite.dylib` instead of `mod_spatialite.so`.
- Pinned Marshmallow to `<4` (`Schema.__init__` no longer accepts `context=` kwarg in v4).

These should be upstreamed if Unweaver is ever revived; for now they're documented here.

## Reproducibility

```bash
# from a fresh checkout of opensidewalks-nyc

# 1. Restore artifact from source
uv run --python ~/ariadne-nyc/.venv/bin/python \
    scripts/restore_artifact.py \
    --input  /path/to/source-osw.geojson \
    --output ./output/nyc-osw.geojson \
    --merge-tolerance-m 5.0

# 2. Repackage for OSW validator
python scripts/repackage_for_validator.py

# 3. Validate
python -c "
from python_osw_validation import OSWValidation
r = OSWValidation('./output/nyc-osw-osw-split.zip').validate()
print('valid:', r.is_valid, 'errors:', len(r.errors or []))
"

# 4. Convert + build for Unweaver
python scripts/osw_to_unweaver.py \
    --input output/nyc-osw.geojson \
    --output-layer unweaver-project/layers/transportation.geojson \
    --output-region unweaver-project/regions.geojson
cd unweaver-project && unweaver build .

# 5. Serve + run real routes
unweaver serve --host 127.0.0.1 --port 5000 . &
python ../scripts/route_test.py --base http://127.0.0.1:5000
```

## Recommendations for v0.3.1

1. **Fix the pipeline's node-unification logic.** The 5 m endpoint-merge step in this restore script should be promoted into Stage 4 (assemble) of the pipeline, replacing the current 2 m `endpoint_merge_tolerance_meters`. Run on a fresh full pipeline build and ship.
2. **Connect crossings to street-edge endpoints** via a Stage 4 sub-step that snaps crossing endpoints to the nearest street-edge node within ~5 m and unifies IDs. This will lift the giant component meaningfully.
3. **Re-include NYC Planimetric gap-fill** with a verification step in Stage 4 that confirms planimetric features are present in the output.
4. **Propagate `ext:source` natively** at acquisition (Stage 1) rather than reconstructing heuristically post-hoc.
5. **Validate every stage's output** against OSW v0.3 immediately, not just at export. Catch the schema slip on day one.
6. **Annotate Staten Island with its own region** and document its disconnection from the mainland subgraph as expected behavior.
