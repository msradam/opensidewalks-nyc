# Quality Report: opensidewalks-nyc v0.3.1-nyc.1

> Audit date: 2026-07-03. Artifact audited: `output/nyc-osw.geojson`, built from scratch by `python -m pipeline build` plus the post-build endpoint snap (`scripts/snap_endpoints.py`). Schema target: **OSW v0.3** (`OpenSidewalks/OpenSidewalks-Schema`, latest tag `0.3`). Validator: **`python-osw-validation` 0.4.4** (latest release, 2026-07-01).
>
> The v0.3.0-nyc.1 report, which audited an artifact produced by a one-shot restoration script from a source file that no longer exists, is preserved in git history. This release replaces that path entirely: the pipeline now produces the conformant artifact from public sources.

## Headline verdict

**The artifact is OSW v0.3 conformant.** The official validator returns `is_valid: True` with zero errors across all 3,374,261 features (1,155,380 nodes, 2,218,881 edges), including the geometry-to-node coordinate check introduced in validator 0.4.0.

It is **not perfect**. The pedestrian graph is fragmented (the largest component holds about 61% of pedestrian-graph nodes, spanning the four mainland boroughs; Staten Island is its own component), planimetric-derived geometry is approximate, and incline is DEM-derived rather than surveyed. Every known limitation is documented below.

## What changed since v0.3.0-nyc.1

| | v0.3.0-nyc.1 (restore script) | v0.3.1-nyc.1 (pipeline) |
|---|---|---|
| Features | 955,026 | 3,374,261 |
| Edges | 460,051 | 2,218,881 |
| Nodes | 494,975 | 1,155,380 |
| Connected components | 107,604 | 11,807 |
| Largest component | 159,506 nodes (32.6%) | 515,336 nodes (60.7% of pedestrian-graph nodes) |
| Planimetric widths + gap-fill | absent | present |
| Incline / elevation | present (USGS 10 m DEM) | present (NYC 2017 LiDAR DTM) |
| Provenance | reconstructed heuristically post-hoc | native from acquisition |
| Reproducible from public sources | no (source file gone) | yes |

The v0.3.0 report's "Recommendations for v0.3.1" asked for the endpoint merge to move into Stage 4, planimetric gap-fill to be re-included, provenance to be propagated at acquisition, and a full pipeline rebuild. This release does those.

## Schema conformance

| Check | Result |
|---|---|
| Validator | `python-osw-validation` 0.4.4 (current; bundles the OSW v0.3 schema) |
| Input | `output/nyc-osw-osw-split.zip` (split `nyc.nodes.geojson` + `nyc.edges.geojson`) |
| Validator output | **`is_valid: True, errors: 0`** |
| Geometry-to-node coordinate check (0.4.0+) | Pass: every edge terminal vertex equals its referenced node coordinate exactly |
| `$schema` | `https://sidewalks.washington.edu/opensidewalks/0.3/schema.json` |
| Root metadata | `dataSource`, `dataTimestamp`, `pipelineVersion`, `region` present |

Two pipeline-level guarantees behind the zero: Stage 4 drops edges that the 2 m endpoint merge collapses into zero-length self-loops (359,340 edges this build; they connect a node to itself and carry no connectivity), and the post-build snap moves every edge endpoint onto its node's coordinate.

## Feature composition

| OSW type | Count |
|---|---|
| Sidewalk edges | 703,467 |
| Crossing edges | 424,564 |
| Footway / steps edges | 429,328 |
| Street edges | 661,522 |
| Curb-ramp nodes | 199,836 |
| Other point nodes | 955,544 |
| **Total** | **3,374,261** |

Edges are directed: each walkable segment appears once per travel direction (u→v and v→u), with `incline` signed in the direction of travel. Counting unique node pairs, the graph has 1,184,309 physical segments; the GraphML export carries exactly that collapsed view. Edge counts are therefore not directly comparable to v0.3.0, which shipped one edge per segment.

## Graph integrity

| Metric | Value |
|---|---|
| Edges with unresolved `_u_id`/`_v_id` | 0 |
| Zero-length edges | 0 (dropped at the Stage 4 merge) |
| Duplicate edges | 12 removed (borough-boundary overlap) |
| Pedestrian-graph nodes (sidewalks + crossings + footways) | 849,417 |
| Pedestrian-graph edges | 949,411 |
| Connected components | 11,807 |
| Largest component | 515,336 nodes (60.7%), spanning Manhattan, Brooklyn, Queens, and the Bronx |
| Second largest | 132,012 nodes |
| Third largest | 72,105 nodes |

Staten Island is geographically separated from the mainland; its subgraph is expected to be a separate component. The remaining fragments are mostly short dangling sidewalks and alleys whose OSM geometry shares no endpoint with adjacent features. Routing consumers should snap query points to the giant component.

Two integration numbers to know before consuming the nodes:

- **220,084 nodes are not referenced by any edge.** Most are original endpoint locations whose references were remapped to a canonical node by the 2 m merge, plus curb ramps that did not integrate into the graph. They are inert for routing; pruning them is open work.
- **134,077 of 199,836 curb-ramp nodes (67%) are edge endpoints.** The rest carry their DOT survey data in the file but are not attached to the graph, mostly because the endpoint they snapped to was itself merged away.

Per-borough edge counts: QN 768,101; BK 515,555; SI 371,831; MN 290,897; BX 266,375. The sidewalk-to-street edge ratio falls from 1.80 in Manhattan to 0.76 in the Bronx, which tracks real OSM sidewalk mapping density, not pipeline behavior.

## Geometric sanity

| Check | Result |
|---|---|
| Invalid geometries (SFA) | 0 |
| Zero-length or sub-0.5 m edges | 0 |
| Features outside a conservative NYC bbox | 27, all within ~50 m of the bbox edge (real boundary features) |
| Edge length (metres) | min 2.0, median 8.4, p95 102, p99 213, max 2,943 |
| Edges over 500 m | 147 (long arterials and park paths without intermediate intersections) |

## Attribute coverage and distributions

| Attribute | Coverage | Notes |
|---|---|---|
| `incline` | 2,218,430 / 2,218,881 edges (99.98%) | p1 -6.1%, median 0.0%, p99 +6.1%; 3,115 edges carry grades beyond ±30%, the steep-noise tail of the DEM |
| `ext:elevation_m` | 1,155,380 / 1,155,380 nodes (100%) | min -3.5 m, p95 25.9 m, max 84.9 m |
| `width` | 815,782 sidewalk edges | OSM `width` tags first, planimetric polygon estimate fills the gaps |
| `surface` | 921,303 edges (41.5%) | asphalt 555,711; concrete 267,531; paving_stones 46,470; the rest smaller |
| `crossing:markings` | 369,932 / 424,564 crossings (87%) | `yes` 266,688; `zebra` 103,244 |
| `kerb` / `tactile_paving` | 199,836 curb nodes | `kerb=lowered` universally (a source limitation); `tactile_paving=yes` wherever the DOT survey recorded a detectable warning surface |

All `surface` and `crossing:markings` values are OSW-canonical; the validator and the audit found zero non-canonical enum values and zero leaked `999.0` sentinels.

### NYC DOT curb-ramp slope conformance (a finding about the city, not the data)

196,069 curb-ramp nodes carry measured slopes (the DOT `999.0` unmeasurable sentinel is omitted).

| ADA threshold | Compliant share |
|---|---|
| Running slope ≤ 5% (ADA running-slope cap) | **33.0%** (64,632 of 196,069) |
| Cross slope ≤ 2% (ADA cross-slope cap) | **82.6%** (161,918 of 196,069) |

Two-thirds of NYC's surveyed curb ramps exceed the ADA running-slope threshold. The dataset surfaces this; it is not a data-quality problem. The same finding held in the v0.3.0 audit. At the edge level, 97.4% of sidewalk edges and 98.9% of crossings have DEM-derived incline at or below 5%.

## Provenance coverage

| Field | Coverage |
|---|---|
| `ext:source` | 100% of 3,374,261 features |
| `ext:source_timestamp` | 100% |
| `ext:pipeline_version` | 100% |
| `ext:osm_id` | OSM-derived edges |
| `ext:borough` | Normalized `MN`/`BK`/`QN`/`BX`/`SI` codes |

One nuance: when a DOT curb ramp and an OSM node occupy the same location, the merged node keeps the OSM node's `ext:source` while the DOT fields (`ext:ramp_id`, slopes, streets) preserve the survey linkage.

## Known limitations

1. **Fragmentation outside the giant component.** About 39% of pedestrian-graph nodes sit outside the mainland giant component (Staten Island plus thousands of small fragments). This reflects upstream OSM coordinate noise and coverage gaps, not pipeline-introduced breaks.
2. **`kerb=lowered` for every DOT ramp.** The DOT survey does not distinguish flush from lowered ramps.
3. **Planimetric centerlines are approximate.** Gap-fill geometry is a minimum-rotated-rectangle axis, roughly meter-level, and coarse for irregular polygons.
4. **Incline is DEM-derived.** Short edges are noisier; values outside the OSW ±1.0 range are dropped as noise rather than clamped.
5. **Crossings are not always incident to street-edge endpoints.** Some crossings attach to sidewalk fragments rather than intersections, consistent with OSM's representation but limiting routing realism at less-mapped intersections.
6. **MTA ADA station index is a sidecar.** It is staged for consumers but not joined to graph nodes.

## Routing-engine end-to-end test

The v0.3.0-nyc.1 artifact was end-to-end tested with **Unweaver** (`nbolten/unweaver` @ `66352c1`), the OpenSidewalks community's reference routing engine: 9/10 distance-profile and 8/10 wheelchair-profile routes succeeded across Manhattan, Brooklyn, Queens, and the Bronx (see `validators/route_test_results.md` and `unweaver-project/`). Those results apply to the previous artifact; the conversion tooling (`scripts/osw_to_unweaver.py`, `scripts/route_test.py`) ships in this repo, and re-running the suite against v0.3.1 is open work. Unweaver's last upstream commit is from 2022 and needs three small compatibility patches on modern Python, documented in the v0.3.0 report in git history.

## Reproducibility

```bash
# from a fresh checkout, Python >= 3.11
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
uv pip install python-osw-validation

# 1. Build (city-wide: ~60-90 min, ~10 GB scratch)
python -m pipeline build

# 2. Snap edge endpoints onto node coordinates, emit the validator ZIP
python scripts/snap_endpoints.py --input output/nyc-osw.geojson

# 3. The gate
python -c "
from python_osw_validation import OSWValidation
r = OSWValidation('output/nyc-osw-osw-split.zip').validate()
print('valid:', r.is_valid, 'errors:', len(r.errors or []))"

# 4. Full data-quality audit (this report's numbers)
python validators/quality_audit.py output/nyc-osw.geojson
```

## Open work

1. **Fold the endpoint snap into Stage 4** so `pipeline build` alone emits the validator-ready artifact.
2. **Replace Stage 5 with the official validator** so the internal gate and the release gate are the same check.
3. **Prune unreferenced nodes** left behind by the endpoint merge, and re-attach the 33% of curb ramps whose snap target was merged away.
4. **Join crossings to street-edge endpoints** to lift the giant component further.
5. **Re-run the Unweaver routing suite** against this artifact.
6. **Join the MTA ADA index** onto transit-adjacent pedestrian nodes.
