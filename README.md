# opensidewalks-nyc

**An OpenSidewalks v0.3-conformant pedestrian graph of New York City.**

A standards-conformant, fully attributed graph of NYC's pedestrian network. Sidewalks, crossings, footways, steps, and curb ramps are first-class features, built from OpenStreetMap, the NYC DOT curb-ramp survey, and NYC Planimetric sidewalk polygons, with per-edge incline from the city's 2017 LiDAR elevation model. The artifact passes the official validator (`python-osw-validation` 0.4.4) with zero errors across all 3,374,261 features, and the entire build reproduces from public sources with three commands.

See [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md) for honest numbers on conformance, topology, coverage gaps, and known limitations.

| | |
|---|---|
| **Spec** | [OpenSidewalks Schema v0.3](https://github.com/OpenSidewalks/OpenSidewalks-Schema) (Taskar Center for Accessible Technology, University of Washington) |
| **Coverage** | All five NYC boroughs. The largest connected component holds 515,336 nodes (61% of pedestrian-graph nodes) and spans the four mainland boroughs; Staten Island is its own subgraph. |
| **Size** | 3,374,261 features (1,155,380 Point nodes, 2,218,881 LineString edges) |
| **Releases** | [GitHub Releases](https://github.com/msradam/opensidewalks-nyc/releases): canonical GeoJSON, FlatGeobuf, GraphML, routing JSON, OSW-validator ZIP, per-borough splits |
| **Code license** | Apache-2.0 |
| **Data license** | ODbL-1.0 (inherited from OpenStreetMap), see [LICENSE-DATA.md](LICENSE-DATA.md) |

## Why this exists

NYC has the densest pedestrian network in North America, and until this project there was no public, standards-conformant, routable graph of it. The artifact fuses:

- **OpenStreetMap**: footways, crossings, steps, and street centerlines, the topological scaffold.
- **NYC DOT Pedestrian Ramp Locations** (`ufzp-rrqu`): 217,679 surveyed curb ramps as curb Point nodes, carrying measured running slope, cross slope, counter slope, and detectable-warning-surface presence.
- **NYC Planimetric Sidewalks** (`52n9-sdep`): sidewalk widths for 815,782 OSM sidewalk edges, plus gap-fill centerlines where OSM has no sidewalk geometry.
- **NYC 2017 LiDAR DTM** (NY State GIS ImageServer): per-edge `incline` and per-node `ext:elevation_m`.

Every feature carries `ext:source`, `ext:source_timestamp`, and `ext:pipeline_version` for auditability, and OSM-derived features keep `ext:osm_id`.

## What's in the graph

Each feature in the canonical GeoJSON is one of:

| OSW type | Tagging | Count |
|---|---|---|
| Sidewalk Edge | `highway=footway, footway=sidewalk` | 703,467 |
| Crossing Edge | `highway=footway, footway=crossing` | 424,564 |
| Footway / Steps Edge | `highway=footway/pedestrian/steps` (other) | 429,328 |
| Street Edge | `highway=residential/service/primary/...` | 661,522 |
| Curb-ramp Point Node | `barrier=kerb` with DOT survey fields | 199,836 |
| Point Node (graph-structural) | edge endpoints | 955,544 |
| **Total** | | **3,374,261** |

Edges are directed: each walkable segment appears once per travel direction, with `incline` signed in the direction of travel (1,184,309 unique segments; the GraphML export carries that collapsed view). Edges carry `_u_id`/`_v_id` graph references, `surface`, `width`, `incline`, `name`, `crossing:markings`, and `ext:*` provenance. Curb nodes carry `kerb`, `tactile_paving`, cross streets, and the DOT slope measurements. See [`SCHEMA.md`](SCHEMA.md) for the full property reference.

## Getting the data

Don't clone for the data; pull a release. The canonical GeoJSON is 2 GB uncompressed.

```bash
# canonical OSW GeoJSON (gzipped)
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.geojson.gz
gunzip nyc-osw.geojson.gz

# compact, spatially indexed FlatGeobuf (recommended for most workloads)
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.fgb

# NetworkX / Gephi
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.graphml.gz

# per-borough splits
for b in MN BK QN BX SI; do
  curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw-$b.geojson.gz
done
```

Verify downloads against `SHA256SUMS` from the release page.

## Quickstart: load the graph

### GeoPandas / pyogrio (FlatGeobuf, spatially indexed)

```python
import geopandas as gpd
gdf = gpd.read_file("nyc-osw.fgb", bbox=(-73.99, 40.74, -73.97, 40.76))  # Times Sq window
sidewalks = gdf[(gdf["highway"] == "footway") & (gdf["footway"] == "sidewalk")]
```

### NetworkX

```python
import networkx as nx
G = nx.read_graphml("nyc-osw.graphml")
print(G.number_of_nodes(), G.number_of_edges())
```

### DuckDB (spatial extension)

```sql
INSTALL spatial; LOAD spatial;
SELECT count(*) FROM ST_Read('nyc-osw.fgb')
WHERE highway = 'footway' AND footway = 'sidewalk';
```

## Reproducing the artifact

The six-stage pipeline is in [`pipeline/`](pipeline/) and documented stage-by-stage in [`METHODOLOGY.md`](METHODOLOGY.md). [`notebooks/build.ipynb`](notebooks/build.ipynb) walks through the build reports and re-runs the conformance check.

```bash
git clone https://github.com/msradam/opensidewalks-nyc
cd opensidewalks-nyc
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
uv pip install python-osw-validation

# 1. Build: acquires all sources, assembles the graph (~60-90 min, ~10 GB scratch)
python -m pipeline build

# 2. Snap edge endpoints onto their node coordinates and emit the validator ZIP
python scripts/snap_endpoints.py --input output/nyc-osw.geojson

# 3. Conformance gate: the official validator must return zero errors
python -c "
from python_osw_validation import OSWValidation
r = OSWValidation('output/nyc-osw-osw-split.zip').validate()
print('valid:', r.is_valid, 'errors:', len(r.errors or []))"
```

Set `SOCRATA_APP_TOKEN` in the environment to lift NYC Open Data rate limits from about 1 req/s to 1000 req/s.

## Sources and licenses

| Source | What it contributes | License |
|---|---|---|
| OpenStreetMap (Overpass via OSMnx) | Footways, crossings, steps, street centerlines, topology | ODbL-1.0 |
| NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`) | 217,679 curb ramps with measured slopes | Public Domain |
| NYC Planimetric Sidewalks (`52n9-sdep`) | Sidewalk widths and gap-fill centerlines | Public Domain |
| NYC Borough Boundaries (`7t3b-ywvw`, OSMnx geocoding fallback) | Region polygons | Public Domain |
| NYC 2017 1-ft LiDAR bare-earth DTM (NY State GIS) | Node elevations, edge inclines | Public Domain |
| MTA subway stations (`drh3-e2fd`, GTFS fallback) | ADA-accessibility sidecar index | Public Domain |

The combined dataset is **ODbL-1.0** by inheritance from OSM. Pipeline code is **Apache-2.0**.

## Limits, honest

- **Coverage follows the sources.** Where neither OSM nor the planimetric polygons record a sidewalk, it is not in the graph. Coverage thins at the borough periphery.
- **The graph is fragmented.** 61% of pedestrian-graph nodes sit in one giant mainland component; the rest are Staten Island (geographically separate, its own component) and thousands of small fragments where OSM features share no endpoint. Routing consumers should snap query points to the giant component.
- **Planimetric centerlines are approximate.** Gap-fill geometry is derived from polygon axes, roughly meter-level.
- **DOT records every ramp as `kerb=lowered`.** The source survey does not distinguish flush from lowered.
- **Incline is DEM-derived.** Short edges are noisier because sub-meter elevation error divides by a small run; values outside the OSW range of plus or minus 1.0 are dropped as noise.
- **No live data.** Elevator outages, construction closures, and weather belong in the consuming application.

## Citation

```bibtex
@dataset{rahman_opensidewalks_nyc_2026,
  author       = {Rahman, Adam Munawar},
  title        = {opensidewalks-nyc: An OpenSidewalks v0.3-conformant pedestrian graph of New York City},
  year         = {2026},
  version      = {0.3.1-nyc.1},
  publisher    = {GitHub},
  url          = {https://github.com/msradam/opensidewalks-nyc}
}
```

See also [`CITATION.cff`](CITATION.cff).

## Acknowledgements

The [OpenSidewalks Schema](https://sidewalks.washington.edu/) is developed by the Taskar Center for Accessible Technology at the University of Washington. This dataset would not exist without that spec or the [AccessMap](https://www.accessmap.io/) project that motivated it. NYC Open Data, NYC DOT, NYC OTI, and the OpenStreetMap contributor community supplied the underlying data.

## AI-assisted authoring

Portions of this repository (pipeline code, conversion scripts, documentation) were drafted with the help of large language models. All output was reviewed and accepted by a human author who takes responsibility for the code and methodology. The source data itself was not generated or modified by AI. See [`NOTICE`](NOTICE) for the full disclosure.

## Status

`v0.3.1-nyc.1`. Rebuilt from scratch by the pipeline (the v0.3.0 release was produced by a one-shot restoration script). This release adds planimetric widths and gap-fill, LiDAR incline and elevations, native per-feature provenance, and a far less fragmented graph. Issues and PRs welcome, especially around accessibility-feature coverage gaps.
