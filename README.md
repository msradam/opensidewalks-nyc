# opensidewalks-nyc

**An OpenSidewalks v0.3-conformant pedestrian graph of New York City.**

A schema-valid graph of NYC's pedestrian network. Sidewalks, crossings, footways, steps, and curb ramps are first-class features, built from OpenStreetMap, the NYC DOT curb-ramp survey, and NYC Planimetric sidewalk polygons, with per-edge incline from the city's 2017 LiDAR elevation model. The published artifact passes `python-osw-validation` 0.4.4 and 0.4.5 with zero errors across all 3,374,261 features, and the build reproduces from public sources.

**Read this before using v0.3.1-nyc.1.** A review in October 2026 found defects in the published files that the validator does not catch. They are fixed in the pipeline code and a corrected release has not been cut yet.

- Elevation and incline are a spurious zero for about 80% of the graph (all of Brooklyn and Staten Island, most of Manhattan and Queens).
- Every curb ramp is tagged `tactile_paving=yes`. The DOT survey records the warning surface as missing on 59% of ramps.
- The Bronx is not connected to the other boroughs, and nearly every bridge is cut at the borough line.
- Sidewalk `width` is overstated, about double on block-ring polygons.
- Most of the 6,122 planimetric gap-fill "sidewalk" edges run through block interiors.
- The files fail `python-osw-validation` 0.5.0 (August 2026), which limits coordinates to 7 decimal places.
- An earlier version of the quality report said two-thirds of the city's curb ramps exceed the ADA running-slope limit. That used the wrong limit and is withdrawn.

Do not present routes from this release as wheelchair-accessible. [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md) has the evidence for each point.

| | |
|---|---|
| **Spec** | [OpenSidewalks Schema v0.3](https://github.com/OpenSidewalks/OpenSidewalks-Schema) (Taskar Center for Accessible Technology, University of Washington) |
| **Coverage** | All five NYC boroughs. The largest connected component holds 428,068 nodes (63% of pedestrian-graph nodes) and covers Manhattan, Brooklyn and Queens; Staten Island and the Bronx are separate subgraphs. |
| **Size** | 3,374,261 features (1,155,380 Point nodes, 2,218,881 LineString edges) |
| **Releases** | [GitHub Releases](https://github.com/msradam/opensidewalks-nyc/releases): canonical GeoJSON, FlatGeobuf, GraphML, routing JSON, OSW-validator ZIP, per-borough splits |
| **Code license** | Apache-2.0 |
| **Data license** | ODbL-1.0 (inherited from OpenStreetMap), see [LICENSE-DATA.md](LICENSE-DATA.md) |

## Why this exists

NYC has the densest pedestrian network in North America. OpenStreetMap already holds a routable sidewalk and crossing network for the city, and research networks such as NYCWalks exist. To the author's knowledge this is the first public graph of NYC in the OpenSidewalks schema (the TDEI catalogue, which needs an account, has not been checked). The artifact fuses:

- **OpenStreetMap**: footways, crossings, steps, and street centerlines, the topological scaffold.
- **NYC DOT Pedestrian Ramp Locations** (`ufzp-rrqu`): 217,679 curb ramps surveyed in 2017 to 2020, of which 199,836 appear as curb Point nodes, carrying measured running slope, cross slope and counter slope. The survey is a snapshot; ramps rebuilt since are not updated.
- **NYC Planimetric Sidewalks** (`52n9-sdep`, 2022 capture): sidewalk widths for 615,099 sidewalk edges, plus gap-fill centerlines where OSM has no sidewalk geometry.
- **NYC 2017 LiDAR DTM** (NY State GIS ImageServer): per-edge `incline` and per-node `ext:elevation_m`, sampled from one downsampled tile per borough (5 to 10 m per pixel).

Every feature carries `ext:source` and `ext:pipeline_version`; edges and non-OSM nodes also carry `ext:source_timestamp`, and OSM-derived edges keep `ext:osm_id`.

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

Edges are directed: most walkable segments appear once per travel direction, with `incline` signed in the direction of travel (1,184,309 unique segments; the GraphML export carries that collapsed view). One-way streets and the gap-fill sidewalks appear in one direction only. Edges carry `_u_id`/`_v_id` graph references, `surface`, `width`, `incline`, `name`, `crossing:markings`, and `ext:*` provenance. Curb nodes carry `kerb`, `tactile_paving`, cross streets, and the DOT slope measurements. See [`SCHEMA.md`](SCHEMA.md) for the full property reference.

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

# 1. Build: acquires all sources, assembles the graph (~60-90 min, ~10 GB scratch)
python -m pipeline build

# 2. Snap edge endpoints onto their node coordinates and emit the validator ZIP
python scripts/snap_endpoints.py --input output/nyc-osw.geojson

# 3. Conformance gate: the official validator must return zero errors.
#    It pins geopandas==0.14.4, which breaks the pipeline, so it runs in its
#    own throwaway environment and is never installed next to the pipeline.
uv run --no-project --isolated --with python-osw-validation python -c "
from python_osw_validation import OSWValidation
r = OSWValidation('output/nyc-osw-osw-split.zip').validate()
print('valid:', r.is_valid, 'errors:', len(r.errors or []))"
```

Set `SOCRATA_APP_TOKEN` in the environment to lift NYC Open Data rate limits from about 1 req/s to 1000 req/s. For a quick trial, uncomment the `study_area` block in `config/build.yaml`; a Staten Island bounding box builds in about 8 minutes.

The city-wide build pulls the whole OSM walk network through the public Overpass API, one borough per query. That is a heavy use of a shared service; a dated regional extract (for example from Geofabrik) is the better source for repeated or scheduled builds, and it would also pin the OSM snapshot date.

## Sources and licenses

| Source | What it contributes | License |
|---|---|---|
| OpenStreetMap (Overpass via OSMnx) | Footways, crossings, steps, street centerlines, topology | ODbL-1.0 |
| NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`) | 217,679 curb ramps with measured slopes | Public Domain |
| NYC Planimetric Sidewalks (`52n9-sdep`) | Sidewalk widths and gap-fill centerlines | Public Domain |
| Borough boundaries from OpenStreetMap via Nominatim (the NYC Open Data dataset `7t3b-ywvw` the pipeline tries first has been withdrawn) | Region polygons, per-borough OSM queries | ODbL-1.0 |
| NYC 2017 topobathymetric LiDAR bare-earth DTM (NY State GIS ImageServer, 1 m service, sampled at 5 to 10 m) | Node elevations, edge inclines | Public Domain |

The combined dataset is **ODbL-1.0** by inheritance from OSM. Pipeline code is **Apache-2.0**.

## Limits, honest

- **Coverage follows the sources.** Where neither OSM nor the planimetric polygons record a sidewalk, it is not in the graph. Coverage thins at the borough periphery.
- **The graph is fragmented.** 63% of pedestrian-graph nodes sit in one component covering Manhattan, Brooklyn and Queens. Staten Island and the Bronx are separate components, and there are thousands of small fragments where OSM features share no endpoint. Snapping every query to the largest component moves Bronx and Staten Island points into another borough.
- **Planimetric centerlines are approximate.** Gap-fill geometry is derived from polygon axes, roughly meter-level.
- **DOT records every ramp as `kerb=lowered`.** The source survey does not distinguish flush from lowered.
- **Incline is DEM-derived, and in this release mostly wrong.** See the notice at the top. Even where the elevation is real, the DEM is sampled at 5 to 10 m per pixel and the median edge is 8.4 m long, so short-edge inclines are noise.
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

`v0.3.1-nyc.1`. Rebuilt from scratch by the pipeline (the v0.3.0 release was produced by a one-shot restoration script). This release adds planimetric widths and gap-fill, LiDAR incline and elevations, native per-feature provenance, and a far less fragmented graph. Several of those additions are defective in the published files (see the notice at the top); the fixes are in the code and a corrected release is pending. Issues and PRs welcome, especially around accessibility-feature coverage gaps.
