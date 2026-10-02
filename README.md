# opensidewalks-nyc

**An OpenSidewalks v0.3-conformant pedestrian graph of New York City.**

A schema-valid graph of NYC's pedestrian network. Sidewalks, crossings, footways, steps, and curb ramps are first-class features, built from OpenStreetMap, the NYC DOT curb-ramp survey, and NYC Planimetric sidewalk polygons, with per-edge incline from the city's 2017 LiDAR elevation model. v0.3.2-nyc.1 passes `python-osw-validation` 0.5.0 with zero errors across all 3,874,332 features, and the build reproduces from public sources and one dated OpenStreetMap extract.

**If you have v0.3.1-nyc.1, replace it.** That release passed the validator with defects the validator does not check: elevation and incline were a spurious zero on 80% of the graph, every curb ramp was tagged `tactile_paving=yes` where the survey says the warning surface is missing on 59%, the boroughs were cut at their bridges, sidewalk widths were about double, and it held no secondary roads. v0.3.2 fixes each of these; [`release-notes/v0.3.2-nyc.1.md`](release-notes/v0.3.2-nyc.1.md) lists what changed and why. An earlier quality report also said two-thirds of the city's curb ramps exceed the ADA running-slope limit. That used the wrong limit and is withdrawn.

**Do not present routes from this graph as wheelchair accessible.** The ramp data is a 2018 survey, incline is a terrain estimate, and a sixth of the ramps are not on the graph. [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md) has the measurements behind every statement here.

| | |
|---|---|
| **Spec** | [OpenSidewalks Schema v0.3](https://github.com/OpenSidewalks/OpenSidewalks-Schema) (Taskar Center for Accessible Technology, University of Washington) |
| **Coverage** | All five NYC boroughs. The largest connected component holds 611,991 nodes (71.5% of pedestrian-graph nodes) and covers Manhattan, Brooklyn, Queens and the Bronx. Staten Island has no walkable link to the others and is a separate component. |
| **Size** | 3,874,332 features (1,189,651 Point nodes, 2,684,681 LineString edges) |
| **OSM data** | as of 2026-10-01T20:22:06Z (Geofabrik `new-york-261001.osm.pbf`, checksum recorded in the file root) |
| **Releases** | [GitHub Releases](https://github.com/msradam/opensidewalks-nyc/releases): canonical GeoJSON, FlatGeobuf, GraphML, routing JSON, OSW-validator ZIP, per-borough splits |
| **Code license** | Apache-2.0 |
| **Data license** | ODbL-1.0 (inherited from OpenStreetMap), see [LICENSE-DATA.md](LICENSE-DATA.md). Each file carries the licence and the attribution line in its root metadata. |

## Why this exists

NYC has the densest pedestrian network in North America. OpenStreetMap already holds a routable sidewalk and crossing network for the city, and research networks such as NYCWalks exist. To the author's knowledge this is the first public graph of NYC in the OpenSidewalks schema (the TDEI catalogue, which needs an account, has not been checked). The artifact fuses:

- **OpenStreetMap**: footways, crossings, steps, shared paths open to walkers, and street centerlines, the topological scaffold.
- **NYC DOT Pedestrian Ramp Locations** (`ufzp-rrqu`): 217,679 curb ramps surveyed in 2017 to 2020, every one a curb Point node carrying measured running slope, cross slope and counter slope and the survey's warning-surface status. 181,686 of them (83.5%) are on the graph. The survey is a snapshot; ramps rebuilt since are not updated.
- **NYC Planimetric Sidewalks** (`52n9-sdep`, 2022 capture): sidewalk widths for 820,940 sidewalk edges, plus 1,159 gap-fill centerlines where OSM has no sidewalk geometry.
- **NYC 2017 LiDAR DTM** (NY State GIS ImageServer): per-edge `incline` and per-node `ext:elevation_m`, interpolated from one downsampled tile per borough (5 to 12 m per pixel).

Every feature carries `ext:source` and `ext:pipeline_version`; edges and non-OSM nodes also carry `ext:source_timestamp`, and OSM-derived edges keep `ext:osm_id`.

## What's in the graph

Each feature in the canonical GeoJSON is one of:

| OSW type | Tagging | Count |
|---|---|---|
| Sidewalk Edge | `highway=footway, footway=sidewalk` | 935,428 |
| Crossing Edge | `highway=footway, footway=crossing` | 437,510 |
| Footway / Steps Edge | `highway=footway` (other) or `highway=steps` | 560,142 |
| Street Edge | `highway=residential/service/secondary/...` | 751,601 |
| Curb-ramp Point Node | `barrier=kerb` with DOT survey fields | 217,679 |
| Point Node (graph-structural) | edge endpoints | 971,972 |
| **Total** | | **3,874,332** |

Edges are directed. Every pedestrian segment appears once per travel direction, with `incline` signed in the direction of travel (963,620 pedestrian segments). Streets follow OSM's `oneway`, so a one-way street is a single edge. Edges carry `_u_id`/`_v_id` graph references, `surface`, `width`, `incline`, `name`, `crossing:markings`, and `ext:*` provenance. Curb nodes carry `kerb`, `tactile_paving`, cross streets, and the DOT slope measurements. See [`SCHEMA.md`](SCHEMA.md) for the full property reference.

## Getting the data

Don't clone for the data; pull a release. The canonical GeoJSON is 1.5 GB uncompressed.

```bash
# canonical OSW GeoJSON (gzipped)
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.geojson.gz
gunzip nyc-osw.geojson.gz

# compact, spatially indexed FlatGeobuf (recommended for most workloads)
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.fgb

# NetworkX / Gephi
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.graphml.gz

# flat nodes dict and edges list with lengths, for a router
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-routing.json.gz

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
G = nx.read_graphml("nyc-osw.graphml")   # a MultiDiGraph: one edge per OSW edge, u to v
print(G.number_of_nodes(), G.number_of_edges())
```

From v0.3.2 the GraphML is directed, because `incline` is signed by direction. Earlier releases shipped an undirected graph that kept one direction of each segment.

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

# 1. Build: acquires all sources, assembles the graph (about 40 min, 33 GB of
#    memory at peak, 11 GB of scratch under data/)
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

# 4. The checks the validator does not do (elevation, components by borough,
#    ramps against the survey, widths, gap-fill)
python validators/post_build_checks.py output/nyc-osw.geojson output/post_build_checks.json
```

Set `SOCRATA_APP_TOKEN` in the environment to lift NYC Open Data rate limits. For a quick trial, uncomment the `study_area` block in `config/build.yaml`; a Staten Island bounding box builds in about 8 minutes.

OpenStreetMap comes from one dated Geofabrik extract, named by URL and SHA-256 in `config/sources.yaml`. A build downloads it once, checks it, and makes no Overpass query. To build on newer OSM data, change both lines. [`scripts/README.md`](scripts/README.md) has the commands that turn a build into the release assets.

## Sources and licenses

| Source | What it contributes | License |
|---|---|---|
| OpenStreetMap (dated Geofabrik extract, graph built with OSMnx) | Footways, crossings, steps, shared paths, street centerlines, topology | ODbL-1.0 |
| NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`) | 217,679 curb ramps with measured slopes | Public Domain |
| NYC Planimetric Sidewalks (`52n9-sdep`) | Sidewalk widths and gap-fill centerlines | Public Domain |
| Borough boundaries from OpenStreetMap via Nominatim (the NYC Open Data dataset `7t3b-ywvw` the pipeline tries first has been withdrawn) | Region polygons, per-borough cut of the OSM graph | ODbL-1.0 |
| NYC 2017 topobathymetric LiDAR bare-earth DTM (NY State GIS ImageServer, 1 m service, sampled at 5 to 12 m) | Node elevations, edge inclines | Public Domain |

The combined dataset is **ODbL-1.0** by inheritance from OSM. Pipeline code is **Apache-2.0**.

## Limits, honest

- **Coverage follows OpenStreetMap.** Where OSM has no separately mapped sidewalk, the graph has none. A ramp can attach only where OSM has a sidewalk, crossing or footway within 5 m: 60.7% of the Bronx's surveyed ramps are on the graph, against 93.8% of Brooklyn's.
- **The graph is fragmented.** 71.5% of pedestrian-graph nodes sit in one component covering Manhattan, Brooklyn, Queens and the Bronx. Staten Island is its own component, and there are thousands of small fragments where OSM ways share no node. 18 of 23 bridges with a pedestrian path are walkable end to end on pedestrian edges; the quality report lists the rest.
- **Incline is an estimate of the terrain.** It comes from a bare-earth model at 5 to 12 m per pixel. Edges on bridges and in tunnels carry `ext:structure` and no incline, because the model describes the ground below them; node elevations there are the ground below too.
- **The ramp data is a 2018 survey.** It says a ramp was there and what DOT measured. DOT's own program data lists 41% of surveyed ramps at corners rebuilt since. The survey's description says its measurements do not establish ADA compliance.
- **A sixth of the ramps are not on the graph.** 18,305 have no pedestrian vertex within 5 m, and 17,688 share a node with a ramp that is on it.
- **Gap-fill sidewalks are the least reliable layer.** 1,159 segments come from planimetric polygons where OSM has no sidewalk. In a sample of 18 checked over orthoimagery, 9 were on a sidewalk or walkway and 4 were plainly wrong, and nearly all are unconnected to the rest of the graph. Filter on `ext:source = nyc_planimetric_sidewalks` to drop them.
- **`width` is the mean width of the planimetric polygon,** not the clear width at that spot. It is within about 0.6 m of a local transect at the median.
- **DOT records every ramp as `kerb=lowered`.** The source survey does not distinguish flush from lowered.
- **No live data.** Elevator outages, construction closures, and weather belong in the consuming application.

## Citation

```bibtex
@dataset{rahman_opensidewalks_nyc_2026,
  author       = {Rahman, Adam Munawar},
  title        = {opensidewalks-nyc: An OpenSidewalks v0.3-conformant pedestrian graph of New York City},
  year         = {2026},
  version      = {0.3.2-nyc.1},
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

`v0.3.2-nyc.1`. A corrected rebuild of v0.3.1-nyc.1, whose published files had defects the validator does not check. This release fixes them, pins the OSM input to a dated extract, and adds the checks that would have caught them (`validators/post_build_checks.py`). Issues and PRs welcome, especially around accessibility-feature coverage gaps.
