# opensidewalks-nyc

**An OpenSidewalks v0.3–conformant pedestrian graph of New York City.**

A standards-conformant, fully attributed graph of NYC's pedestrian network — sidewalks, crossings, footways, steps, and curb ramps as first-class features — built from OpenStreetMap and the NYC DOT curb-ramp survey. Validated against the official OSW v0.3 spec (`python-osw-validation` 0.3.7, 100% pass) and end-to-end-tested with the OpenSidewalks community's reference routing engine, [Unweaver](https://github.com/nbolten/unweaver).

See [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md) for honest numbers on conformance, topology, coverage gaps, and routing tests.

| | |
|---|---|
| **Spec** | [OpenSidewalks Schema v0.3](https://github.com/OpenSidewalks/OpenSidewalks-Schema) (Taskar Center for Accessible Technology, University of Washington) |
| **Coverage** | All five NYC boroughs (Staten Island is its own subgraph; mainland-NYC giant component covers Manhattan / Brooklyn / Queens / Bronx) |
| **Size** | 955,026 features (494,975 Point nodes, 460,051 LineString edges) |
| **Releases** | [GitHub Releases](https://github.com/msradam/opensidewalks-nyc/releases) — canonical GeoJSON, FlatGeobuf, GraphML, OSW-validator-format ZIP, per-borough splits |
| **Code license** | Apache-2.0 |
| **Data license** | ODbL-1.0 (inherited from OpenStreetMap) — see [LICENSE-DATA.md](LICENSE-DATA.md) |

## Why this exists

NYC has the densest pedestrian network in North America. Until now there has been no public, standards-conformant, routable graph of it. This artifact fuses two principal sources:

- **OpenStreetMap** provides footways, crossings, and the topological scaffold — including ~210k sidewalks, ~121k crossings, and ~184k pedestrian-passable street centerlines.
- **NYC DOT's Pedestrian Ramp Locations** (`ufzp-rrqu`) contributes ~187k surveyed curb-ramp Point Nodes with measured geometry: running slope, cross slope, counter slope, ADA violation flags, condition, tactile paving, dimensions.

Each feature carries `ext:source`, `ext:source_timestamp`, and `ext:pipeline_version` for auditability. The artifact validates 100% against the OSW v0.3 schema (split-format, edges + nodes).

A v0.3.1 will additionally fuse NYC Planimetric Sidewalks (`vfx9-tbb6`) for OSM-sparse areas — see the recommendations section in [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md).

## What's in the graph

Each feature in the canonical GeoJSON is one of:

| OSW type | Example highway/footway | Count |
|---|---|---|
| Sidewalk Edge | `highway=footway, footway=sidewalk` | 210,425 |
| Crossing Edge | `highway=footway, footway=crossing` | 121,450 |
| Footway / Steps Edge | `highway=footway/pedestrian/steps` (other) | 12,932 |
| Street Edge | `highway=residential/service/primary/...` | 184,482 |
| Point Node (graph-structural) | sidewalk endpoints | 307,605 |
| Curb-ramp Point Node | `barrier=kerb`, with slope/condition props | 187,368 |
| **Total features** |  | **955,026** |

Edges carry: `_u_id`, `_v_id` (graph endpoints), `surface`, `incline`, `ext:running_slope_pct`, `ext:cross_slope_pct`, `ext:counter_slope_pct`, `ext:kerb`, `ext:tactile_paving`, `ext:ada_violations`, `crossing:markings`, `width`, `ext:lit`, `name`, `ext:borough`, `ext:osm_id`.

Points carry: `ext:elevation_m` (USGS 3DEP 10 m DEM), and where applicable `barrier=kerb`, `kerb`, `tactile_paving`.

## Getting the data

Don't clone for the data — pull a release. The 430 MB canonical GeoJSON is too big for in-tree storage.

```bash
# canonical OSW GeoJSON
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.geojson

# compact, spatially indexed FlatGeobuf (recommended for most workloads)
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.fgb

# NetworkX / Gephi
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.graphml

# per-borough splits
for b in MN BK QN BX SI; do
  curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw-$b.geojson
done
```

Verify with `SHA256SUMS` from the release page.

## Quickstart: load the graph

### NetworkX

```python
import networkx as nx
G = nx.read_graphml("nyc-osw.graphml")
print(G.number_of_nodes(), G.number_of_edges())
```

### GeoPandas / pyogrio (FlatGeobuf, spatially indexed)

```python
import geopandas as gpd
gdf = gpd.read_file("nyc-osw.fgb", bbox=(-73.99, 40.74, -73.97, 40.76))  # Times Sq window
sidewalks = gdf[(gdf["highway"] == "footway") & (gdf["footway"] == "sidewalk")]
```

### DuckDB (spatial extension)

```sql
INSTALL spatial; LOAD spatial;
SELECT count(*) FROM ST_Read('nyc-osw.fgb')
WHERE highway = 'footway' AND "footway" = 'sidewalk';
```

## Reproducibility

The full pipeline is in [`pipeline/`](pipeline/) and documented stage-by-stage in [`METHODOLOGY.md`](METHODOLOGY.md). [`notebooks/build.ipynb`](notebooks/build.ipynb) is a guided walkthrough.

```bash
git clone https://github.com/msradam/opensidewalks-nyc
cd opensidewalks-nyc
uv venv && source .venv/bin/activate
uv pip install -e .

python -m pipeline build              # all six stages, ~60–90 min, ~10 GB scratch
python -m pipeline build --stage 3    # resume from a stage
python -m pipeline validate           # OSW conformance check
```

Set `SOCRATA_APP_TOKEN` in the environment to lift NYC Open Data rate limits from 1 req/s to 1000 req/s.

## Sources and licenses

| Source | What it contributes | License |
|---|---|---|
| OpenStreetMap (Overpass via OSMnx) | Footways, crossings, steps, residential streets, road topology | ODbL-1.0 |
| NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`) | 217k+ curb ramps with slope and condition | Public Domain |
| NYC Planimetric Sidewalks (`vfx9-tbb6`) | Sidewalk polygons (gap-fill source) | Public Domain |
| NYC Borough Boundaries (`7t3b-ywvw`) | Region polygons | Public Domain |
| USGS 3DEP 10 m DEM | Elevation on point nodes | Public Domain |

The combined dataset is **ODbL-1.0** by inheritance from OSM. Pipeline code is **Apache-2.0**.

## Limits, honest

- **OSM gaps.** Where neither OSM nor Planimetric covers a sidewalk, it isn't in the graph. Coverage is uneven at the borough periphery.
- **Planimetric centerline geometry is approximate** (~meter-level). The 5 m endpoint snap mitigates but does not eliminate misalignment with OSM nodes.
- **The Voronoi centerline extraction fails on irregular / L-shaped polygons.** Failures are counted in the build report, not silently dropped.
- **DOT ramp dataset records all ramps as `kerb=lowered`** — there is no flush/lowered distinction in the source.
- **No live data.** Elevator-outage feeds, construction closures, and weather are out of scope. Plug those into your routing layer.

## Citation

```bibtex
@dataset{rahman_opensidewalks_nyc_2026,
  author       = {Rahman, Adam Munawar},
  title        = {opensidewalks-nyc: An OpenSidewalks v0.3-conformant pedestrian graph of New York City},
  year         = {2026},
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

`v0.3.0-nyc.1` — first public release. Reproducible from documented sources. Issues and PRs welcome, especially around accessibility-feature coverage gaps.
