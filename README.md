# opensidewalks-nyc

**An OpenSidewalks v0.3-conformant pedestrian graph of New York City.**

A schema-valid graph of NYC's pedestrian network. Sidewalks, crossings, footways, steps, and curb ramps are first-class features, built from OpenStreetMap, the NYC DOT curb-ramp survey, and NYC Planimetric sidewalk polygons, with per-edge incline from the city's 2017 LiDAR terrain model and, on bridges and elevated ways, from the LiDAR point clouds themselves. v0.3.3-nyc.1 passes `python-osw-validation` 0.5.0 with zero errors across all 4,068,058 features, and the build reproduces from public sources and one dated OpenStreetMap extract in a single run.

**If you have v0.3.1-nyc.1, replace it.** That release passed the validator with defects the validator does not check: elevation and incline were a spurious zero on 80% of the graph, every curb ramp was tagged `tactile_paving=yes` where the survey says the warning surface is missing on 59%, the boroughs were cut at their bridges, sidewalk widths were about double, and it held no secondary roads. v0.3.2 fixes each of these; [`release-notes/v0.3.2-nyc.1.md`](release-notes/v0.3.2-nyc.1.md) lists what changed and why. An earlier quality report also said two-thirds of the city's curb ramps exceed the ADA running-slope limit. That used the wrong limit and is withdrawn.

**Do not present routes from this graph as wheelchair accessible.** The ramp data is a 2018 survey, incline is an estimate from an airborne survey, and a sixth of the ramps are not on the graph. v0.3.3 fixes the incline on and around bridges that sent v0.3.2's wheelchair routes the long way round, and checks the rule that ties ramps to crossings against imagery; [`release-notes/v0.3.3-nyc.1.md`](release-notes/v0.3.3-nyc.1.md) lists what changed and [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md) has the measurements behind every statement here.

| | |
|---|---|
| **Spec** | [OpenSidewalks Schema v0.3](https://github.com/OpenSidewalks/OpenSidewalks-Schema) (Taskar Center for Accessible Technology, University of Washington) |
| **Coverage** | All five NYC boroughs. The largest connected component holds 611,968 nodes (71.7% of pedestrian-graph nodes) and covers Manhattan, Brooklyn, Queens and the Bronx. Staten Island has no walkable link to the others and is a separate component. |
| **Size** | 4,068,058 features (1,186,910 Point nodes, 2,881,148 LineString edges) |
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
| Sidewalk Edge | `highway=footway, footway=sidewalk` | 933,110 |
| Crossing Edge | `highway=footway, footway=crossing` | 437,510 |
| Footway / Steps Edge | `highway=footway` (other) or `highway=steps` | 560,142 |
| Street Edge | `highway=residential/service/secondary/...` | 950,386 |
| Curb-ramp Point Node | `barrier=kerb` with DOT survey fields | 217,679 |
| Point Node (graph-structural) | edge endpoints | 969,231 |
| **Total** | | **4,068,058** |

Edges are directed. Every segment, street edges included, appears once per travel direction, with `incline` signed in the direction of travel. Edges carry `_u_id`/`_v_id` graph references, `surface`, `width`, `incline`, `name`, `crossing:markings`, `ext:structure` on bridges, tunnels and elevated ways, and `ext:*` provenance. Curb nodes carry `kerb`, `tactile_paving`, cross streets, and the DOT slope measurements. Nodes carry `ext:elevation_m`, and on structures `ext:elevation_source`. See [`SCHEMA.md`](SCHEMA.md) for the full property reference. The planimetric gap-fill centrelines of earlier releases are not in the graph; they ship as a separate file, `nyc-gapfill-sidewalks.geojson`, whose root says what they are.

## Getting the data

Don't clone for the data; pull a release. The canonical GeoJSON is 1.5 GB uncompressed.

```bash
# canonical OSW GeoJSON (gzipped)
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.geojson.gz
gunzip nyc-osw.geojson.gz

# compact, spatially indexed FlatGeobuf (recommended for most workloads)
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.fgb

# NetworkX / Gephi (directed multigraph; nyc-osw-undirected.graphml.gz is the
# simple undirected graph of releases before v0.3.2, for code written against it)
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

# 1. Build: acquires all sources, assembles the graph (about 48 min, 35 GB of
#    memory at peak, 14 GB of scratch under data/, of which about 2 GB are
#    LiDAR tiles around the structures and 650 MB terrain tiles)
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

# 4. The checks the validator does not do (elevation, deck heights, components
#    by borough, ramps against the survey, widths, the sidecar)
python validators/post_build_checks.py output/nyc-osw.geojson output/post_build_checks.json
```

Set `SOCRATA_APP_TOKEN` in the environment to lift NYC Open Data rate limits. For a quick trial, uncomment the `study_area` block in `config/build.yaml`; a Staten Island bounding box builds in about 8 minutes.

OpenStreetMap comes from one dated Geofabrik extract, named by URL and SHA-256 in `config/sources.yaml`. A build downloads it once, checks it, and makes no Overpass query. To build on newer OSM data, change both lines. [`scripts/README.md`](scripts/README.md) has the commands that turn a build into the release assets.

## Compared with other routers

[`compare/`](compare/) measures this graph's wheelchair routes against routers people already use, each run on this machine from the same OpenStreetMap extract: OpenRouteService 10.0.1 (wheelchair and walking profiles), Valhalla 3.9.0 (pedestrian costing) and Unweaver, the engine this graph's profile was written for. No public routing service is called. 12,437 origin and destination pairs are routed: 2,000 seeded random pairs in each borough and city-wide, 17 landmark pairs, and 420 Brownsville trips between public facilities.

What it found, in short:

- **This graph finds fewer routes.** Its wheelchair profile finds a route for 91% of random pairs in Brooklyn, 68% in Queens, 64% in Manhattan, 51% in the Bronx and 50% on Staten Island. OpenRouteService on plain OSM finds 97% to 99% everywhere.
- **Most of OpenRouteService's routes cross where this graph has no surveyed ramp.** By this graph's data, 88% to 95% of its wheelchair routes in each borough use at least one crossing with no surveyed ramp within 5 m of an end, and 13% (Brooklyn) to 73% (Staten Island) spend more than 10 m in the roadway. OSM carries no kerb tag there that OpenRouteService reads as a barrier, and where no sidewalk is mapped it routes along the street.
- **The same engine on this graph's data behaves like this graph.** Given this graph converted back to OSM with the ramp rule on the crossing ends ([`scripts/osw_to_osm.py`](scripts/osw_to_osm.py)), OpenRouteService finds 67% to 95% of the pairs in a borough, and its routes use an unramped crossing 0.02 to 0.10 times per kilometre, against 0.7 to 1.1 on plain OSM.
- **This graph is worse in ways the comparison names.** It refuses crossings an OSM mapper tagged with a lowered or flush kerb (24.9% of the 15,599 crossing ends where the 2018 survey has no ramp within 5 m), it ignores OSM's surface, smoothness and `wheelchair=no` tags, and it has no route where OSM has no sidewalk.
- **The search is the engine's.** On 1,354 requests (840 in Brownsville, 34 between landmarks, 480 random across the city), this repo's search and Unweaver agree every time on whether a route exists, and on its length to within 7 m.

None of this says which route a wheelchair user could travel. That takes the field audit the demo describes. The result files and the method are in `research_notes/compare/` (not in the repository); `compare/analyse.py` regenerates them.

## The Brownsville demo

[`demo/`](demo/) is a static page for Brooklyn Community District 16: the sidewalk network with width and incline, the crossings, every surveyed curb ramp drawn by NYC DOT's own assessment (with rebuilt corners marked), and ten trips routed three ways (this graph's wheelchair profile, OpenRouteService's wheelchair profile on plain OpenStreetMap, and OpenRouteService's walking profile). Every route is also written out step by step, so the page works with a screen reader and without the map. A panel says what was checked, where this graph does worse, and what nobody has checked yet. Nothing on it has been verified on the ground.

Open `demo/index.html` in a browser. There is no server and nothing to install: the data are plain script files under `demo/data/`, and the only library, Leaflet 1.9.4, is stored in `demo/vendor/`. The page has no basemap tiles; streets come from the same OpenStreetMap extract as the graph. To publish it, serve the `demo/` folder as it is. GitHub Pages serves only the root or `/docs` of a branch, so push the folder to a branch of its own (`git subtree push --prefix demo origin gh-pages`) and point Pages at that branch.

```bash
open demo/index.html                       # macOS; or double-click the file

# Rebuild the data files from the comparison's results (see demo/build_config.json for the inputs)
python scripts/build_demo_data.py demo/build_config.json
```

The page is checked with axe-core 4.11 and pa11y 9.1 (both of its runners) in headless Chrome at desktop and phone width, with a keyboard pass and a reduced-motion pass; it has zero violations in each.

## Sources and licenses

| Source | What it contributes | License |
|---|---|---|
| OpenStreetMap (dated Geofabrik extract, graph built with OSMnx) | Footways, crossings, steps, shared paths, street centerlines, topology | ODbL-1.0 |
| NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`) | 217,679 curb ramps with measured slopes | Public Domain |
| NYC Planimetric Sidewalks (`52n9-sdep`) | Sidewalk widths and gap-fill centerlines | Public Domain |
| Borough boundaries from OpenStreetMap via Nominatim (the NYC Open Data dataset `7t3b-ywvw` the pipeline tries first has been withdrawn) | Region polygons, per-borough cut of the OSM graph | ODbL-1.0 |
| NYC 2017 topobathymetric LiDAR bare-earth DTM (NY State GIS ImageServer, 1 m service, fetched at 2 m) | Node elevations, edge inclines | Public Domain |
| LiDAR point clouds, 2017 NYC and 2014 USGS surveys (NOAA Digital Coast, Entwine tiles) | Deck heights on bridges and elevated ways | Public Domain |
| NYC 2018 orthoimagery (maps.nyc.gov tiles), NY State orthoimagery | Checks only: the ramp to crossing rule, routes drawn by hand | Public (viewing) |

The combined dataset is **ODbL-1.0** by inheritance from OSM. Pipeline code is **Apache-2.0**.

## Limits, honest

- **Coverage follows OpenStreetMap.** Where OSM has no separately mapped sidewalk, the graph has none. A ramp can attach only where OSM has a sidewalk, crossing or footway within 5 m: 60.7% of the Bronx's surveyed ramps are on the graph, against 93.8% of Brooklyn's.
- **The graph is fragmented.** 71.7% of pedestrian-graph nodes sit in one component covering Manhattan, Brooklyn, Queens and the Bronx. Staten Island is its own component, and there are thousands of small fragments where OSM ways share no node. 20 of 23 bridges with a pedestrian path are walkable end to end on pedestrian edges; the rest are gaps in OSM, listed with what a mapper would add in `research_notes/next/bridges/osm_mapper_notes.md` (not posted).
- **Incline is an estimate from an airborne survey.** Off structures it comes from the terrain model at 2 m, smoothed along the path over edges shorter than 5 m; on bridges and elevated ways from the deck heights in the LiDAR point clouds. A kerb ramp a metre long is below what it can see. Tunnel edges, and 48 structure nodes whose deck the surveys did not catch, carry none. Structures built after May 2017 are read as what the survey saw then.
- **The ramp data is a 2018 survey.** It says a ramp was there and what DOT measured. DOT's own program data lists 41% of surveyed ramps at corners rebuilt since. The survey's description says its measurements do not establish ADA compliance.
- **A sixth of the ramps are not on the graph.** 36,180 curb nodes nodes carry a surveyed ramp but sit on no edge: no pedestrian vertex within 5 m, or a node shared with another ramp.
- **A ramp counts for a crossing when it lies within 5 m of the crossing's end.** On 200 crossings rated over 2018 imagery, 98.8% of the crossings that rule passes have a surveyed ramp positioned to serve them at both ends. It says a ramp was surveyed there, not that it is usable today.
- **Gap-fill sidewalks are not in the graph.** The planimetric centrelines ship as `nyc-gapfill-sidewalks.geojson`: in a sample of 18, 9 were on a sidewalk or walkway and 4 were plainly wrong.
- **`width` is the mean width of the planimetric polygon,** not the clear width at that spot. It is within about 0.6 m of a local transect at the median.
- **DOT records every ramp as `kerb=lowered`.** The source survey does not distinguish flush from lowered.
- **No live data.** Elevator outages, construction closures, and weather belong in the consuming application.

## Citation

```bibtex
@dataset{rahman_opensidewalks_nyc_2026,
  author       = {Rahman, Adam Munawar},
  title        = {opensidewalks-nyc: An OpenSidewalks v0.3-conformant pedestrian graph of New York City},
  year         = {2026},
  version      = {0.3.3-nyc.1},
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

`v0.3.3-nyc.1`. Deck heights on structures from LiDAR point clouds, a 2 m terrain model, incline smoothed over short edges, street edges in both directions, and the ramp to crossing rule checked against imagery. v0.3.2-nyc.1 was the corrected rebuild of v0.3.1-nyc.1. Issues and PRs welcome, especially around accessibility-feature coverage gaps.
