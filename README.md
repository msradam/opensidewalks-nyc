# opensidewalks-nyc

A pedestrian graph of New York City in the [OpenSidewalks v0.3](https://github.com/OpenSidewalks/OpenSidewalks-Schema) schema: sidewalks, crossings, footways, steps and curb ramps, with incline on every edge. Built from OpenStreetMap, the NYC DOT curb ramp survey, NYC sidewalk polygons and the city's LiDAR.

**Experimental. Nothing has been checked on the ground, so do not present routes from it as wheelchair accessible.**

[Brownsville demo](https://msradam.github.io/opensidewalks-nyc/) · [Download](https://github.com/msradam/opensidewalks-nyc/releases/latest) · [Methodology](METHODOLOGY.md) · [Evidence](evaluation/)

| | |
|---|---|
| Features | 4,068,058 (1,186,910 nodes, 2,881,148 directed edges) |
| Curb ramps | 217,679 from the 2018 to 2019 DOT survey, with measured slopes; 83% attached to the graph |
| Incline | 2017 LiDAR terrain model at 2 m; deck heights from the LiDAR point clouds on bridges |
| Validation | `python-osw-validation` 0.5.0, zero errors |
| OSM data | Geofabrik extract of 2026-10-01 |
| Licence | Data ODbL-1.0 ([LICENSE-DATA.md](LICENSE-DATA.md)), code Apache-2.0 |

## Download

```bash
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.geojson.gz
```

Also on the release page: FlatGeobuf, directed and undirected GraphML, a routing JSON, per-borough splits, the validator ZIP and `SHA256SUMS`. Property reference: [SCHEMA.md](SCHEMA.md).

```python
import geopandas as gpd
gdf = gpd.read_file("nyc-osw.fgb", bbox=(-73.99, 40.74, -73.97, 40.76))
```

## How it compares

The same 12,437 trips were routed with this graph's wheelchair profile and with OpenRouteService, Valhalla and Unweaver, all run locally on the same OSM extract ([protocol](evaluation/compare/PROTOCOL.md)).

| | Brooklyn | Queens | Manhattan | Bronx | Staten Island |
|---|---|---|---|---|---|
| This graph finds a route | 91% | 68% | 64% | 51% | 50% |
| OpenRouteService wheelchair finds a route | 99% | 98% | 98% | 97% | 98% |
| Of its routes, share that cross with no surveyed ramp, exceed this profile's incline limits, or use the roadway | 91% | 97% | 97% | 98% | 98% |

Unlike OpenRouteService and Valhalla, this graph uses a ramp survey and measured incline. It pays for that by answering less often. The comparison does not show that it routes a wheelchair user better; that takes a field audit, which is the next step.

## Limits

- The ramp data is a 2018 survey. It says a ramp was there, not that it is usable today.
- Coverage follows OSM: no mapped sidewalk, no edge. The graph is fragmented, and Staten Island is its own component.
- A crossing counts as ramped when a surveyed ramp is within 5 m of each end. That rule was checked on 200 crossings over aerial imagery by language-model raters, not by people and not on site ([evidence](evaluation/crossing_rule/)).
- Incline is an airborne estimate. A one-metre kerb ramp is below what it can see.
- The routing profile ignores OSM's surface, smoothness and `wheelchair=no` tags.

The full list, with numbers, is in [evaluation/README.md](evaluation/README.md).

## Build it

```bash
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
python -m pipeline build          # about 48 min, 35 GB peak memory
```

[METHODOLOGY.md](METHODOLOGY.md) describes each stage and [scripts/README.md](scripts/README.md) turns a build into release files.

## Credits

The schema is by the [Taskar Center for Accessible Technology](https://sidewalks.washington.edu/), University of Washington. Data from OpenStreetMap contributors, NYC Open Data, NYC DOT, NYC OTI, NY State GIS and NOAA. Parts of the code and docs were drafted with language models and reviewed by the author; see [NOTICE](NOTICE).

Cite with [CITATION.cff](CITATION.cff). Issues welcome.
