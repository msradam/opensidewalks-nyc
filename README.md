# _opensidewalks-nyc_<!-- omit from toc -->

An experimental pedestrian network dataset of New York City in the [OpenSidewalks Schema](https://github.com/OpenSidewalks/OpenSidewalks-Schema) v0.3.

[Demo](https://msradam.github.io/opensidewalks-nyc/) · [Download](https://github.com/msradam/opensidewalks-nyc/releases/latest) · [Evidence](evaluation/)

![Pedestrian edges in Washington Heights and Inwood, coloured by incline](docs/img/incline-washington-heights.png)

## Table of Contents<!-- omit from toc -->

- [Introduction](#introduction)
- [Dataset Contents](#dataset-contents)
	- [Edges](#edges)
	- [Nodes](#nodes)
	- [Fields](#fields)
	- [Network Topology](#network-topology)
- [Data Sources](#data-sources)
- [Download](#download)
- [Validation](#validation)
- [Evaluation](#evaluation)
	- [Comparison with Other Routers](#comparison-with-other-routers)
- [Brownsville Demo](#brownsville-demo)
- [Limitations](#limitations)
- [Building the Dataset](#building-the-dataset)
- [License and Attribution](#license-and-attribution)
- [Versions](#versions)

# Introduction

<a id="introduction"></a>

opensidewalks-nyc is a pedestrian network of all five boroughs of New York City: sidewalks, street crossings, footways, steps and curb ramps, encoded as the Nodes and Edges of the OpenSidewalks Schema so that the data can be loaded directly as a routable graph.

Following the OpenSidewalks approach, this dataset does not label any path as wheelchair accessible. It stores what was measured (the incline of each Edge, sidewalk width, the slopes and warning surface of each surveyed curb ramp) so that an application can interpret those values against a person's own needs, with rules like "no incline greater than 8.3 percent".

This is an experimental dataset. It is derived from OpenStreetMap and public city surveys, and none of it has been checked on the ground. It should not be used to tell anyone that a route is accessible.

# Dataset Contents

<a id="dataset-contents"></a>

The dataset holds 4,068,058 features: 2,881,148 Edges and 1,186,910 Nodes. Every Edge is directed, with `incline` signed in its direction of travel, and every segment appears once in each direction.

![Sidewalk, Crossing, Footway and Steps Edges with Curb Nodes in Downtown Brooklyn](docs/img/network-structure.png)

## Edges

<a id="edges"></a>

| Entity | Tags | Count |
|---|---|---|
| Sidewalk | `highway=footway`, `footway=sidewalk` | 933,110 |
| Crossing | `highway=footway`, `footway=crossing` | 437,510 |
| Footway and Steps | `highway=footway` (other), `highway=steps` | 560,142 |
| Street | `highway=residential`, `service`, `secondary`, `unclassified` and other road classes | 950,386 |

## Nodes

<a id="nodes"></a>

| Entity | Tags | Count |
|---|---|---|
| Curb Ramp | `barrier=kerb`, `kerb=lowered`, with the NYC DOT survey fields | 217,679 |
| Node | Edge endpoints | 969,231 |

## Fields

<a id="fields"></a>

Edges carry `incline`, `width`, `surface`, `name`, `crossing:markings` and, on bridges, tunnels and elevated ways, `ext:structure`. Curb Ramps carry `tactile_paving` and the survey's running, cross and counter slopes. Nodes carry `ext:elevation_m`. Every feature carries `ext:source` and `ext:pipeline_version`. The full field list is in [SCHEMA.md](SCHEMA.md).

## Network Topology

<a id="network-topology"></a>

As the schema asks, curb ramps are mapped at Edge endpoints where a Crossing meets a Sidewalk. 181,499 of the 217,679 surveyed ramps (83%) sit at an Edge endpoint. The rest have no pedestrian vertex within 5 m and are kept as Nodes on no Edge.

# Data Sources

<a id="data-sources"></a>

| Source | Contributes | License |
|---|---|---|
| OpenStreetMap, Geofabrik extract of 2026-10-01 | Footways, crossings, steps, streets, topology | ODbL-1.0 |
| NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`), surveyed 2018 to 2019 | Curb ramps with measured slopes and warning surface | NYC Open Data terms of use |
| NYC Planimetric Sidewalks (`52n9-sdep`), 2022 capture | Sidewalk widths | NYC Open Data terms of use |
| NYC 2017 LiDAR terrain model (NY State GIS), read at 2 m | Node elevation and Edge incline | Public, no licence attached |
| 2017 NYC and 2014 USGS LiDAR point clouds (NOAA) | Deck heights on bridges and elevated ways | Public, no licence attached |

[METHODOLOGY.md](METHODOLOGY.md) describes how each source is read and joined.

# Download

<a id="download"></a>

Each [release](https://github.com/msradam/opensidewalks-nyc/releases/latest) holds the dataset as OpenSidewalks GeoJSON, FlatGeobuf, directed and undirected GraphML, a routing JSON, per-borough GeoJSON, the validator ZIP and `SHA256SUMS`.

```bash
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.geojson.gz
```

```python
import geopandas as gpd
edges = gpd.read_file("nyc-osw.fgb", bbox=(-73.99, 40.74, -73.97, 40.76))
```

# Validation

<a id="validation"></a>

The release passes the OpenSidewalks validator, [`python-osw-validation`](https://pypi.org/project/python-osw-validation/) 0.5.0, with zero errors. The validator checks the schema, not whether the data matches the street. The checks it does not do (elevation, deck heights, connectivity by borough, ramps against the survey, widths) are in [`validators/post_build_checks.py`](validators/post_build_checks.py) and their results in [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md).

# Evaluation

<a id="evaluation"></a>

The protocols, ratings and result files behind every number here are in [`evaluation/`](evaluation/). Some checks were rated over aerial imagery by language-model agents following a written protocol. No person rated them and nothing was checked on the ground.

## Comparison with Other Routers

<a id="comparison-with-other-routers"></a>

The same 12,437 trips were routed with this dataset's wheelchair profile and with OpenRouteService and Valhalla, all run locally on the same OpenStreetMap extract ([protocol](evaluation/compare/PROTOCOL.md)).

| | Brooklyn | Queens | Manhattan | Bronx | Staten Island |
|---|---|---|---|---|---|
| This dataset finds a route | 91% | 68% | 64% | 51% | 50% |
| OpenRouteService wheelchair finds a route | 99% | 98% | 98% | 97% | 98% |
| Of its routes, share that cross with no surveyed ramp, exceed the incline limits, or use the roadway | 91% | 97% | 97% | 98% | 98% |

This dataset uses a ramp survey and measured incline, which OpenRouteService and Valhalla do not, and so answers less often. The comparison does not show that it routes a wheelchair user better. That needs a field audit.

# Brownsville Demo

<a id="brownsville-demo"></a>

The [demo](https://msradam.github.io/opensidewalks-nyc/) shows Brooklyn Community District 16: sidewalks by incline, crossings, every surveyed curb ramp by NYC DOT's assessment, and ten trips routed by this dataset and by OpenRouteService. Every route is also written out as text. The page passes axe-core and pa11y with zero violations and has not been tested by a screen-reader user.

![The demo's opening view with its warning](docs/img/demo-overview.png)

![A trip routed three ways: this dataset in blue, OpenRouteService wheelchair in orange, OpenRouteService walking in black](docs/img/demo-trip.png)

# Limitations

<a id="limitations"></a>

- The curb ramp data is a 2018 to 2019 survey. It records that a ramp was there, not that it is usable today.
- Coverage follows OpenStreetMap. Where OSM has no separately mapped sidewalk, the dataset has none. The network is fragmented, and Staten Island is its own component.
- A Crossing counts as ramped when a surveyed ramp lies within 5 m of each end. That rule was checked on 200 crossings over aerial imagery by language-model raters ([evidence](evaluation/crossing_rule/)).
- Incline is estimated from an airborne survey. A kerb ramp a metre long is below what it can resolve.
- The wheelchair profile does not read OSM's `surface`, `smoothness` or `wheelchair=no` tags.

The full list, with numbers, is in [evaluation/README.md](evaluation/README.md#what-this-evidence-does-not-show).

# Building the Dataset

<a id="building-the-dataset"></a>

```bash
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
python -m pipeline build          # about 48 minutes, 35 GB peak memory
```

[METHODOLOGY.md](METHODOLOGY.md) describes each stage, and [scripts/README.md](scripts/README.md) turns a build into release files.

# License and Attribution

<a id="license-and-attribution"></a>

Data is ODbL-1.0, inherited from OpenStreetMap ([LICENSE-DATA.md](LICENSE-DATA.md)). Code is Apache-2.0. Cite with [CITATION.cff](CITATION.cff).

The OpenSidewalks Schema is developed by the [Taskar Center for Accessible Technology](https://sidewalks.washington.edu/) at the University of Washington. Data comes from OpenStreetMap contributors, NYC Open Data, NYC DOT, NYC OTI, NY State GIS and NOAA. Parts of the code and documentation were drafted with language models and reviewed by the author ([NOTICE](NOTICE)).

# Versions

<a id="versions"></a>

| Version | Release Date | Link | Notes |
|---|---|---|---|
| 0.3.3-nyc.1 | 2026-10-03 | [GitHub](https://github.com/msradam/opensidewalks-nyc/releases/tag/v0.3.3-nyc.1) | First public release. OpenSidewalks Schema v0.3 |
