# _opensidewalks-nyc_<!-- omit from toc -->

An experimental pedestrian network dataset of New York City in the [OpenSidewalks Schema](https://github.com/OpenSidewalks/OpenSidewalks-Schema) v0.3. Nothing in it has been checked on the ground. Do not use it to tell anyone that a route is accessible.

opensidewalks-nyc is an independent project by Adam Munawar Rahman. It is not made or endorsed by the Taskar Center for Accessible Technology, OpenSidewalks or TDEI, nor by NYC DOT or the City of New York. Report a problem in [GitHub issues](https://github.com/msradam/opensidewalks-nyc/issues).

[Demo](https://msradam.github.io/opensidewalks-nyc/) · [How it is built](https://msradam.github.io/opensidewalks-nyc/how-it-works.html) · [Download](https://github.com/msradam/opensidewalks-nyc/releases/latest) · [Evidence](evaluation/)

![Pedestrian edges in Washington Heights and Inwood, coloured by incline](docs/img/incline-washington-heights.png)

## Table of Contents<!-- omit from toc -->

- [Introduction](#introduction)
- [Dataset Contents](#dataset-contents)
	- [Edges](#edges)
	- [Nodes](#nodes)
	- [Fields](#fields)
	- [Network Topology](#network-topology)
	- [Known Deviations from the Schema](#known-deviations-from-the-schema)
- [Data Sources](#data-sources)
- [Download](#download)
- [Validation](#validation)
- [Evaluation](#evaluation)
	- [Comparison with Other Routers](#comparison-with-other-routers)
- [Brownsville Demo](#brownsville-demo)
- [Limitations](#limitations)
- [Related Work](#related-work)
- [Building the Dataset](#building-the-dataset)
- [How This Was Made](#how-this-was-made)
- [License and Attribution](#license-and-attribution)
- [Versions](#versions)

# Introduction

<a id="introduction"></a>

opensidewalks-nyc is a pedestrian network of all five boroughs of New York City: sidewalks, street crossings, footways, steps and curb ramps, encoded as OpenSidewalks Nodes and Edges so that it loads as a routable graph.

Following the OpenSidewalks approach, this dataset labels no path as wheelchair accessible. It stores values that an application can read against a person's own needs, with rules like "no incline greater than 8.3 percent". Only the curb ramp slopes are measurements, made by NYC DOT's survey. Incline and width are estimates.

# Dataset Contents

<a id="dataset-contents"></a>

The dataset holds 4,068,058 features: 2,881,148 Edges and 1,186,910 Nodes. Every Edge is directed, with `incline` signed in its direction of travel.

![Sidewalk, Crossing, Footway and Steps Edges with Curb Nodes in Downtown Brooklyn](docs/img/network-structure.png)

## Edges

<a id="edges"></a>

| Entity | Tags | Count |
|---|---|---|
| Sidewalk | `highway=footway`, `footway=sidewalk` | 933,110 |
| Crossing | `highway=footway`, `footway=crossing` | 437,510 |
| Footway | `highway=footway` with no `footway` subtag | 544,672 |
| Steps | `highway=steps` | 15,470 |
| Motor vehicle roads | `highway=residential`, `service`, `secondary`, `unclassified` and other road classes | 950,386 |

## Nodes

<a id="nodes"></a>

| Entity | Tags | Count |
|---|---|---|
| Curb Ramp | `barrier=kerb`, `kerb=lowered`, with the NYC DOT survey fields | 217,679 |
| Bare Node | Edge endpoints with no other entity type | 969,231 |

## Fields

<a id="fields"></a>

Edges carry `incline`, `width`, `surface`, `name`, `crossing:markings` and `ext:structure`. Curb Ramps carry `tactile_paving` and the survey's slopes. Nodes carry `ext:elevation_m`. [SCHEMA.md](SCHEMA.md) lists every field.

## Network Topology

<a id="network-topology"></a>

The schema puts curb ramps at Edge endpoints and expects a Footway between a Sidewalk and a Crossing. This dataset follows OpenStreetMap's geometry, which joins many Crossings directly to Sidewalks: in Manhattan, 52% of the nodes on a Crossing also touch a Sidewalk. Each surveyed ramp is snapped to the nearest pedestrian Edge endpoint within 5 m. 181,499 of the 217,679 ramps (83%) sit on one, 126,152 of them on a Crossing. The other 36,180 are on no Edge, because no vertex lies within 5 m or another ramp took it.

## Known Deviations from the Schema

<a id="known-deviations-from-the-schema"></a>

- Many Crossings join Sidewalks directly, with no Footway between them.
- OSM `highway=pedestrian` ways are written as Footway. Pedestrian Road is not used.
- `crossing:markings` comes from OSM `crossing=*`, not OSM's `crossing:markings` tag. `uncontrolled` is written as `zebra` (108,778 Crossings) and `unmarked` is dropped.
- No Edge carries `foot`, so every road Edge has unknown pedestrian access.
- `tactile_paving=yes` includes defective and misplaced warning surfaces, and DOT's raw condition is not kept.
- OSM node tags (`kerb`, elevators) are not carried. Every Curb Ramp comes from the DOT survey.
- Both directions are stored, so applications must not add reverse Edges.
- 36,180 Curb Ramp Nodes are on no Edge.

# Data Sources

<a id="data-sources"></a>

| Source | Contributes | License |
|---|---|---|
| OpenStreetMap, Geofabrik extract of 2026-10-01 | Footways, crossings, steps, streets, topology | ODbL-1.0 |
| NYC DOT Pedestrian Ramp Locations (`ufzp-rrqu`), collected for DOT by Cyclomedia, mostly in 2018 | Curb ramps with slopes and warning surface | NYC Open Data terms of use |
| NYC Planimetric Sidewalks (`52n9-sdep`), 2022 capture | Sidewalk widths | NYC Open Data terms of use |
| NYC 2017 LiDAR terrain model (NY State GIS) | Node elevation and Edge incline | Public, no licence attached |
| 2017 NYC and 2014 USGS LiDAR point clouds (NOAA) | Deck heights on bridges and elevated ways | Public, no licence attached |

[METHODOLOGY.md](METHODOLOGY.md) describes how each source is read and joined. [NOTICE](NOTICE) lists every source the data and the demo use.

# Download

<a id="download"></a>

Each [release](https://github.com/msradam/opensidewalks-nyc/releases/latest) holds GeoJSON, FlatGeobuf, GraphML, a routing JSON, per-borough GeoJSON, the validator ZIP and `SHA256SUMS`.

```bash
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.geojson.gz   # 158 MB
curl -LO https://github.com/msradam/opensidewalks-nyc/releases/latest/download/nyc-osw.fgb          # 1.2 GB, spatially indexed
```

```python
import geopandas as gpd
edges = gpd.read_file("nyc-osw.fgb", bbox=(-73.99, 40.74, -73.97, 40.76))
```

# Validation

<a id="validation"></a>

The release passes [`python-osw-validation`](https://pypi.org/project/python-osw-validation/) 0.5.0 with zero errors. It checks form, not the schema's topology rules or whether the data matches the street. [`validators/post_build_checks.py`](validators/post_build_checks.py) does the other checks (elevation, deck heights, connectivity, ramps, widths), with results in [`validators/QUALITY_REPORT.md`](validators/QUALITY_REPORT.md).

# Evaluation

<a id="evaluation"></a>

The protocols, ratings and result files behind every number here are in [`evaluation/`](evaluation/). Language models rated the aerial imagery ([How This Was Made](#how-this-was-made)).

## Comparison with Other Routers

<a id="comparison-with-other-routers"></a>

The same random trips, 2,000 per borough, were routed locally on one OpenStreetMap extract by this dataset's wheelchair profile, OpenRouteService (ORS) 10.0.1 and Valhalla 3.9.0 ([protocol](evaluation/compare/PROTOCOL.md), [tables](evaluation/compare/results/tables.md)). ORS used its wheelchair profile with the recommended weighting, incline limit 10, kerb limit 0.06 m, elevation off and `kerbs_on_crossings` true. Valhalla used the wheelchair type with elevation off and the default sidewalk weighting. Trip ends were snapped to the nearest usable edge.

| | Brooklyn | Queens | Manhattan | Bronx | Staten Island |
|---|---|---|---|---|---|
| This dataset's wheelchair profile finds a route | 91% | 68% | 64% | 51% | 50% |
| ORS wheelchair finds a route | 99% | 98% | 98% | 97% | 98% |
| Share of ORS routes that fail this project's rules | 91% | 97% | 97% | 98% | 98% |
| ORS given this graph's data and no roadway agrees on whether a route exists | 93% | 93% | 91% | 89% | 90% |

The third row applies this project's rules (a surveyed ramp within 5 m of each crossing end, no slope over 8.3% up or 10% down, no steps, no more than 10 m of roadway) using data ORS did not have: the ramp survey and the LiDAR incline. It measures missing data, not a worse engine, and the fourth row shows that given the same data ORS mostly agrees. "Roadway" includes street centrelines that OSM tags as having a sidewalk.

Part of the gap is in how ORS reads OpenStreetMap. In these runs ORS v10.0.1 did not read `kerb=raised`, and 51% of its Brooklyn routes cross a crossing OSM tags that way. It also reads a bare `kerb:height` of 0.15 or more as centimetres ([ORS #2293](https://github.com/GIScience/openrouteservice/issues/2293)).

Snapping to graph nodes gives lower shares (84.8% in Brooklyn; see [evaluation/README.md](evaluation/README.md)). The comparison does not show that this dataset routes a wheelchair user better.

# Brownsville Demo

<a id="brownsville-demo"></a>

The [demo](https://msradam.github.io/opensidewalks-nyc/) shows Brooklyn Community District 16: sidewalks by incline, crossings, every surveyed curb ramp by NYC DOT's assessment, and ten trips routed by this dataset and by OpenRouteService, each also written out as text. The page passes axe-core and pa11y with zero violations and has not been tested by a screen-reader user.

![The demo's opening view with its warning](docs/img/demo-overview.png)

![A trip routed three ways: this dataset in blue, OpenRouteService wheelchair in orange, OpenRouteService walking in black](docs/img/demo-trip.png)

# Limitations

<a id="limitations"></a>

- The ramp survey was captured from vehicle imagery, mostly in 2018. It shows that a ramp was there, not that it is usable today, and DOT says the data does not establish ADA compliance.
- Where OSM maps sidewalks as `sidewalk=*` tags on the street, a valid OSM scheme, the graph has no sidewalk Edge, because this pipeline does not read those tags. The network is in many pieces, and Staten Island has no pedestrian link to the other boroughs.
- A Crossing counts as ramped when a surveyed ramp lies within 5 m of each end. Language-model raters checked that rule over imagery of 200 crossings ([evidence](evaluation/crossing_rule/)).
- Incline is estimated from airborne LiDAR and can understate the steepest part of an Edge. Width is a polygon's mean width, not the clear width.
- The wheelchair profile does not read OSM's `surface`, `smoothness` or `wheelchair=no` tags, or a ramp's slope or DOT status.

The full list, with numbers, is in [evaluation/README.md](evaluation/README.md).

# Related Work

<a id="related-work"></a>

- [Project Sidewalk](https://projectsidewalk.org/) collects crowdsourced sidewalk accessibility labels over street-level imagery. It has no NYC deployment.
- [AccessMap](https://github.com/TaskarCenterAtUW/AccessMap) is the Taskar Center's routing application for OpenSidewalks data.
- The Taskar Center and partners publish OpenSidewalks datasets for other regions through TDEI.
- [NYCWalks](https://www.nature.com/articles/s44284-025-00383-y) (MIT City Form Lab, 2026) is a connected network of NYC sidewalk, crosswalk and footpath centrelines built from the city's planimetric layer.

# Building the Dataset

<a id="building-the-dataset"></a>

```bash
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
python -m pipeline build          # about 48 minutes, 35 GB peak memory
python scripts/snap_endpoints.py --input output/nyc-osw.geojson
```

[`notebooks/how-it-works.ipynb`](notebooks/how-it-works.ipynb) follows a few blocks through every stage and runs in under a minute without a build ([rendered](https://msradam.github.io/opensidewalks-nyc/how-it-works.html)). [scripts/README.md](scripts/README.md) turns a build into release files.

# How This Was Made

<a id="how-this-was-made"></a>

The author designed and directed the project and reviewed and accepted the code, documents and results. Language models (Anthropic Claude models, run through Claude Code; exact versions were not recorded) drafted most of the code and documentation, did the imagery ratings, and diagnosed the routes and bridges. Every kappa quoted here is agreement between model instances. No person rated imagery, and nothing was checked on the ground. Tools (axe-core, pa11y, `python-osw-validation`, the test suite) checked the rest.

# License and Attribution

<a id="license-and-attribution"></a>

Data is ODbL-1.0 because it is derived from OpenStreetMap ([LICENSE-DATA.md](LICENSE-DATA.md)). Every release file is ODbL, including `nyc-gapfill-sidewalks.geojson`, whose root wrongly says public domain; LICENSE-DATA.md governs, and the label will be fixed next release. Credit "© OpenStreetMap contributors" ([openstreetmap.org/copyright](https://www.openstreetmap.org/copyright)), and release any derived database under ODbL. Code is Apache-2.0. Cite with [CITATION.cff](CITATION.cff).

The OpenSidewalks Schema and `python-osw-validation` are developed by the [Taskar Center for Accessible Technology](https://sidewalks.washington.edu/) at the University of Washington. The routing setup and the wheelchair cost function are adapted from [Unweaver](https://github.com/nbolten/unweaver) (Nick Bolten, Apache-2.0), the engine behind AccessMap, and the limits of 8.3% up and 10% down are the defaults of Unweaver's example wheelchair cost function. This project added refusals for steps and street centrelines. Data comes from OpenStreetMap contributors, NYC DOT, NYC OTI, NY State GIS and NOAA.

# Versions

<a id="versions"></a>

| Version | Release Date | Link | Notes |
|---|---|---|---|
| 0.3.3-nyc.1 | 2026-10-03 | [GitHub](https://github.com/msradam/opensidewalks-nyc/releases/tag/v0.3.3-nyc.1) | First public release. 0.3.3-nyc.1 is this dataset's own version, and it uses OpenSidewalks Schema v0.3. |
