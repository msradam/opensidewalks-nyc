# Data license: ODbL-1.0

The graph data distributed via GitHub Releases of this repository (`nyc-osw.geojson`, `nyc-osw.fgb`, `nyc-osw.graphml`, the per-borough splits, and any derived format) is licensed under the **[Open Database License v1.0 (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/)**.

This license is **inherited from OpenStreetMap**, which is the source for the topological scaffold (footways, crossings, road centerlines). Under ODbL Section 4.4 ("Share-alike"), any database derived from an ODbL database must itself be made available under ODbL. We comply by releasing this dataset under ODbL.

## Attribution

If you use this dataset, you must:

1. Credit **OpenStreetMap contributors** (per [OSM's attribution guidelines](https://www.openstreetmap.org/copyright)).
2. Credit **opensidewalks-nyc** with a link to this repository or to a release.
3. Indicate that the data is licensed under ODbL-1.0.
4. Where applicable, also credit:
   - **New York City Department of Transportation** for the curb-ramp survey (`ufzp-rrqu`)
   - **New York City Office of Technology and Innovation** for the planimetric sidewalks (`52n9-sdep`) and the 2017 topobathymetric LiDAR elevation model, served by the **New York State GIS Program Office**

The borough boundaries in the region polygon are OpenStreetMap data (fetched through Nominatim), so they fall under the OpenStreetMap credit.

A suggested attribution string:

> Pedestrian network derived from opensidewalks-nyc (ODbL-1.0). © OpenStreetMap contributors, NYC Open Data, NYS GIS.

## What ODbL requires of you (summary, not legal advice)

- **Share-alike**: derived databases must be ODbL.
- **Attribution**: as above.
- **Keep open**: if you publicly distribute the database (or substantial extracts), distribute it openly. You may apply technical protection only if you also provide an unrestricted copy.

Use of the data to *produce* a work (a map image, a route, a research paper) is "Produced Work" under ODbL — that work doesn't have to be ODbL, but it must include the attribution.

## Code license

Pipeline code, scripts, and notebooks in this repository are licensed under **[Apache-2.0](LICENSE)**, separately from the data.

## Public-domain inputs

The NYC Open Data sources (`ufzp-rrqu`, `52n9-sdep`) and the NYC 2017 LiDAR elevation model carry no licence. NYC Administrative Code 23-502(d) requires city public data sets to be available "without any registration requirement, license requirement or restrictions on their use", and lets the city ask redistributors to name the source and version and describe any modifications. This project treats them as public domain and names each source and its retrieval date in the feature provenance. Their inclusion does not change the ODbL share-alike requirement on the combined database, because OSM ODbL terms govern any database substantially derived from OSM.
