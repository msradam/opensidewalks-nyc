# Data license: ODbL-1.0

Every data file distributed in the GitHub Releases of this repository is licensed under the **[Open Database License v1.0 (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/)**. That covers `nyc-osw.geojson`, `nyc-osw.fgb`, both GraphML files, the routing JSON, the per-borough splits, the validator ZIP, `nyc-gapfill-sidewalks.geojson` and any derived format. The same licence covers the demo's data files under `demo/data/` and the result files under `evaluation/`, except the DOT assessment statuses described under "Sources used only in the demo".

The licence is inherited from OpenStreetMap, which is the source of the network geometry (footways, crossings, steps, road centrelines). Under ODbL section 4.4, a database derived from an ODbL database must itself be offered under ODbL, so this dataset is released under ODbL.

OpenSidewalks NYC is an independent project by Adam Munawar Rahman. It is not made or endorsed by the Taskar Center for Accessible Technology, OpenSidewalks or TDEI, nor by NYC DOT or the City of New York.

## Attribution

If you use this dataset, you must:

1. Credit **OpenStreetMap contributors**, with a link to [openstreetmap.org/copyright](https://www.openstreetmap.org/copyright).
2. Credit **OpenSidewalks NYC** with a link to this repository or to a release.
3. Indicate that the data is licensed under ODbL-1.0.
4. Where applicable, also credit:
   - **New York City Department of Transportation** for the curb-ramp survey (`ufzp-rrqu`), which a contractor (Cyclomedia) collected for DOT from vehicle-mounted street-level imagery and LiDAR, with records captured March 2017 to January 2020, mostly in 2018
   - **New York City Office of Technology and Innovation** for the planimetric sidewalks (`52n9-sdep`) and the 2017 topobathymetric LiDAR elevation model, served by the **New York State GIS Program Office**
   - **NOAA Digital Coast**, which serves the LiDAR point clouds of the 2017 NYC survey (NYC OTI) and the 2014 survey of the **U.S. Geological Survey** as Entwine tiles, used for deck heights on bridges and elevated ways

The borough boundaries in the region polygon are OpenStreetMap data (fetched through Nominatim), so they fall under the OpenStreetMap credit.

The attribution string, as written into the root of every release file:

> Pedestrian network from OpenSidewalks NYC, ODbL-1.0. Map data © OpenStreetMap contributors (openstreetmap.org/copyright). Also from NYC DOT and NYC OTI data on NYC Open Data, and LiDAR from NYS GIS and NOAA.

The files of v0.3.3-nyc.1 carry an older, shorter string without the copyright link and NOAA. Files in releases up to v0.3.4-nyc.1 spell the name `opensidewalks-nyc` in this line. Use the string above for them too.

## What ODbL requires of you (summary, not legal advice)

A database you derive from this one and share must also be released under ODbL. You must give the attribution above. If you publicly distribute the database or a substantial extract, distribute it openly; you may add technical protection only if you also provide an unrestricted copy. If you publish a map or route made from a modified version, you must also offer the modified database or a file of your changes (ODbL section 4.6).

A work produced from the data (a map image, a route, a research paper) is a "Produced Work" under ODbL. It does not have to be ODbL, but it must include the attribution.

## Code license

Pipeline code, scripts, and notebooks in this repository are licensed under **[Apache-2.0](LICENSE)**, separately from the data.

## Sources and their terms

The NYC Open Data sources (`ufzp-rrqu`, `52n9-sdep`) and the NYC 2017 LiDAR elevation model carry no licence. NYC Administrative Code 23-502(d) requires city public data sets to be available "without any registration requirement, license requirement or restrictions on their use", and lets the city ask redistributors to name the source and version and describe any modifications. This project treats them as free of licence requirements and use restrictions under that section, and names each source and its retrieval date in the feature provenance. The LiDAR point clouds on NOAA's archive (the 2014 USGS survey and the 2017 city survey) are published there with no licence attached, and this project treats them the same way. Their inclusion does not change the ODbL share-alike requirement on the combined database, because the ODbL governs any database substantially derived from OpenStreetMap.

### Sources used only in the demo

| Source | Terms |
|---|---|
| NYC DOT survey assessment map layer (`CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD`) on DOT's ArcGIS service, linked from [nycpedramps.info/survey](https://www.nycpedramps.info/survey), read 2 October 2026 | No stated terms. It is not an NYC Open Data data set, and this project does not relicense the statuses taken from it. |
| NYC DOT Pedestrian Ramp Program Progress (`e7gc-ub6z`), read 2 October 2026 | NYC Open Data terms of use |
| NYC DCP Community Districts (`5crt-au7u`) | NYC Open Data terms of use |
| NYC DCP Facilities Database (`ji82-xba5`) | NYC Open Data terms of use |
| NYCHA Public Housing Developments (`phvi-damg`) | NYC Open Data terms of use |
| NYC Parks Cool It! NYC 2020 cooling sites (`h2bn-gu9k`) | NYC Open Data terms of use |
| MTA Subway Entrances and Exits 2024 (`i9wp-a4ja`, data.ny.gov) | New York State open data terms of use |
| NYC OTI, NYC Orthos 2024 (used for the imagery checks, not shown) | CC BY 4.0 |
