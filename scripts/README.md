# scripts/

Conversion scripts that turn the canonical OSW GeoJSON into the formats shipped on the GitHub Release page.

| Script | Output | Notes |
|---|---|---|
| `split_by_borough.py` | `nyc-osw-{MN,BK,QN,BX,SI}.geojson` | Two-pass streaming. Edges and Pedestrian Zones bucketed by `ext:borough`; nodes included in every borough whose edges or zones reference them, and a node nothing references in the borough of its own `ext:borough`. Root metadata copied. |
| `to_flatgeobuf.py` | `nyc-osw.fgb` | FlatGeobuf via pyogrio + GDAL. Spatially indexed. Holds the Nodes, the Edges and the Pedestrian Zone polygons. FlatGeobuf has no list type, so a zone's `_w_id` is written as a JSON array in a string. Licence and credit in the layer description. |
| `to_graphml.py` | `nyc-osw.graphml` | NetworkX GraphML. Directed multigraph, one edge per OSW edge. Each Pedestrian Zone is written as the edges a person can walk across it: its ring and the straight chords between its entrances that stay inside the polygon, each with `ext:zone`. Properties coerced to GraphML primitives. |
| `to_graphml.py --undirected` | `nyc-osw-undirected.graphml` | A simple undirected graph, one edge per pair of nodes (the first the file holds for that pair), with `_u_id` and `_v_id` kept as attributes so `incline` can still be read the right way round. The reverse direction and parallel edges are gone. |
| `to_routing_json.py` | `nyc-routing.json` | Flat nodes dict and edges list, with edge lengths. Pedestrian Zones are expanded into ring and chord edges as in the GraphML. |
| `osw_to_unweaver.py` | Input layer for [Unweaver](https://github.com/nbolten/unweaver) (Nick Bolten, Apache-2.0) | Applies the 5 m ramp to crossing rule (see `METHODOLOGY.md`) and expands each Pedestrian Zone into ring and chord edges, walked like footways. The cost function in `unweaver-project/` is adapted from Unweaver's example wheelchair profile. Build the Unweaver graph with `unweaver build PROJECT --changes-sign incline`: Unweaver adds a reversed copy of each edge, and without that flag a climb passes as a descent. The dataset already stores both directions, so those copies are parallel duplicates; they do not change route lengths. |
| `osw_to_osm.py` | OSM XML | Converts the graph back to OSM tags for OpenRouteService, with the ramp rule written on the crossing ends. A Pedestrian Zone becomes a closed way with `area=yes`. Used by the router comparison. For local routing only: never upload the output to OpenStreetMap. It contains NYC DOT data and synthetic `kerb` tags, and its ids are not OpenStreetMap's. |

The release conversions read the canonical `nyc-osw.geojson` produced by `python -m pipeline build` and then snapped by `scripts/snap_endpoints.py`. Run them after the snap, so every asset agrees with the canonical file. From v0.3.5 the snap also moves each Pedestrian Zone's ring vertices onto their Nodes and adds `nyc.zones.geojson` to the validator ZIP.

## Run all conversions

```bash
INPUT=output/nyc-osw.geojson
OUT=release-assets
mkdir -p "$OUT"

python scripts/split_by_borough.py "$INPUT" "$OUT"
python scripts/to_flatgeobuf.py "$INPUT" "$OUT/nyc-osw.fgb"
python scripts/to_graphml.py "$INPUT" "$OUT/nyc-osw.graphml"
python scripts/to_graphml.py "$INPUT" "$OUT/nyc-osw-undirected.graphml" --undirected
python -m scripts.to_routing_json "$INPUT" "$OUT"
cp "$INPUT" "$OUT/nyc-osw.geojson"
cp output/nyc-osw-osw-split.zip "$OUT/"
cp output/nyc-gapfill-sidewalks.geojson "$OUT/"   # not part of the graph; its root says what it is

# The release ships the large files gzipped; the checksums are of what ships.
cd "$OUT"
gzip -k -9 nyc-osw.geojson nyc-osw-??.geojson nyc-osw.graphml nyc-osw-undirected.graphml nyc-routing.json

# The rating sheets. Copy the ZIP in before taking the checksums, so
# SHA256SUMS lists it (it was missing from the sums of v0.3.4).
cp /path/to/evaluation-sheets.zip .
shasum -a 256 *.gz nyc-osw.fgb nyc-osw-osw-split.zip nyc-gapfill-sidewalks.geojson evaluation-sheets.zip > SHA256SUMS
```

## The rating sheets

`evaluation-sheets.zip` holds all 248 rating sheets and a README: the 200 crossing sheets behind `evaluation/crossing_rule/` and the 48 disagreement sheets behind `evaluation/compare/disagreements/`. The crossing sheets were drawn by `evaluation/crossing_rule/code/sample.py` and the disagreement sheets by `evaluation/compare/disagreements/code/sheets.py`. Both read inputs from the working archive, so the ZIP is copied from one release to the next and is not rebuilt. The ZIP holds those two sets of sheets as `crossing_rule/NNN.jpg` and `disagreements/`. No script records how it was packed. It is a release asset from v0.3.4 and is in `SHA256SUMS` from v0.3.5.

The sheets are not ODbL. Their imagery is NYC orthoimagery (NYC OTI, 2018 and 2024) under CC BY 4.0, and the lines and points drawn on it are OpenStreetMap data (ODbL) and NYC DOT ramp positions.

## The Brownsville demo data

`build_demo_data.py demo/build_config.json` writes `demo/data/*.js`. Its inputs, listed in `demo/build_config.json`, are build-time paths under `research_notes/`, the project's local and git-ignored working archive: the Brownsville clip, the graph arrays, the pairs and routes of the router comparison, DOT's saved ramp assessment pages and the progress CSV. Those files are large or carry third-party material, so they are not in the repository; the code that makes the comparison's inputs is in `compare/`, and the summary results are in `evaluation/`. The demo's prose for its status panel and sources table is in the same config file.
