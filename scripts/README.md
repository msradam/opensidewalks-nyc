# scripts/

Conversion scripts that turn the canonical OSW GeoJSON into the formats shipped on the GitHub Release page.

| Script | Output | Notes |
|---|---|---|
| `split_by_borough.py` | `nyc-osw-{MN,BK,QN,BX,SI}.geojson` | Two-pass streaming. Edges bucketed by `ext:borough`; nodes included in every borough whose edges reference them, and a node no edge references in the borough of its own `ext:borough`. Root metadata copied. |
| `to_flatgeobuf.py` | `nyc-osw.fgb` | FlatGeobuf via pyogrio + GDAL. Spatially indexed. Licence and credit in the layer description. |
| `to_graphml.py` | `nyc-osw.graphml` | NetworkX GraphML. Directed multigraph, one edge per OSW edge. Properties coerced to GraphML primitives. |
| `to_graphml.py --undirected` | `nyc-osw-undirected.graphml` | For code written against the undirected file of releases before v0.3.2: a simple undirected graph, one edge per pair of nodes (the first the file holds for that pair), with `_u_id` and `_v_id` kept as attributes so `incline` can still be read the right way round. The reverse direction and parallel edges are gone. |
| `to_routing_json.py` | `nyc-routing.json` | Flat nodes dict and edges list, with edge lengths. |

All scripts read the canonical `nyc-osw.geojson` produced by `python -m pipeline build` and then snapped by `scripts/snap_endpoints.py`. Run them after the snap, so every asset agrees with the canonical file.

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
shasum -a 256 *.gz nyc-osw.fgb nyc-osw-osw-split.zip nyc-gapfill-sidewalks.geojson > SHA256SUMS
```
