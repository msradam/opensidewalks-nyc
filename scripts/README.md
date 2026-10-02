# scripts/

Conversion scripts that turn the canonical OSW GeoJSON into the formats shipped on the GitHub Release page.

| Script | Output | Notes |
|---|---|---|
| `split_by_borough.py` | `nyc-osw-{MN,BK,QN,BX,SI}.geojson` | Two-pass streaming. Edges bucketed by `ext:borough`; nodes included in every borough whose edges reference them, and a node no edge references in the borough of its own `ext:borough`. Root metadata copied. |
| `to_flatgeobuf.py` | `nyc-osw.fgb` | FlatGeobuf via pyogrio + GDAL. Spatially indexed. Licence and credit in the layer description. |
| `to_graphml.py` | `nyc-osw.graphml` | NetworkX GraphML. Directed multigraph, one edge per OSW edge. Properties coerced to GraphML primitives. |
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
python -m scripts.to_routing_json "$INPUT" "$OUT"
cp "$INPUT" "$OUT/nyc-osw.geojson"
cp output/nyc-osw-osw-split.zip "$OUT/"

# The release ships the large files gzipped; the checksums are of what ships.
cd "$OUT"
gzip -k -9 nyc-osw.geojson nyc-osw-??.geojson nyc-osw.graphml nyc-routing.json
shasum -a 256 *.gz nyc-osw.fgb nyc-osw-osw-split.zip > SHA256SUMS
```
