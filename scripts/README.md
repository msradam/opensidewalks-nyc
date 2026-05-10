# scripts/

Conversion scripts that turn the canonical OSW GeoJSON into the formats shipped on the GitHub Release page.

| Script | Output | Notes |
|---|---|---|
| `split_by_borough.py` | `nyc-osw-{MN,BK,QN,BX,SI}.geojson` | Two-pass streaming. Edges bucketed by `ext:borough`; nodes included in every borough whose edges reference them. |
| `to_flatgeobuf.py` | `nyc-osw.fgb` | FlatGeobuf via pyogrio + GDAL. Spatially indexed. |
| `to_graphml.py` | `nyc-osw.graphml` | NetworkX GraphML. Undirected. Properties coerced to GraphML primitives. |

All scripts read the canonical `nyc-osw.geojson` produced by `python -m pipeline build`.

## Run all conversions

```bash
INPUT=output/nyc-osw.geojson
OUT=release-assets
mkdir -p "$OUT"

python scripts/split_by_borough.py "$INPUT" "$OUT"
python scripts/to_flatgeobuf.py "$INPUT" "$OUT/nyc-osw.fgb"
python scripts/to_graphml.py "$INPUT" "$OUT/nyc-osw.graphml"
cp "$INPUT" "$OUT/nyc-osw.geojson"

cd "$OUT" && shasum -a 256 *.geojson *.fgb *.graphml > SHA256SUMS
```
