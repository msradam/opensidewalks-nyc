# data/

This directory is a placeholder. **Built artifacts do not live in the repo tree.** They are distributed as **[GitHub Release assets](https://github.com/msradam/opensidewalks-nyc/releases)**.

The pipeline writes intermediate staged data here (gitignored):

```
data/
├── raw/        # untouched downloads from upstream sources
├── clean/      # cleaned sources and data/clean/cleaning_report.md
└── staged/     # per-stage intermediates, including the pre-export
                # data/staged/nyc-osw-unvalidated.geojson
```

`output/` (sibling, also gitignored) holds the canonical `nyc-osw.geojson` plus derived formats. `scripts/` reads from `output/` to produce release assets.

## Why releases, not LFS

GitHub LFS free-tier quotas fill quickly with several versions of large geodata, and release assets have no such cap. Each release tag pins a reproducible build that can be dated to its source-fetch timestamps. `releases/latest/download/nyc-osw.fgb` always resolves to the newest asset.

Every release asset is ODbL-1.0 except `evaluation-sheets.zip`, whose imagery is NYC orthoimagery (NYC OTI, 2018 and 2024) under CC BY 4.0 with overlays from OpenStreetMap (ODbL) and NYC DOT ramp positions (see [`../LICENSE-DATA.md`](../LICENSE-DATA.md)).

See [`../README.md`](../README.md#download) for the download commands.
