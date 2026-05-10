# data/

This directory is a placeholder. **Built artifacts do not live in the repo tree** — they are distributed as **[GitHub Release assets](https://github.com/msradam/opensidewalks-nyc/releases)**.

The pipeline writes intermediate staged data here (gitignored):

```
data/
├── raw/        # untouched downloads from upstream sources
├── staged/     # per-stage intermediates (post-clean, post-schema-map, ...)
└── clean/      # final pre-export FeatureCollection (also gitignored)
```

`output/` (sibling, also gitignored) holds the canonical `nyc-osw.geojson` plus derived formats. `scripts/` reads from `output/` to produce release assets.

## Why releases, not LFS

- **No size cap.** GitHub LFS free-tier quotas are easy to blow through with multi-version geo data; releases have no such ceiling.
- **Versioned downloads.** Each release tag pins a reproducible build, datable to a specific source-fetch timestamp.
- **Direct URLs.** `releases/latest/download/nyc-osw.fgb` always resolves to the newest asset.

See [`../README.md`](../README.md) for the curl one-liners.
