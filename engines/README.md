# Engines for the router comparison

These are the Dockerfiles and configurations used to build the three reference engines in the router comparison (`evaluation/compare/`). Built graphs, tiles and extracts are not kept here: they are large and regenerate from the pinned OSM extract and the tracked scripts. Versions, image digests, build times and checksums are in `evaluation/compare/results/engines.json`. No public routing API was used; the runners in `compare/` refuse any base URL that is not `localhost`.

## The OSM extract

All engines start from Geofabrik's `new-york-261001.osm.pbf` (SHA-256 `1214968b2c988e13ee6a6bde6b9c9891337a588475b7fd2ac1939e175073d8b7`, the extract pinned in `config/sources.yaml`), clipped to the five boroughs:

```sh
docker build -t osmium engines/osm
docker run --rm -v "$PWD:/w" -w /w osmium \
  osmium extract --bbox=-74.28,40.48,-73.68,40.93 --strategy=complete_ways \
  new-york-261001.osm.pbf -o engines/osm/nyc-261001.osm.pbf
shasum -a 256 -c engines/osm/nyc-261001.sha256
```

## OpenRouteService 10.0.1

The image is pinned by digest in `ors/run.sh`. Each arm has its own folder with `config/ors-config.yml`; put the arm's source file in `ors/ARM/files/` before the first start, and ORS builds its graph into `ors/ARM/graphs/`.

| Arm | Source file in `files/` | Made by |
|---|---|---|
| `armA` | `nyc-261001.osm.pbf` | the clip above |
| `armB_strict` | `nyc-osw-strict.osm.pbf` | `python scripts/osw_to_osm.py --input output/nyc-osw.geojson --output ... --no-ramp raised` |
| `armB_known` | `nyc-osw-known.osm.pbf` | the same with `--no-ramp unknown` |
| `armC` | `nyc-osw-pedestrian.osm.pbf` | the same as strict with `--pedestrian-only` |
| `probe` | `probe.osm.pbf` | `python compare/ors_tag_probe.py write ...` |

`ors/run.sh ARM PORT` starts an arm in the foreground with `REBUILD_GRAPHS=False`, for restarts on a built graph. `ors/run_detached.sh ARM PORT` starts it in the background with the image's default and smaller heap. `ors/probe.sh` starts the tag probe on port 8083. Then `compare/run_ors.py` and `compare/ors_tag_probe.py ask` send the requests.

## Valhalla 3.9.0

Valhalla runs in process from the `pyvalhalla` wheel, so there is no Dockerfile. `valhalla/valhalla.json` is the configuration `compare/run_valhalla.py build` writes: `pyvalhalla` defaults, with `service_limits.pedestrian.max_distance` raised to 250000 and `tile_dir` set to `engines/valhalla/tiles`. The `ipc:///tmp/...` entries are Valhalla's own default socket addresses, not paths on the machine that ran the comparison.

```sh
uv run --no-project --isolated --python 3.12 --with pyvalhalla==3.9.0 \
  python compare/run_valhalla.py build engines/osm/nyc-261001.osm.pbf engines/valhalla
```

## Unweaver

`unweaver/Dockerfile` builds Unweaver at commit `66352c1` (2022-11-02) on Debian, which supplies `mod_spatialite` inside the container. A project folder is the output of `scripts/osw_to_unweaver.py` (the routing layer and `regions.geojson`) plus the four cost and profile files in `unweaver-project/`, unchanged. Build it with `unweaver build PROJECT --changes-sign incline`. Without `--changes-sign incline` Unweaver copies each edge reversed with its incline unchanged, so a climb passes as a descent (`evaluation/compare/results/unweaver_bk16_without_changes_sign.json`). Then `compare/run_unweaver.py` sends the requests.

## Not included

The converted extracts, the ORS graphs, the Valhalla tiles and the Unweaver graphs (about 5 GB in all). The upstream ORS example configuration files that ship in the image are also left out.
