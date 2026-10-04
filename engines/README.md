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

Build each graph once with `ors/run_detached.sh ARM PORT`, which starts it in the background with a 4 GB heap (`XMX=4g`); arm A needed 7 GB, so set `XMX=7g` in the script for it. `ors/run.sh ARM PORT` starts an arm in the foreground with `REBUILD_GRAPHS=False`, for restarts on a built graph. `ors/probe.sh` starts the tag probe on port 8083. Then `compare/run_ors.py` and `compare/ors_tag_probe.py ask` send the requests.

The runs used the wheelchair profile with the recommended weighting, an incline limit of 10 and a kerb limit of 0.06 m, with elevation off and `kerbs_on_crossings: true` (the ORS default). In v10.0.1 the wheelchair incline limit reads only the OSM `incline` tag, so elevation does not change which routes it allows. Observed in these runs, ORS v10.0.1 does not read `kerb=raised`, and it reads a bare `kerb:height` of 0.15 or more as centimetres ([ORS issue #2293](https://github.com/GIScience/openrouteservice/issues/2293)).

## Valhalla 3.9.0

Valhalla runs in process from the `pyvalhalla` wheel, so there is no Dockerfile. `valhalla/valhalla.json` is the configuration `compare/run_valhalla.py build` writes: `pyvalhalla` defaults, with `service_limits.pedestrian.max_distance` raised to 250000 and `tile_dir` set to `engines/valhalla/tiles`. The elevation tiles it points at were never built, so every edge has zero grade and `use_hills` acts on nothing. The runs used pedestrian costing with `type: wheelchair` and `sidewalk_factor` at its default of 1.0. Valhalla 3.9.0 has no kerb option, does not enforce `max_grade`, and penalises steps (600 s) without forbidding them, so it is a stair-avoiding foot baseline, not a wheelchair router. The `ipc:///tmp/...` entries are Valhalla's own default socket addresses, not paths on the machine that ran the comparison.

```sh
uv run --no-project --isolated --python 3.12 --with pyvalhalla==3.9.0 \
  python compare/run_valhalla.py build engines/osm/nyc-261001.osm.pbf engines/valhalla
```

## Unweaver

`unweaver/Dockerfile` builds Unweaver at commit `66352c1` (2022-11-02) on Debian, which supplies `mod_spatialite` inside the container. A project folder is the output of `scripts/osw_to_unweaver.py` (the routing layer and `regions.geojson`) plus the four cost and profile files in `unweaver-project/`, unchanged. `cost-wheelchair.py` is adapted from Unweaver's example wheelchair profile (Nick Bolten, Apache-2.0); this project added refusals for steps and street centrelines. Build it with `unweaver build PROJECT --changes-sign incline`. Without `--changes-sign incline` Unweaver copies each edge reversed with its incline unchanged, so a climb passes as a descent (`evaluation/compare/results/unweaver_bk16_without_changes_sign.json`). Then `compare/run_unweaver.py` sends the requests.

## Not included

The converted extracts, the ORS graphs, the Valhalla tiles and the Unweaver graphs (about 5 GB in all). The upstream ORS example configuration files that ship in the image are also left out.
