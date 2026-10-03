#!/bin/sh
# usage: run_detached.sh ARM PORT   (background)
cd "$(dirname "$0")/$1" && docker run -d --rm --name "ors-$1" -p "$2:8082" \
  -v "$PWD/config:/home/ors/config" -v "$PWD/graphs:/home/ors/graphs" \
  -v "$PWD/files:/home/ors/files" -v "$PWD/logs:/home/ors/logs" \
  -e ORS_CONFIG_LOCATION=/home/ors/config/ors-config.yml -e XMS=1g -e XMX=4g \
  openrouteservice/openrouteservice@sha256:b2f66b5e33949e86a279f3de654ef99e2964eb9112d16fb52788c6bb63ff941c
