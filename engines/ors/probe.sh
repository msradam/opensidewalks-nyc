#!/bin/sh
# Start ORS on the tag probe fixture (port 8083) and wait until it is ready.
cd "$(dirname "$0")/probe" && docker run -d --rm --name ors-probe -p 8083:8082 \
  -v "$PWD/config:/home/ors/config" -v "$PWD/graphs:/home/ors/graphs" -v "$PWD/files:/home/ors/files" -v "$PWD/logs:/home/ors/logs" \
  -e ORS_CONFIG_LOCATION=/home/ors/config/ors-config.yml -e XMS=512m -e XMX=1g \
  openrouteservice/openrouteservice@sha256:b2f66b5e33949e86a279f3de654ef99e2964eb9112d16fb52788c6bb63ff941c >/dev/null
for i in $(seq 1 60); do curl -s localhost:8083/ors/v2/health | grep -q ready && exit 0; /bin/sleep 2; done; exit 1
