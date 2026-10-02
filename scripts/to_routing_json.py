"""Write the routing-friendly JSON from the canonical OSW GeoJSON.

Stage 6 writes output/nyc-routing.json before scripts/snap_endpoints.py has
run, so that copy predates the endpoint snap and the 7-decimal rounding. For
a release, make it from the snapped file instead.

Usage:
    python scripts/to_routing_json.py INPUT.geojson OUTDIR/
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pipeline.stages.export import export_routing_json

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: to_routing_json.py INPUT.geojson OUTDIR/", file=sys.stderr)
        sys.exit(2)
    out_dir = Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)
    export_routing_json(json.loads(Path(sys.argv[1]).read_text()), out_dir)
