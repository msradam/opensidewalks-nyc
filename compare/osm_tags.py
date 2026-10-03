"""What raw OSM says about the ways this graph is built from.

Reads the extract once and keeps, for every way this graph references, the
tags a wheelchair router looks at and the kerb values on the way's nodes
(ORS reads kerb height for a crossing from the nodes of the whole way).
`barriers` turns that into the reasons a raw-OSM wheelchair router could
refuse the way.

usage: python compare/osm_tags.py PBF GRAPH_NPZ OUT_JSON
"""
import json
import sys
from pathlib import Path

import numpy as np
import osmium

KEEP = ("highway", "footway", "surface", "smoothness", "tracktype", "incline", "width", "wheelchair",
        "bridge", "tunnel", "layer", "access", "foot", "crossing", "name")
KERB_KEYS = ("kerb", "curb", "sloped_curb", "sloped_kerb", "kerb:height")
# Surfaces ORS's wheelchair default (cobblestone:flattened or better) refuses.
ROUGH = {"cobblestone", "sett", "unhewn_cobblestone", "pebblestone", "gravel", "fine_gravel", "compacted", "unpaved",
         "ground", "dirt", "earth", "grass", "grass_paver", "sand", "mud", "woodchips", "rock", "wood"}
BAD_SMOOTHNESS = {"intermediate", "bad", "very_bad", "horrible", "very_horrible", "impassable"}


class _Read(osmium.SimpleHandler):
    def __init__(self, wanted):
        super().__init__()
        self.wanted, self.kerb, self.ways = wanted, {}, {}

    def node(self, n):
        k = {t.k: t.v for t in n.tags if t.k in KERB_KEYS}
        if k:
            self.kerb[n.id] = k

    def way(self, w):
        if w.id in self.wanted:
            tags = {t.k: t.v for t in w.tags if t.k in KEEP}
            kerbs = [self.kerb[n.ref] for n in w.nodes if n.ref in self.kerb]
            if kerbs:
                tags["_kerb_nodes"] = kerbs
            self.ways[w.id] = tags


def read(pbf, npz, out):
    wanted = set(np.load(npz)["osm_id"].tolist()) - {-1}
    h = _Read(wanted)
    h.apply_file(str(pbf))
    Path(out).write_text(json.dumps(h.ways))
    print(f"{len(h.ways):,} of {len(wanted):,} ways found; {sum('_kerb_nodes' in t for t in h.ways.values()):,} with a kerb-tagged node; "
          f"{len(h.kerb):,} kerb-tagged nodes in the extract")


def incline_percent(value):
    """An OSM incline value as an absolute percentage, or None. up, down and steep count as over any limit."""
    v = str(value).strip().lower()
    if v in ("up", "down", "steep", "yes"):
        return 100.0
    try:
        return abs(float(v.replace("%", "").replace(",", ".")))
    except ValueError:
        return None


def barriers(tags, limit_percent=8.3):
    """Reasons a raw-OSM wheelchair router could refuse this way."""
    out = []
    if tags.get("highway") == "steps":
        out.append("steps")
    if tags.get("wheelchair") == "no":
        out.append("wheelchair=no")
    if tags.get("surface") in ROUGH:
        out.append("surface")
    if tags.get("smoothness") in BAD_SMOOTHNESS:
        out.append("smoothness")
    inc = incline_percent(tags["incline"]) if "incline" in tags else None
    if inc is not None and inc > limit_percent:
        out.append("incline")
    if tags.get("footway") == "crossing" and any(k.get("kerb") == "raised" for k in tags.get("_kerb_nodes", [])):
        out.append("kerb=raised")
    return out


if __name__ == "__main__":
    read(*sys.argv[1:4])
