"""Ask a local ORS what it does with each form of kerb, incline and width tag.

`write` makes a tiny OSM file: one corridor per tag form, each a sidewalk, a
crossing and a sidewalk in a line, joined only at their west ends, so a route
along a corridor has to use its crossing. `ask` routes end to end along each corridor under each
wheelchair limit and records whether ORS lets the route through.

usage: python compare/ors_tag_probe.py write OUT.osm.pbf
       python compare/ors_tag_probe.py ask BASE_URL OUT.json
"""
import json
import sys
from pathlib import Path

import osmium
import requests

M = 1 / 111320
# name: (tags on both crossing ends, tags on the crossing way)
FORMS = {
    "no tags": ({}, {}),
    "kerb=lowered": ({"kerb": "lowered"}, {}),
    "kerb=flush": ({"kerb": "flush"}, {}),
    "kerb=raised": ({"kerb": "raised"}, {}),
    "kerb=no": ({"kerb": "no"}, {}),
    "kerb=raised kerb:height=0.14": ({"kerb": "raised", "kerb:height": "0.14"}, {}),
    "kerb=raised kerb:height=0.15": ({"kerb": "raised", "kerb:height": "0.15"}, {}),
    "kerb=raised kerb:height=0.18": ({"kerb": "raised", "kerb:height": "0.18"}, {}),
    "incline=0.083 (a ratio)": ({}, {"incline": "0.083"}),
    "incline=-0.0039;0.0039": ({}, {"incline": "-0.0039;0.0039"}),
    "incline=6.4%": ({}, {"incline": "6.4%"}),
    "incline=6.6%": ({}, {"incline": "6.6%"}),
    "incline=-8.3%": ({}, {"incline": "-8.3%"}),
    "incline=10.4%": ({}, {"incline": "10.4%"}),
    "incline=10.6%": ({}, {"incline": "10.6%"}),
    "width=0.8": ({}, {"width": "0.8"}),
    "width=1.2": ({}, {"width": "1.2"}),
}
LIMITS = {
    "kerb 0.03": {"maximum_sloped_kerb": 0.03}, "kerb 0.06": {"maximum_sloped_kerb": 0.06},
    "kerb 0.1": {"maximum_sloped_kerb": 0.1},
    "incline 6": {"maximum_incline": 6}, "incline 10": {"maximum_incline": 10},
    "width 0.9": {"minimum_width": 0.9},
}


def ends(i):
    lat = 40.60 + i * 0.01      # corridors about 1.1 km apart
    return [-73.90, lat], [-73.90 + 110 * M / 0.758, lat]


def write(out):
    Path(out).unlink(missing_ok=True)
    w = osmium.SimpleWriter(out)
    ways, nid = [], 0
    for i, (node_tags, way_tags) in enumerate(FORMS.values()):
        (x0, y), (x1, _) = ends(i)
        xs = [x0, x0 + (x1 - x0) * 50 / 110, x0 + (x1 - x0) * 60 / 110, x1]
        for j, x in enumerate(xs):
            w.add_node(osmium.osm.mutable.Node(id=nid + j + 1, location=(x, y), tags=node_tags if j in (1, 2) else {}, version=1))
        side = {"highway": "footway", "footway": "sidewalk"}
        ways += [([nid + 1, nid + 2], side), ([nid + 2, nid + 3], {"highway": "footway", "footway": "crossing", **way_tags}),
                 ([nid + 3, nid + 4], side)]
        nid += 4
    # One spine joins the west ends: ORS drops small networks that stand alone.
    ways.append((list(range(1, nid, 4)), {"highway": "footway", "footway": "sidewalk"}))
    for i, (refs, tags) in enumerate(ways, start=1):
        w.add_way(osmium.osm.mutable.Way(id=i, nodes=refs, tags=tags, version=1))
    w.close()


def ask(base, out):
    assert base.startswith(("http://localhost", "http://127.0.0.1")), "local engines only"
    res = {}
    for i, form in enumerate(FORMS):
        (x0, y), (x1, _) = ends(i)
        o, d = [x0 + 5 * M, y], [x1 - 5 * M, y]      # inside the graph's bounding box, or ORS finds no point
        res[form] = {}
        for name, r in LIMITS.items():
            body = {"coordinates": [o, d], "instructions": False, "options": {"profile_params": {"restrictions": r}}}
            j = requests.post(f"{base}/ors/v2/directions/wheelchair/geojson", json=body, timeout=30).json()
            # 2009 is "no route": the corridor is blocked. Anything else is a failed probe.
            code = j.get("error", {}).get("code")
            res[form][name] = "passes" if "features" in j else "blocked" if code == 2009 else f"error {code}"
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print(f'{"":32}' + "  ".join(f"{k:>10}" for k in LIMITS))
    for form, row in res.items():
        print(f"{form:32}" + "  ".join(f"{v:>10}" for v in row.values()))


if __name__ == "__main__":
    {"write": write, "ask": ask}[sys.argv[1]](*sys.argv[2:])
