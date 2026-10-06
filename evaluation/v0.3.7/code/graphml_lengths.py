"""Count the edges of GraphML files that carry `length_m`, and check the value.

usage: graphml_lengths.py OUT.json LABEL=FILE.graphml[.gz] ...
Streams each file. For every edge with `length_m` and a node position at both
ends, it also compares the value with the straight distance between the two
nodes: an edge's line is never shorter than that.
"""
import gzip
import json
import math
import sys
import xml.etree.ElementTree as ET

NS = "{http://graphml.graphdrawing.org/xmlns}"


def dist(a, b):
    lat1, lat2 = math.radians(a[1]), math.radians(b[1])
    d = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(math.radians(b[0] - a[0]) / 2) ** 2
    return 2 * 6371000.0 * math.asin(math.sqrt(d))


out = {}
for arg in sys.argv[2:]:
    label, path = arg.split("=", 1)
    keys, xy = {}, {}
    edges = with_len = zone = zone_with_len = shorter = total = 0
    with (gzip.open if path.endswith(".gz") else open)(path, "rb") as f:
        for _, el in ET.iterparse(f, events=("end",)):
            if el.tag == NS + "key":
                keys[el.get("id")] = (el.get("for"), el.get("attr.name"))
            elif el.tag == NS + "node":
                d = {keys[c.get("key")][1]: c.text for c in el if c.tag == NS + "data"}
                xy[el.get("id")] = (float(d["x"]), float(d["y"]))
                el.clear()
            elif el.tag == NS + "edge":
                d = {keys[c.get("key")][1]: c.text for c in el if c.tag == NS + "data"}
                edges += 1
                is_zone = "ext:zone" in d
                zone += is_zone
                if d.get("length_m") not in (None, ""):
                    with_len += 1
                    zone_with_len += is_zone
                    L = float(d["length_m"])
                    total += L
                    a, b = xy.get(el.get("source")), xy.get(el.get("target"))
                    shorter += a is not None and b is not None and L < dist(a, b) - 0.01
                el.clear()
    out[label] = {"file": path.rsplit("/", 1)[-1], "nodes": len(xy), "edges": edges, "edges_with_length_m": with_len,
                  "share": round(with_len / edges, 4), "zone_edges": zone, "zone_edges_with_length_m": zone_with_len,
                  "sum_of_length_m_km": round(total / 1000, 1),
                  "edges_whose_length_m_is_under_the_straight_distance_between_their_nodes": shorter}
    print(label, out[label], flush=True)
json.dump(out, open(sys.argv[1], "w"), indent=1)
