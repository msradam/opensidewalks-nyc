"""Pedestrian Zones: the schema's Polygon entity for plazas and other areas.

An OSM way tagged `area=yes` is a surface, not a path along its edge. The
schema writes such a surface as a Pedestrian Zone: a Polygon whose `_w_id`
lists the Node ids of its ring, in ring order, and says that every pair of
those Nodes is joined. This module holds the two things several stages and
scripts need: building a zone from the OSM edges of one way, and expanding a
zone back into the edges a graph consumer can walk.
"""

from __future__ import annotations

import math

from shapely.geometry import LineString, Polygon
from shapely.ops import linemerge, unary_union
from shapely.prepared import prep

from pipeline.utils.ids import feature_id, node_id

# Chords shorter than this get no incline: Stage 4 smooths heights over
# edges this short, and a raw difference of two 0.1 m heights over 3 m reads
# as a 3% grade.
_MIN_CHORD_INCLINE_M = 5.0


def ring_from_edges(geoms: list[LineString]) -> list[tuple[float, float]] | None:
    """The closed ring the edges of one area way form, as coordinates, or None.

    Returns the ring with its first point repeated at the end. None when the
    pieces do not join into one closed ring, or the ring is not a valid
    polygon (it crosses itself, or has no area).
    """
    if not geoms:
        return None
    merged = linemerge(unary_union(geoms))
    if merged.geom_type != "LineString" or not merged.is_ring:
        return None
    coords = [(float(x), float(y)) for x, y, *_ in merged.coords]
    poly = Polygon(coords)
    if not poly.is_valid or poly.area <= 0:
        return None
    return coords


def zone_feature(ring: list[tuple[float, float]], props: dict) -> dict:
    """A Pedestrian Zone's properties and geometry for a ring and its way tags.

    `_w_id` is the Node id of each ring vertex, in ring order, without the
    closing repeat, so that `_w_id[i]` is the vertex `coordinates[0][i]`.
    """
    poly = Polygon(ring)
    w_ids = [node_id(x, y) for x, y in ring[:-1]]
    return {
        "_id": feature_id(poly.wkt, "pedestrian_zone", props.get("ext:source", "osm_walk")),
        "_w_id": w_ids,
        "highway": "pedestrian",
        **{k: v for k, v in props.items() if k not in ("_id", "_w_id", "highway", "_u_id", "_v_id")},
        "geometry": poly,
    }


def _haversine_m(a, b):
    r = 6371000.0
    lat1, lat2 = math.radians(a[1]), math.radians(b[1])
    d = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(math.radians(b[0] - a[0]) / 2) ** 2
    return 2 * r * math.asin(math.sqrt(d))


def zone_edges(zone: dict, node_xy: dict, referenced: set, node_z: dict | None = None) -> list[dict]:
    """The directed edges a consumer walks for one zone feature (a GeoJSON dict).

    Two kinds, both inside the zone: the ring itself, between consecutive
    `_w_id` Nodes, and a straight chord between every two ring Nodes that
    some Edge also touches (the zone's entrances), when the chord lies
    within the polygon. A route can then cross a plaza between any two of
    its entrances, or follow its edge, and never cut across a courtyard the
    ring bends around. Each edge is one direction; the reverse is emitted
    too. Incline is rise over run from the Nodes' `ext:elevation_m`, left
    off for chords under 5 m.
    """
    p = zone.get("properties") or {}
    w = p.get("_w_id") or []
    if len(w) < 3 or any(n not in node_xy for n in w):
        return []
    poly = prep(Polygon([node_xy[n] for n in w]))
    pairs = {(w[i], w[(i + 1) % len(w)]) for i in range(len(w))}
    doors = [n for n in dict.fromkeys(w) if n in referenced]
    for i, a in enumerate(doors):
        for b in doors[i + 1:]:
            if (a, b) in pairs or (b, a) in pairs or a == b:
                continue
            if poly.covers(LineString([node_xy[a], node_xy[b]])):
                pairs.add((a, b))
    out = []
    for a, b in sorted(pairs):
        if a == b:
            continue
        length = _haversine_m(node_xy[a], node_xy[b])
        for u, v in ((a, b), (b, a)):
            incline = None
            if node_z and u in node_z and v in node_z and length >= _MIN_CHORD_INCLINE_M:
                incline = round((node_z[v] - node_z[u]) / length, 4)
                if abs(incline) > 1.0:
                    incline = None
            out.append({
                "_id": f"{p.get('_id')}:{u}:{v}",
                "_u_id": u, "_v_id": v,
                "highway": "pedestrian",
                "ext:zone": p.get("_id"),
                "length_m": round(length, 3),
                "incline": incline,
                **{k: p[k] for k in ("name", "surface", "foot", "ext:borough", "ext:osm_id", "ext:structure") if p.get(k) is not None},
                "coordinates": [list(node_xy[u]), list(node_xy[v])],
            })
    return out
