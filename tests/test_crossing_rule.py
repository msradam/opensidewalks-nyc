"""Checks for the rule that says a crossing has curb ramps, as the routing
layer applies it.

Run: python tests/test_crossing_rule.py
"""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from osw_to_unweaver import crossings_with_ramps

M = 1 / 111320                                  # one metre of latitude, in degrees
ME = 1 / (111320 * math.cos(math.radians(40.7)))  # one metre of longitude, at the city's latitude


def _edge(eid, u, v, highway="footway", footway=None):
    return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[0, 0], [0, 0]]},
            "properties": {"_id": eid, "_u_id": u, "_v_id": v, "highway": highway, "footway": footway}}


def test_a_crossing_is_judged_whole_with_ramps_within_reach_of_its_ends():
    # Sidewalk s1 - a ... crossing a-m-b (two edges, m a lane node no sidewalk
    # reaches) ... b - sidewalk s2. The surveyed ramps are not on a or b but
    # on sidewalk vertices r1 and r2 beside them, 3 m away.
    xy = {"s1": (0, 0), "a": (0, 10 * M), "m": (0, 16 * M), "b": (0, 22 * M), "s2": (0, 32 * M),
          "r1": (3 * ME, 10 * M), "r2": (3 * ME, 22 * M)}
    feats = [_edge("e1", "s1", "a"), _edge("c1", "a", "m", footway="crossing"), _edge("c2", "m", "b", footway="crossing"),
             _edge("e2", "b", "s2"), _edge("e3", "a", "r1"), _edge("e4", "b", "r2")]
    assert crossings_with_ramps(feats, {"r1", "r2"}, xy, 5.0) == {"c1", "c2"}
    # The strict reading (ramp on the crossing's own node) finds nothing here.
    assert crossings_with_ramps(feats, {"r1", "r2"}, xy, 0.0) == set()
    # One end without a ramp in reach fails the whole crossing.
    assert crossings_with_ramps(feats, {"r1"}, xy, 5.0) == set()
    xy["r2"] = (6 * ME, 22 * M)
    assert crossings_with_ramps(feats, {"r1", "r2"}, xy, 5.0) == set()


def test_a_crossing_with_no_end_on_the_network_is_not_ramped():
    xy = {"a": (0, 0), "b": (0, 10 * M), "r": (0, 0)}
    feats = [_edge("c", "a", "b", footway="crossing")]
    assert crossings_with_ramps(feats, {"r"}, xy, 5.0) == set()


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
