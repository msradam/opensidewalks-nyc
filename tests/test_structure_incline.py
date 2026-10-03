"""Checks for incline on bridges and other structures, and for incline on
short edges. The official validator looks at none of this.

Run: python tests/test_structure_incline.py
"""

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString

from pipeline.stages.assemble import _smoothed_for_incline
from pipeline.utils.deck import label_surfaces, surface_levels

NAN = float("nan")


def test_returns_group_into_surfaces():
    # Ground returns at 10 m (class 2), a deck at 16 m (class 17), a car roof
    # on the deck and a few tree returns at 20 m (class 1).
    z = [10.0, 10.1, 9.9, 10.05, 16.0, 16.1, 15.9, 16.05, 16.0, 17.5, 20.0, 20.2, 20.1]
    cls = [2, 2, 2, 2, 17, 17, 17, 17, 17, 1, 1, 1, 1]
    levels = surface_levels(np.c_[z, cls], [2, 17])
    assert [(round(h), c, solid) for h, _, c, solid in levels] == [(10, True, True), (16, True, True), (20, False, False)]
    # Unclassified returns at a classified surface's height are that surface.
    levels = surface_levels(np.c_[[16.0, 16.1, 15.9, 16.2, 16.1, 16.0], [17, 17, 17, 1, 1, 1]], [2, 17])
    assert len(levels) == 1 and levels[0][2]
    # A flat, well populated unclassified surface is solid: a plaza deck.
    deck = np.c_[np.full(20, 48.6) + np.linspace(-0.05, 0.05, 20), np.ones(20)]
    assert [(c, solid) for _, _, c, solid in surface_levels(deck, [2, 17])] == [(False, True)]
    assert surface_levels(np.empty((0, 2)), [2, 17]) == []


def test_deck_height_follows_the_path_not_the_ground_below():
    # 0 - 1 - 2 - 3 - 4 - 5, 20 m apart. 0 and 5 stand on the ground at 10 m.
    # 1 to 4 are on a bridge over water: the terrain model says 2 m, the
    # returns show the deck, and at node 2 also an upper deck 7 m above it.
    edges = [(i, i + 1, 20.0, False) for i in range(5)]
    seed = np.array([False, True, True, True, True, False])
    dtm = np.array([10.0, 2.0, 2.0, 2.0, 2.0, 10.0])
    levels = [[(10.0, 30, True, True)], [(11.0, 30, True, True)], [(12.0, 30, True, True), (19.0, 60, True, True)],
              [(12.0, 30, True, True)], [(11.0, 30, True, True)], [(10.0, 30, True, True)]]
    z, kind = label_surfaces(6, edges, seed, dtm, levels)
    assert z.tolist() == [10.0, 11.0, 12.0, 12.0, 11.0, 10.0]
    assert kind.tolist() == [0, 1, 1, 1, 1, 0]


def test_untagged_approach_is_on_the_deck_and_a_path_below_is_not():
    # 0 ground, 1 an approach viaduct OSM does not tag (the deck is 5 m up
    # and hides the ground), 2 and 3 the tagged bridge, 4 ground. 5 is a
    # path passing under the bridge, joined to the ground at 0 only: the
    # only returns there are the deck above. 6 is a sidewalk under an
    # elevated railway, joined to the bridge at 2 by a plain edge (a station
    # entrance mapped without steps): the ground shows there, so it stays.
    edges = [(0, 1, 60.0, False), (1, 2, 20.0, False), (2, 3, 20.0, False),
             (3, 4, 60.0, False), (0, 5, 20.0, False), (2, 6, 8.0, False)]
    seed = np.array([False, False, True, True, False, False, False])
    dtm = np.array([10.0, 10.0, 3.0, 3.0, 10.0, 3.0, 10.0])
    levels = [[(10.0, 30, True, True)], [(15.0, 30, True, True)], [(16.0, 30, True, True)],
              [(16.0, 30, True, True)], [(10.0, 30, True, True)], [(16.0, 30, True, True)],
              [(10.0, 20, True, True), (16.5, 40, False, True)]]
    z, kind = label_surfaces(7, edges, seed, dtm, levels)
    assert (z[1], kind[1]) == (15.0, 1), "the approach takes the deck's height"
    assert (z[5], kind[5]) == (3.0, 0), "the path below stays on the terrain model"
    assert (z[6], kind[6]) == (10.0, 0), "the sidewalk under the railway stays on the ground"
    assert z[2] == z[3] == 16.0


def test_a_ramp_running_down_an_embankment_keeps_the_deck_height():
    # The tagged bridge ends at 1, still 1.7 m above the terrain model. The
    # untagged nodes 2 and 3 are on the ramp down: the ground beside it shows
    # (classified, at terrain height) and so does the ramp surface, a little
    # above. 4 is where the ramp meets the ground.
    edges = [(0, 1, 20.0, False), (1, 2, 12.0, False), (2, 3, 12.0, False), (3, 4, 12.0, False)]
    seed = np.array([True, True, False, False, False])
    dtm = np.array([20.0, 22.1, 22.1, 21.5, 21.0])
    levels = [[(24.0, 30, True, True)], [(23.8, 30, True, True)],
              [(22.1, 10, True, True), (23.3, 20, True, True)], [(21.5, 10, True, True), (22.6, 20, True, True)],
              [(21.0, 30, True, True)]]
    z, kind = label_surfaces(5, edges, seed, dtm, levels)
    assert z.tolist() == [24.0, 23.8, 23.3, 22.6, 21.0]
    assert kind.tolist() == [1, 1, 1, 1, 0]
    # Where the ramp has all but met the ground, the LiDAR ground height is
    # kept until it agrees with the terrain model, so the terrain model's
    # error on the embankment does not land on the ramp's last edge.
    dtm = np.array([20.0, 22.1, 21.7, 21.6, 21.0])
    levels = [[(24.0, 30, True, True)], [(23.8, 30, True, True)], [(22.4, 30, True, True)],
              [(21.8, 30, True, True)], [(21.0, 30, True, True)]]
    z, kind = label_surfaces(5, edges, seed, dtm, levels)
    assert z.tolist() == [24.0, 23.8, 22.4, 21.6, 21.0]
    assert kind.tolist() == [1, 1, 1, 0, 0]


def test_plaza_deck_is_taken_over_the_ground_seen_past_its_edge():
    # Revson Plaza: a deck over an avenue, unclassified in the survey but flat
    # and dense, with a few classified ground returns from the avenue below.
    # OSM joins it to College Walk at ground level by a plain edge.
    edges = [(0, 1, 10.0, False), (1, 2, 10.0, False)]
    seed = np.array([False, True, True])
    dtm = np.array([42.8, 42.9, 42.9])
    levels = [[(42.8, 20, True, True)], [(42.8, 5, True, True), (48.7, 51, False, True)],
              [(48.6, 50, False, True)]]
    z, kind = label_surfaces(3, edges, seed, dtm, levels)
    assert z.tolist() == [42.8, 48.7, 48.6]


def test_covered_span_is_interpolated_between_its_ends():
    # 1 and 3 are bridge nodes with a clear deck; 2, between them, is under a
    # roof whose top is the only surface the survey saw.
    edges = [(0, 1, 30.0, False), (1, 2, 10.0, False), (2, 3, 30.0, False), (3, 4, 30.0, False)]
    seed = np.array([False, True, True, True, False])
    dtm = np.array([10.0, 4.0, 4.0, 4.0, 14.0])
    levels = [[(10.0, 30, True, True)], [(12.0, 30, True, True)], [(25.0, 30, False, False)], [(16.0, 30, True, True)],
              [(14.0, 30, True, True)]]
    z, kind = label_surfaces(5, edges, seed, dtm, levels)
    assert kind.tolist() == [0, 1, 2, 1, 0]
    assert abs(z[2] - 13.0) < 1e-6, "a quarter of the way from 12 m to 16 m"


def test_steps_reach_the_deck_not_the_ground_under_it():
    # A flight of steps from the ground up to a deck 6 m above. At the top
    # node both the deck and the ground beside it show.
    edges = [(0, 1, 8.0, True), (1, 2, 20.0, False)]
    seed = np.array([False, True, True])
    dtm = np.array([10.0, 10.0, 10.0])
    levels = [[(10.0, 30, True, True)], [(10.0, 20, True, True), (16.0, 20, True, True)], [(16.0, 30, True, True)]]
    z, kind = label_surfaces(3, edges, seed, dtm, levels)
    assert z.tolist() == [10.0, 16.0, 16.0]


def test_a_tower_top_beside_a_labelled_deck_is_not_taken():
    # Deck nodes 0, 1 and 3 at 71 m. Node 2 stands at a bridge tower: the only
    # surface the survey saw there is the tower top, 175 m. It is not started
    # on that surface; it is interpolated between its neighbours.
    edges = [(0, 1, 10.0, False), (1, 2, 10.0, False), (2, 3, 10.0, False), (3, 4, 10.0, False)]
    seed = np.array([False, True, True, True, False])
    dtm = np.array([71.0, 5.0, 5.0, 5.0, 70.0])
    levels = [[(71.0, 30, True, True)], [(71.5, 30, True, True)], [(175.0, 60, False, True)],
              [(70.9, 30, True, True)], [(70.0, 30, True, True)]]
    z, kind = label_surfaces(5, edges, seed, dtm, levels)
    assert kind[2] == 2 and abs(z[2] - 71.2) < 1e-6


def test_a_bridge_is_entered_from_its_landing_not_its_tower():
    # No node is on the ground: an all-tagged span with a tower node in the
    # middle, where the only surface is the tower top. The chain must start
    # at a landing, not at the tower, so the tower node is interpolated.
    edges = [(0, 1, 30.0, False), (1, 2, 30.0, False), (2, 3, 30.0, False), (3, 4, 30.0, False)]
    seed = np.array([True] * 5)
    dtm = np.array([20.0, 0.0, 0.0, 0.0, 20.0])
    levels = [[(24.0, 30, True, True)], [(27.0, 30, True, True)], [(175.0, 80, False, True)],
              [(27.0, 30, True, True)], [(24.0, 30, True, True)]]
    z, kind = label_surfaces(5, edges, seed, dtm, levels)
    assert kind.tolist() == [1, 1, 2, 1, 1] and abs(z[2] - 27.0) < 1e-6


def test_structure_with_no_survey_gets_no_height():
    edges = [(0, 1, 20.0, False), (1, 2, 20.0, False)]
    seed = np.array([True, True, True])
    z, kind = label_surfaces(3, edges, seed, np.array([1.0, 1.0, 1.0]), [[], [], []])
    assert kind.tolist() == [-1, -1, -1] and np.isnan(z).all()


def _path(heights, spacing_m):
    """A straight path of nodes spacing_m apart with the given heights."""
    step = spacing_m / 84400.0
    ids = [f"n{i}" for i in range(len(heights))]
    coords = {i: (-73.99 + k * step, 40.7) for k, i in enumerate(ids)}
    edges = gpd.GeoDataFrame(
        {"_u_id": ids[:-1], "_v_id": ids[1:], "highway": "footway"},
        geometry=[LineString([coords[a], coords[b]]) for a, b in zip(ids[:-1], ids[1:])],
        crs="EPSG:4326")
    return edges, coords, dict(zip(ids, heights))


def test_short_edge_noise_is_smoothed_and_a_steady_slope_is_not():
    # Flat ground with 0.3 m of survey noise on one node of a 1 m edge: read
    # raw, that edge is a 30% grade.
    flat = [10.0] * 6 + [10.3] + [10.0] * 6
    out = _smoothed_for_incline(*_path(flat, 1.0))
    grades = np.abs(np.diff([out[f"n{i}"] for i in range(13)]))
    assert grades.max() < 0.083, "no edge of a flat path may read as too steep"
    # A steady 10% slope in 1 m edges keeps its grade away from the ends.
    slope = [10.0 + 0.1 * i for i in range(13)]
    out = _smoothed_for_incline(*_path(slope, 1.0))
    grades = np.diff([out[f"n{i}"] for i in range(13)])
    assert np.allclose(grades[3:-3], 0.1, atol=1e-6)
    # Edges of 10 m are left as measured.
    out = _smoothed_for_incline(*_path([10.0, 10.9, 10.0], 10.0))
    assert out == {"n0": 10.0, "n1": 10.9, "n2": 10.0}


def test_a_real_change_of_level_is_not_smoothed_away():
    # A 0.8 m step between two nodes 1 m apart is a wall or untagged steps.
    out = _smoothed_for_incline(*_path([10.0, 10.0, 10.0, 10.8, 10.8, 10.8], 1.0))
    assert out["n3"] - out["n2"] > 0.79
    # Height unknown at one node: its neighbours are not pulled toward zero.
    out = _smoothed_for_incline(*_path([10.0, NAN, 10.0], 1.0))
    assert out == {"n0": 10.0, "n2": 10.0}


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
