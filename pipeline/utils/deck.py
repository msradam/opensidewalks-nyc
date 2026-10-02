"""Elevations for nodes on bridges and other structures.

The terrain model is bare earth: under a deck it gives the ground or the water.
LiDAR returns near a node show every surface there (ground, deck, an upper
deck, a car roof), so the walking surface is picked by continuity: start from
nodes that are plainly on the ground and follow the path, taking at each node
the surface closest in height to the node before it.
"""

import numpy as np

# Returns further apart in height than this are different surfaces.
LEVEL_GAP_M = 0.5
# A surface this close to the terrain model is the ground.
GROUND_TOL_M = 1.0
# How far a surface may be from the previous node's height: a fixed part for
# survey noise plus a share of the edge length (a 12% grade, steeper than any
# ramp a path is built with).
STEP_TOL_M, STEP_TOL_GRADE = 0.6, 0.12
# Steps climb at about 1:2.
STEPS_TOL_GRADE = 1.0
# An unclassified surface counts as solid with this many returns within this
# interquartile range of height.
FLAT_MIN_RETURNS, FLAT_IQR_M = 8, 0.15
# A solid surface this little above the terrain, at a node OSM does not put on
# a structure, may be the end of a ramp or an embankment the deck runs down,
# so the node is left for the deck to claim. Higher up it is something else
# (an elevated railway over a sidewalk) and the node stays on the ground.
LOW_DECK_M = 3.0
# A deck that has come down to the ground hands over to the terrain model
# where the two agree this closely. Until then the LiDAR ground height is
# kept, so the last edge of a ramp does not read the terrain model's error.
HANDOVER_M = 0.3


def surface_levels(returns: np.ndarray, surface_classes=()) -> list[tuple[float, int, bool, bool]]:
    """(height, number of returns, classified, solid) for each surface in a
    column of returns.

    returns: (n, 2) rows of height and LAS classification. A surface is
    classified if the survey marks it as something to walk on (ground, bridge
    deck). It is solid if it is classified, or if it is flat and well
    populated: a plaza over a road or a deck on a building is not classified
    as a bridge, but it is as flat as one. Tree crowns, fences and cables are
    neither, and are kept as unsure surfaces.
    """
    def clusters(z, classified):
        if len(z) < 3:
            return []
        z = np.sort(z)
        groups = np.split(z, np.flatnonzero(np.diff(z) > LEVEL_GAP_M) + 1)
        floor = max(3, 0.1 * len(z))
        return [(float(np.median(g)), len(g), classified,
                 classified or (len(g) >= FLAT_MIN_RETURNS
                                and np.subtract(*np.percentile(g, [75, 25])) <= FLAT_IQR_M))
                for g in groups if len(g) >= floor]

    on_surface = np.isin(returns[:, 1], surface_classes)
    sure = clusters(returns[on_surface, 0], True)
    rest = [lv for lv in clusters(returns[~on_surface, 0], False)
            if all(abs(lv[0] - h) > LEVEL_GAP_M for h, _, _, _ in sure)]
    return sure + rest


def label_surfaces(n: int, edges: list[tuple[int, int, float, bool]],
                   seed: np.ndarray, dtm: np.ndarray,
                   levels: list[list[tuple[float, int, bool, bool]]]) -> tuple[np.ndarray, np.ndarray]:
    """Pick one height per node.

    edges: (a, b, length in metres, is steps), undirected.
    seed:  True where the node is on an edge OSM tags as a structure.
    dtm:   terrain model height per node (NaN if unknown).
    levels: surface_levels() per node.

    Returns (height, kind). kind is 0 where the terrain model stands, 1 where
    a LiDAR surface was taken, 2 where the height was interpolated between
    neighbours, and -1 for a seed node nothing could be said about.
    """
    adj: list[list[tuple[int, float, bool]]] = [[] for _ in range(n)]
    for a, b, length, steps in edges:
        adj[a].append((b, length, steps))
        adj[b].append((a, length, steps))

    z = np.full(n, np.nan)
    kind = np.full(n, -1)

    def on_ground(i: int, h: float) -> bool:
        return not np.isnan(dtm[i]) and abs(h - dtm[i]) <= GROUND_TOL_M

    # What a node may stand on. OSM puts a seed node on a structure, so where
    # a solid surface shows above the ground, the ground (seen past the edge
    # of the deck) is not a candidate. Elsewhere every surface is.
    options: list[list[tuple[float, bool]]] = []
    for i in range(n):
        lv = [(h, solid) for h, _, _, solid in levels[i]]
        if seed[i]:
            decks = [(h, solid) for h, solid in lv if solid and not on_ground(i, h)]
            lv = decks or lv
        options.append(lv)

    for i in range(n):
        # Off a structure, a node where the survey classified ground at
        # terrain height is on the ground, whatever hangs above it (a tree,
        # an elevated railway). So is a node with no returns: nothing says
        # otherwise. A node whose classified surfaces are all elsewhere, or
        # that has only unclassified ones, is left for a deck to claim or not.
        if seed[i] or np.isnan(dtm[i]):
            continue
        classified = [h for h, _, c, _ in levels[i] if c]
        low_deck = any(s and GROUND_TOL_M < h - dtm[i] <= LOW_DECK_M for h, s in options[i])
        if not low_deck and (any(abs(h - dtm[i]) <= HANDOVER_M for h in classified) or (
                not classified and all(on_ground(i, h) for h, _ in options[i]))):
            z[i], kind[i] = dtm[i], 0

    def claim(a: int, b: int, length: float, steps: bool) -> bool:
        """Label b from its labelled neighbour a, if a surface at b fits."""
        # A node OSM does not put on a structure leaves the ground only as the
        # continuation of a deck: never straight from the ground, and never
        # by steps (steps down from a deck end on the ground).
        if not seed[b] and (kind[a] == 0 or steps):
            return False
        tol = STEP_TOL_M + (STEPS_TOL_GRADE if steps else STEP_TOL_GRADE) * length
        near = [(h, s) for h, s in options[b] if abs(h - z[a]) <= tol]
        # A solid surface in reach beats an unsure one that is closer.
        near = [h for h, s in near if s] or [h for h, _ in near]
        if steps:
            # Steps onto a deck: where both the deck and the ground below
            # show, the top of the steps is the deck.
            near = [h for h in near if not on_ground(b, h)] or near
        if not near:
            return False
        h = min(near, key=lambda h: abs(h - z[a]))
        if not seed[b] and abs(h - dtm[b]) <= HANDOVER_M:
            z[b], kind[b] = dtm[b], 0   # the deck has met the ground
        else:
            z[b], kind[b] = h, 1
        return True

    def spread(frontier: list[int]) -> None:
        """Follow plain edges as far as they go, then one flight of steps at
        a time, so a deck is reached by its ramps before its stairs."""
        stack, flights = list(frontier), []
        while stack or flights:
            if not stack:
                a, b, length = flights.pop()
                if kind[b] < 0 and claim(a, b, length, True):
                    stack.append(b)
                continue
            a = stack.pop()
            for b, length, steps in adj[a]:
                if kind[b] >= 0 or not options[b]:
                    continue
                if steps:
                    flights.append((a, b, length))
                elif claim(a, b, length, False):
                    stack.append(b)

    spread([i for i in range(n) if kind[i] >= 0])
    # A structure no labelled node leads onto (its approaches are on structure
    # too, it is reached by lift, or OSM joins it to the ground a storey
    # below): start it from the node with the clearest solid surface. An
    # unsure one may be a tree. A node beside a labelled one whose surfaces
    # were all out of reach is not started: what it saw is not the deck (a
    # bridge tower, say), and interpolation between its neighbours is right.
    counts = [{round(h, 3): c for h, c, _, _ in levels[i]} for i in range(n)]
    share = {}
    for i in range(n):
        solid = [counts[i][round(h, 3)] for h, s in options[i] if s]
        if kind[i] < 0 and seed[i] and solid and not any(kind[b] >= 0 for b, _, _ in adj[i]):
            share[i] = max(solid) / sum(counts[i].values())
    for start in sorted(share, key=share.get, reverse=True):
        if kind[start] < 0:
            z[start] = max((h for h, s in options[start] if s),
                           key=lambda h: counts[start][round(h, 3)])
            kind[start] = 1
            spread([start])

    # Seed nodes with no usable surface (under a roof or an upper deck): each
    # takes the length-weighted mean of its neighbours, which on a chain is
    # straight-line interpolation between its two labelled ends. Only nodes
    # that some labelled node leads to can be solved for.
    stack = [i for i in range(n) if kind[i] >= 0]
    reached = set()
    while stack:
        for b, _, _ in adj[stack.pop()]:
            if kind[b] < 0 and seed[b] and b not in reached:
                reached.add(b)
                stack.append(b)
    todo = sorted(reached)
    if todo:
        from scipy.sparse import lil_matrix
        from scipy.sparse.linalg import spsolve
        col = {i: k for k, i in enumerate(todo)}
        A, rhs = lil_matrix((len(todo), len(todo))), np.zeros(len(todo))
        for i, k in col.items():
            for b, length, _ in adj[i]:
                w = 1.0 / max(length, 0.1)
                if b in col:
                    A[k, k] += w
                    A[k, col[b]] -= w
                elif kind[b] >= 0:
                    A[k, k] += w
                    rhs[k] += w * z[b]
        z[todo], kind[todo] = spsolve(A.tocsr(), rhs), 2

    # What is left off-structure stays on the terrain model.
    rest = (kind < 0) & ~seed & ~np.isnan(dtm)
    z[rest], kind[rest] = dtm[rest], 0
    return z, kind
