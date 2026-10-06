# Modified from the example wheelchair cost function of Unweaver
# (https://github.com/nbolten/unweaver, Copyright Nick Bolten, Apache License
# 2.0). Changes by OpenSidewalks NYC (Adam Munawar Rahman, 2026): the function
# refuses steps and street centrelines (the subclass test below) unless OSM
# says the street has a sidewalk along it, and reads an edge marked
# incline_unknown as steeper than any limit; the uphill and downhill limits
# are the example's. This notice is the one Apache-2.0 section 4(b) asks for
# on a modified file.
def cost_fun_generator(G, avoidCurbs=True, uphill=0.083, downhill=-0.1):
    def cost_fun(u, v, d):
        # A wheelchair cannot take steps.
        if d["subclass"] == "steps":
            return None
        # A street centreline is the middle of the roadway, not a place to
        # route one, unless OpenStreetMap tags the street as having a
        # sidewalk along it (sidewalk=both, left, right or yes) and no
        # separate sidewalk way. Then the street edge stands in for the
        # sidewalk beside it: its length and incline are the street's, and
        # its intersections have no crossing, so no kerb is checked there.
        if d["subclass"] == "street" and not d.get("sidewalk"):
            return None
        # No curb ramps? No route
        if d["footway"] == "crossing" and not d["curbramps"]:
            return None
        # Too steep? Where the heights at the two ends are not a slope the
        # edge could have (incline_unknown), something changes level there
        # that the data cannot grade: unmapped steps, a deck joined to the
        # ground. That is read as steeper than any finite limit. An edge
        # nothing measured (a tunnel, an elevator) has no incline and passes.
        incline = d["incline"]
        if incline is None and d.get("incline_unknown"):
            incline = float("inf")
        if incline is not None and (incline > uphill or incline < downhill):
            return None

        if d["length"] is None:
            return 0
        return d["length"]

    return cost_fun
