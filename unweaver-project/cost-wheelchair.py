# Modified from the example wheelchair cost function of Unweaver
# (https://github.com/nbolten/unweaver, Copyright Nick Bolten, Apache License
# 2.0). Changes by OpenSidewalks NYC (Adam Munawar Rahman, 2026): the function
# refuses steps and street centrelines (the subclass test below); the uphill
# and downhill limits are the example's. This notice is the one Apache-2.0
# section 4(b) asks for on a modified file.
def cost_fun_generator(G, avoidCurbs=True, uphill=0.083, downhill=-0.1):
    def cost_fun(u, v, d):
        # A wheelchair cannot take steps, and a street centreline is the
        # middle of the roadway, not a place to route one.
        if d["subclass"] in ("steps", "street"):
            return None
        # No curb ramps? No route
        if d["footway"] == "crossing" and not d["curbramps"]:
            return None
        # Too steep?
        if d["incline"] is not None:
            if d["incline"] > uphill or d["incline"] < downhill:
                return None

        if d["length"] is None:
            return 0
        return d["length"]

    return cost_fun
