A tiny test fixture for a GraphHopper-based engine must be one connected network, and query points must lie inside its bounding box.

The first ORS tag probe had 17 separate corridors: ORS kept one and reported "could not find routable point" for the other 16, because small networks that stand alone are dropped at import. A query point 2 mm beyond the last node failed the same way. Both failures look exactly like "the tag blocks the route", so a probe must tell error 2009 (no route) from 2010 (no point) and must include a control corridor with no tags. Evidence: compare/ors_tag_probe.py.
