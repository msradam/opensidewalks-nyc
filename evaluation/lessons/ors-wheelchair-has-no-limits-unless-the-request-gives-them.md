A local OpenRouteService wheelchair request with no `restrictions` blocks nothing: a 15 cm kerb, a 10.6% incline and a 0.8 m width all pass.

The 6% incline and 0.06 m kerb in ORS's documentation are what a client is expected to send, not what the engine applies. On the tag probe fixture every corridor passes under "none given" (research_notes/compare/armB/ors_tag_probe.json). So "the ORS wheelchair profile" in a comparison has to name its restrictions, and a run with none is a foot route with a different weighting. The comparison uses incline 6 and 10 with kerb 0.06 as the bracket.
