# Router comparison: protocol

This is the protocol for comparing this graph's wheelchair profile with OpenRouteService, Valhalla and Unweaver. It has three parts: the plan as written before the run, the protocol as run, and where the two differ and why. The plan in full, with the reasons for choosing ORS as the reference, is section 3 of `../background/landscape_2026-10-03.md`. The results are in `results/`, and `results/tables.md` has every table by area.

## 1. The plan

1. Same inputs. Build every engine from `new-york-261001.osm.pbf` and record engine version, configuration and build time.
2. Two data arms. Arm A is each engine on raw OSM. Arm B is ORS on this graph converted back to OSM with TDEI's `osm-osw-reformatter`, with this graph's ramp state written as kerb tags on crossing nodes and its LiDAR incline as `incline` on edges. A difference between ORS on arm A and on arm B is a data difference. A difference between ORS on arm B and this graph's own search is a rule or algorithm difference.
3. Same pairs. Reuse the seeded 2,000 random pairs per borough behind `../reachability/reach.json` and its landmark pairs. Record each engine's snap distance and report apart any pair whose snapped ends differ by more than 25 m.
4. Matched settings. This profile allows 8.3% up and 10% down and blocks a crossing end with no surveyed ramp. ORS takes incline limits of 3, 6, 10 or 15, so run it at 6 and at 10 to bracket the profile. Run ORS kerb limits at 0.03 m and 0.06 m. Run width unset and at 0.9 m.
5. Measures. Route found or not, as a 2 by 2 table per area. Detour against the same engine's foot route. Overlap, the share of each route within 10 m of the other. An audit of each engine's route with this graph's data (steps, crossings with no surveyed ramp within 5 m, edges over the incline limits, metres in the roadway) and of this graph's routes with raw OSM tags.
6. One cause per disagreement. Find the first thing on the reference route that this profile refuses and name it: kerb data, incline data, connectivity, structure or rule. Hand-check a sample per cause over imagery with two raters.
7. Expectation, written down in advance. ORS on raw OSM will route through crossings this profile blocks, because an untagged kerb passes in ORS, and will rarely block on incline, because few NYC ways carry an `incline` tag. That is a finding about the data, not a fault in ORS.

## 2. As run

Engines. OpenRouteService 10.0.1 (Docker image pinned by digest, profiles `wheelchair` and `foot-walking`, `kerbs_on_crossings: true`, elevation off), Valhalla 3.9.0 (the `pyvalhalla` wheel, pedestrian costing, type `wheelchair`, `use_hills` 0, distance cap raised) and Unweaver at commit `66352c1` (Docker, this repository's cost function unchanged). All were built from the pinned extract clipped with osmium to the box -74.28, 40.48, -73.68, 40.93. The Dockerfiles and configurations are in `../../engines/`, and versions, digests and build times are in `results/engines.json`.

Pairs. 12,437 pairs from `compare/pairs.py`: the same 2,000 seeded random pairs per borough and city-wide as `reach.json`, its 17 landmark pairs, and 420 trips in Brownsville (Brooklyn Community District 16). The Brownsville origins are 11 NYCHA developments, 10 senior centres and 21 subway entrances, and the destinations are 21 public schools, 11 clinics, 11 cooling sites, 3 libraries and the 2 subway elevators within 1 km. Each origin goes to the nearest destination of each kind and to one other drawn at random. Pairs whose snapped ends lie more than 25 m apart between routers are counted apart; shares of routes found are over all pairs and every other measure is over the rest.

Arms. Arm A is each engine on plain OSM. Arm B is ORS on this graph converted to OSM by `scripts/osw_to_osm.py`, run twice: strict, with a crossing end that has no surveyed ramp tagged `kerb=raised` with `kerb:height=0.14`, and known, with such ends left untagged. Arm C is the strict conversion with the street centrelines left out, so ORS has exactly the edges this profile uses.

Settings. The reference is ORS's wheelchair profile with its own recommended weighting, incline limit 10 and kerb limit 0.06 m. Incline 6, kerb 0.03 m, width 0.9 m and the `shortest` preference were also run and are in the tables.

Measures. As planned in step 5, with routes matched to edges by the engine's own OSM way ids (`results/comparison.json`).

Causes. Each disagreement with the reference is given the first barrier on the ORS route, as planned in step 6. The hand check drew 48 places, 12 per cause, over the city's March 2024 orthoimagery with neutral overlays (`disagreements/PROTOCOL.md`). Two raters took 24 sheets each (`ratings_A_001_024.json` and `ratings_A_025_048.json`) without being told the causes, and a third rated the 24 odd-numbered sheets again without seeing their answers (`ratings_B_odd.json`). The raters were language-model instances given only the written protocol and the sheets; no person rated.

Unweaver check. The search in `compare/graph.py` is a re-implementation, so it was run against Unweaver itself on the same layer and cost function: 840 requests in Brownsville, 34 between landmarks and 480 random across the city (`results/unweaver_*.json`).

## 3. Where the protocol changed, and why

- The converter. TDEI's `osm-osw-reformatter` 0.4.2 does not round-trip what ORS reads (`probes/roundtrip_reformatter.json`). It writes incline as a ratio with both directions joined, which ORS reads as 0, and it leaves kerb tags on the surveyed ramp nodes only, so the 5 m ramp rule is lost. Arm B therefore uses `scripts/osw_to_osm.py`, the smallest converter that carries incline as a signed percentage, width in metres and the ramp rule on the crossing ends. What ORS does with each tag form was tested first on a fixture (`probes/ors_tag_probe.json`).
- The reference uses ORS's own weighting. With `preference: shortest`, which matches this graph's search, ORS's wheelchair profile gives the foot route (71% of Brownsville routes over 10 m in the roadway, against 1.9% under its own weighting). The recommended weighting is what a user sees. Whether a route is found is the same under both.
- Trip ends snap to the nearest point on an edge, on a real network. Node snapping put this graph's start up to half a block from ORS's, and in the first run close to half of the Brownsville pairs had to be set aside for it. Snapping onto cut-off fragments then failed 10 Brownsville trips that start metres from the sidewalk. Both were unfair to this graph. The second fix raised its shares of routes found by 3 to 8 points. This is why the shares here differ from `../reachability/reach.json`, which snaps to the nearest node; see `../README.md`.
- Routes are matched by the engine's way ids. Matching by geometry alone showed ORS taking steps on about a fifth of Manhattan routes. By its own way ids it takes none (`results/osm_tag_summary.json`). The first check of that returned zero for the wrong reason, an empty id list (`../lessons/a-check-that-finds-nothing-must-be-able-to-find-something.md`).
- Arm C was added, so that the rule part of the gap could be split into roadway and incline.
- The 0.03 m kerb setting is reported and set aside. At that setting ORS refuses a `kerb=lowered` crossing (0.03 held as a 32-bit float, times 100, is just under 3), which on plain OSM cuts its routes found in Brooklyn from 98.8% to 42.1%. That is an artefact of the engine, so the 0.03 runs are not used as a bracket.

## 4. Known limits of the run

Arm B strict and known were converted before the converter kept coincident ways of different classes, and lack 286 of 1,437,901 ways; arm C was converted after the fix. The hand-check sample was drawn before the snapping fix; causes depend on ORS's route, so they did not move, but the sheets were not redrawn. Unweaver city-wide was sampled at 40 pairs per area, since a query takes seconds to minutes.
