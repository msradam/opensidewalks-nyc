# Result files for v0.3.7-nyc.1

Each claim the v0.3.7 release notes and the README make about v0.3.7 itself has a file here. The build ran on 2026-10-06 after `python -m pipeline clean`: stages 1 to 3 at commit `070f35a`, then, after Stage 4's node merge failed and was corrected twice more, stages 4 to 6 at commit `c442658` on the same staged data (`build.json` records each run). v0.3.7 is measured against v0.3.6, whose files are in [`../v0.3.6/`](../v0.3.6/).

| File | What it is | Made by |
|---|---|---|
| `build.json` | The commit, start and end times, wall time, peak memory, the SHA-256 of the pinned OpenStreetMap extract before and after the clean, and the rehearsal that came before | `/usr/bin/time -l python -m pipeline build` |
| `snap_report.json` | Edge ends and zone vertices moved onto their Nodes, and the counts of Nodes, Edges and Zones | `scripts/snap_endpoints.py` |
| `validator.json` | The `python-osw-validation` 0.5.0 result and the SHA-256 of the ZIP it read. That checksum is the `nyc-osw-osw-split.zip` line of `SHA256SUMS` | The validator in its own environment |
| `SHA256SUMS` | Checksums of the 13 release assets | `shasum -a 256` |
| `checks.json` | The checks the validator does not make. New blocks: `ramp_position` (where each surveyed ramp sits), `osm_kerbs`, `elevators`, `street_sidewalk_tags` and `shared_paths` | `validators/post_build_checks.py` |
| `checks_v0.3.6.json` | The same checks, with the same code, on the v0.3.6 file. It gives the "before" figures | `validators/post_build_checks.py` |
| `compare_v036.json` | Every feature of v0.3.6 against v0.3.7. Edges, zones and bare nodes are matched by `_id`; a surveyed ramp is matched by `ext:ramp_id`, because its node id is the id of the vertex it snapped to, which this release changes on purpose. Each difference is grouped by the change that caused it, and anything no change explains is listed as unexplained | `code/compare_v036.py` |
| `tag_counts.json` | The sidewalk, kerb, elevator and cycleway tags counted in the pinned extract, which decided the representation | A pyosmium pass over the extract (`code/scan_tags.py`) |
| `reach_same_pairs_v0.3.7.json` | The node pairs of `../v0.3.5/pairs_v0.3.4.json` routed on v0.3.7. `routed` holds one character per pair, `1` for a route | `code/reach_same_pairs.py` |
| `reach.json` | Reachability with pairs drawn afresh, and the 17 landmark routes | `evaluation/reachability/code/reach.py` |
| `bridges.json` | The 23 bridges tested end to end on pedestrian edges, with zone outlines counted as pedestrian edges | `code/bridges_with_zones.py`, then the bridge test of `evaluation/bridges/` |
| `README.md` | What the files show | |

## What the files show

The validator returns `is_valid: true` with no errors on a ZIP of three files: nodes, edges and zones.

v0.3.7 has 3,995,589 features against 3,986,649: 8,792 cycleway and track edges and 4,809 nodes are new (3,594 on a new edge, 1,240 surveyed ramps at the node id of the crossing end they moved to, and one bare node at a merged endpoint), and 4,661 features of v0.3.6 are gone (4,654 surveyed ramps whose node id changed, four road edges and three bare nodes at that merged endpoint). Every one of the 217,679 ramps is matched by `ext:ramp_id` with every survey field equal; 48,630 changed node. Geometry is the same on 2,797,528 of the 2,797,534 shared edges. Incline is the same on 2,795,142 shared edges; it differs on 492 edges at elevators (no incline now), on 1,884 within three hops of a new edge or a moved endpoint, on the six at that endpoint, and on 10 by at most 0.0047 that no change explains. 344,718 street edges carry `ext:sidewalk`. 22,238 OSM kerb nodes and 137 elevator nodes are new in kind; 85,931 surveyed ramps sit on an OSM kerb node and carry its values in `ext:osm_kerb` and `ext:osm_tactile_paving`, 4,383 disagreeing about the kerb (OSM `flush` on 4,245) and 26,043 about the warning surface. All 2,201 zones, all widths, surfaces, names and crossing markings are unchanged; two nodes beside a new edge changed height.

Ramps. 184,939 surveyed ramps are attached (181,499 in v0.3.6); 166,310 sit on a crossing end (120,625), 16,159 on no crossing (55,347). By borough the share on a crossing end is 94.2% in Brooklyn, 93.5% in Manhattan, 91.3% in Queens, 84.0% in the Bronx and 76.0% on Staten Island.

Routing. The wheelchair profile routes 1,760 pairs in Brooklyn, 1,311 pairs in Queens, 1,097 pairs in Manhattan, 933 pairs in Bronx, 885 pairs in Staten Island (88.0%, 65.6%, 54.9%, 46.7%, 44.3%), against 1,694, 1,258, 1,056, 872, 861 in v0.3.6, and 848 of the 2,000 pairs with ends anywhere in the city against 803. It lost 2 pairs (both with ends anywhere in the city, each at a crossing whose surveyed ramp moved onto the end of the crossing it serves and out of the 5 m reach of this one) and gained 292: streets tagged as having a sidewalk, OSM's lowered and flush kerbs at crossing ends, elevators, and the cycleways and tracks kept under the United States default. No other profile lost a pair, and the walking profile gained 10 across the five boroughs on the new cycleway and track edges (`reach_same_pairs_v0.3.7.json`). Of the 17 landmark routes, the wheelchair profile now finds one it did not: between the two ends of the High Line it finds a 1,706 m route, 17 m longer than the walker's, on the Tenth Avenue sidewalks (68% sidewalk, 17% footway, 15% crossing), not on the High Line's deck; the elevator at 30th Street is carried, but the route the profile picks still stays at street level. Three bridge detours shortened: Pulaski from 14,119 m to 3,691 m, Macombs Dam from 8,691 m to 4,842 m (23% of it on streets tagged as having a sidewalk) and Queensboro from 24,131 m to 19,186 m, and nine street-level routes shortened by 38 m to 266 m. The Manhattan Bridge and the Wards Island footbridge still have no wheelchair route (`reach.json`).

Bridges. 21 of 23 are joined on pedestrian edges, against 20: the RFK Bridge's Bronx span joins through the island path, a cycleway with no `foot` tag, at 1,201 m for 597 m straight (2,484 m along the street in v0.3.6). The 145th Street Bridge and the Broadway Bridge are still joined only along the street centreline. The pedestrian graph has 4,945 components against 4,975.

## Licence

These files hold OpenStreetMap ids and are under ODbL-1.0, like the graph.
