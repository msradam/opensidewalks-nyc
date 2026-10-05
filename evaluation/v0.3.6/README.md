# Result files for v0.3.6-nyc.1

Each claim the v0.3.6 release notes and the README make about v0.3.6 itself has a file here. The build ran once on 2026-10-05 from commit `3383c6a`, after `python -m pipeline clean`. v0.3.5 was an internal build that was not released; its files are in [`../v0.3.5/`](../v0.3.5/), and v0.3.6 is measured against it here.

| File | What it is | Made by |
|---|---|---|
| `build.json` | The commit, start and end times, wall time, peak memory, the SHA-256 of the pinned OpenStreetMap extract before and after the clean, and the two rehearsals that came before | `/usr/bin/time -l python -m pipeline build` |
| `snap_report.json` | Edge ends and zone vertices moved onto their Nodes, and the counts of Nodes, Edges and Zones | `scripts/snap_endpoints.py` |
| `validator.json` | The `python-osw-validation` 0.5.0 result, the run time, and the SHA-256 of the ZIP it read. That checksum is the `nyc-osw-osw-split.zip` line of `SHA256SUMS` | The validator in its own environment |
| `SHA256SUMS` | Checksums of the 13 release assets | `shasum -a 256` |
| `checks.json` | The checks the validator does not make. Its `heights` block is new: terrain "no data" taken as a height, nodes inside tunnels, edges whose two heights are not a slope | `validators/post_build_checks.py` |
| `checks_v0.3.5.json` | The same checks, with the same code, on the v0.3.5 file. It gives the "before" figures | `validators/post_build_checks.py` |
| `compare_v035.json` | Every feature of v0.3.5 against v0.3.6 by `_id`. Each Node whose height differs and each Edge whose incline differs is grouped by the fix that caused it, and anything no fix explains would be listed as unexplained | `code/compare_v035.py` |
| `graphml_lengths.json` | Edges with `length_m` in the four GraphML files of v0.3.5 and v0.3.6 | `code/graphml_lengths.py` |
| `reach_same_pairs_v0.3.6.json` | The node pairs of `../v0.3.5/pairs_v0.3.4.json` routed on v0.3.6. `routed` holds one character per pair, `1` for a route | `code/reach_same_pairs.py` |
| `reach.json` | Reachability with pairs drawn afresh, and the 17 landmark routes | `evaluation/reachability/code/reach.py` |
| `bridges.json` | The 23 bridges tested end to end on pedestrian edges, with zone outlines counted as pedestrian edges | `code/bridges_with_zones.py`, then the bridge test of `evaluation/bridges/` |

## What the files show

The validator returns `is_valid: true` with no errors on a ZIP of three files: nodes, edges and zones.

v0.3.6 has the same 3,986,649 features as v0.3.5, each with the same `_id`. No geometry differs, and no property differs other than `incline`, `ext:elevation_m`, `ext:elevation_source`, the new `ext:incline_unknown`, and the version and timestamp stamps. All 217,679 Curb Ramps are equal field for field. Of the 461 figures the post-build checks produce, the ones that differ between the two versions are all counts of heights or inclines.

Terrain "no data". In v0.3.5, 219 terrain Nodes had a "no data" pixel among the four they are interpolated from; 198 of them carried a height, and 196 of those heights equal the blend with the zeros. In v0.3.6, 220 Nodes have such a pixel; 203 have no height, 17 take it from the pixels that hold data, and none equals the blend. Nodes at exactly 0.0 m fell from 218 to 39. Against v0.3.5, 182 Nodes lost a height for this reason and 15 changed.

Tunnels. 1,308 Nodes touch only tunnel Edges. Eight of them are on the outline of a Pedestrian Zone, which is open ground, and keep their height. The other 1,300 carried the height of the ground above in v0.3.5 and have none in v0.3.6. Two of the 1,300 had a deck height, so 14,400 Nodes carry `ext:elevation_source`, against 14,402. No other deck height changed. Nodes without `ext:elevation_m` went from 50 to 1,532.

Jumps between surfaces. In v0.3.5, 1,076 Edges that are not Steps read 50% or steeper, 926 of them on no tagged structure. In v0.3.6 none does. 2,068 Edges carry `ext:incline_unknown`: 1,074 that had an incline of 50% or more, and 994 that had none because the grade computed over 100%. Incline differs on 1,422 Edges in all: those 1,074, and 348 at or next to the Nodes whose height changed (300 lost their incline and 48 changed). It is identical on the other 2,796,116. Edges with an incline went from 2,793,720 to 2,792,346.

Routing. The routing layer has 2,478 marked edges: the 2,068, and 410 of the 96,650 zone edges. The wheelchair profile passes 1,845,159 edges, against 1,845,874. On the same 10,000 pairs (2,000 per borough) it routes 1,694 in Brooklyn, 1,258 in Queens, 1,056 in Manhattan, 872 in the Bronx and 861 on Staten Island, against 1,696, 1,259, 1,068, 879 and 861: 23 pairs lost and 1 gained. The profile with no incline limit and the three profiles that do not read incline give the same result on every pair. Of the 17 landmark routes, one changed for the wheelchair profile: it finds no route between the two ends of the High Line, where in v0.3.5 it left the deck over an Edge with no incline. It still finds no route across the Manhattan Bridge and long detours across the Williamsburg, Queensboro and Pulaski bridges.

Bridges. The same 20 of 23 are joined on pedestrian edges, with the same walking distances, and the pedestrian graph has the same 4,975 components.

GraphML. In v0.3.5, 96,650 of the 2,894,188 edges of the directed file had `length_m` (3.3%), and 44,450 of the 1,443,112 of the undirected file. In v0.3.6 every edge of both files has it, and no value is shorter than the straight distance between the edge's two nodes.

## Licence

These files hold OpenStreetMap ids and are under ODbL-1.0, like the graph.
