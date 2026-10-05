# Result files for v0.3.5-nyc.1

v0.3.5-nyc.1 was an internal build. It was not released, and v0.3.6 is the release that carries its changes (see [`../v0.3.6/`](../v0.3.6/)). Each claim the documents made about v0.3.5 has a file here. The build ran once on 2026-10-04 from commit `5d04a19`, after `python -m pipeline clean`.

| File | What it is | Made by |
|---|---|---|
| `build.json` | The commit, start and end times, wall time, peak memory, and the SHA-256 of the pinned OpenStreetMap extract before and after the clean | `/usr/bin/time -l python -m pipeline build` |
| `snap_report.json` | Edge ends and zone vertices moved onto their Nodes, and the counts of Nodes, Edges and Zones | `scripts/snap_endpoints.py` |
| `validator.json` | The `python-osw-validation` 0.5.0 result, the run time, and the SHA-256 of the ZIP it read. That checksum is the `nyc-osw-osw-split.zip` line of `SHA256SUMS` | The validator in its own environment |
| `SHA256SUMS` | Checksums of the 13 release assets, `evaluation-sheets.zip` included | `shasum -a 256` |
| `checks.json` | The checks the validator does not make: counts, form, elevation, incline, deck heights, components, curb ramps, widths, zones | `validators/post_build_checks.py` |
| `compare_v034.json` | Every feature of v0.3.4 against v0.3.5 by `_id`: which exist on one side only, and whether geometry, incline, elevation or any other property differs | `code/compare_versions.py` |
| `bridges.json` | The 23 bridges tested end to end on pedestrian edges, with zone outlines counted as pedestrian edges | The bridge test of `evaluation/bridges/` |
| `reach.json` | Reachability with 2,000 pairs per borough drawn afresh on v0.3.5, and the 17 landmark routes | `evaluation/reachability/code/reach.py` |
| `pairs_v0.3.4.json` | The node pairs that the same seed draws on v0.3.4 | `code/reach_same_pairs.py --dump-pairs` |
| `reach_same_pairs_v0.3.4.json`, `reach_same_pairs_v0.3.5.json` | Those pairs routed on each version. `routed` holds one character per pair, `1` for a route | `code/reach_same_pairs.py` |

## What the files show

The validator returns `is_valid: true` with no errors on a ZIP of three files: nodes, edges and zones.

v0.3.5 has 3,986,649 features: 1,186,910 Nodes, 2,797,538 Edges and 2,201 Pedestrian Zones. Against v0.3.4, 83,644 Edges are gone (73,700 Pedestrian Road and 9,944 Footway), and every one of them lay on the outline of an area that is now a zone. 2,201 zones and 34 road Edges are new. The 34 are the reverse directions of one-way road segments: in v0.3.4 a plaza outline Edge between the same two Nodes stood in for the reverse, and with the outline gone the pipeline adds it. This was checked on the two validator ZIPs: each of the 34 new Edges runs opposite a removed outline Edge between the same two Nodes.

On the 3,984,414 features both versions share, no geometry differs. Elevation differs on 6 Nodes and incline on 16 Edges. All six Nodes carry a deck height from the LiDAR point clouds (`compare_v034.json` lists them under `examples`). Four are within a few meters of each other at the south end of the High Line (near -74.0079, 40.7419), one is at -73.9845, 40.7703 in Manhattan and one at -73.9875, 40.6605 in Brooklyn. One of the six, `086fe621094e97d8`, is also the one feature with another property changed. Why these six moved was not traced further; the deck height step follows structure edges, and plaza outlines on structures are no longer edges. `ext:source` changed from `osm_walk` to `nyc_dot_ramps` on the 181,499 Curb Ramps that sit on an OpenStreetMap vertex, and 45,616 Edges gained `ext:osm_highway=path`.

The pedestrian graph has the same 4,975 components and the same largest component of 611,968 Nodes as before, with zone outlines counted as connections. 20 of 23 bridges are joined on pedestrian edges, the same 20.

Reachability needs care. `reach.py` draws pairs from each borough's pool of Nodes, and the zones changed which Nodes are in the Brooklyn and Queens pools by a few Nodes, so its Brooklyn and Queens pairs in `reach.json` are different trips from the ones measured on v0.3.3 and v0.3.4. On the same pairs, no pair lost its route under any profile. The wheelchair profile gained 8 pairs in Manhattan (1,060 to 1,068 of 2,000), 5 in the Bronx (874 to 879) and none in Brooklyn (1,696), Queens (1,259) or Staten Island (861). The gains are routes that cross a plaza. A zone edge under 5 m carries no incline unless its ends differ by more than 0.5 m, so part of the gain may come from short plaza edges whose small height differences are no longer read as a grade; this was not separated.

The GraphML files, the routing JSON and these reachability results were made with the zone expansion as committed after the build, in commit `e997165` (`build.json` says what differs). With the rule as it stood at the build commit, which gave no incline to any zone edge under 5 m, the Manhattan Bridge landmark pair found a wheelchair route across a change of level. With the committed rule it finds none, as in v0.3.3.

## Licence

These files hold OpenStreetMap ids and are under ODbL-1.0, like the graph.
