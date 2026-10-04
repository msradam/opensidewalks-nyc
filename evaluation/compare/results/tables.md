# Comparison tables

From `comparison.json`. Random areas have 2,000 pairs each. Measured pairs are those whose snapped ends lie within 25 m of each other across routers.

## Key

Added 2026-10-03 after external review. Everything below the key is as `code/tables.py` wrote it from `comparison.json`. The protocol is `../PROTOCOL.md`.

Columns. BK, QN, MN, BX, SI and citywide are 2,000 seeded random pairs each, drawn from graph nodes. Brownsville is 420 trips in Brooklyn Community District 16. "landmark hand-checked" is the 8 landmark pairs whose routes were checked over imagery in `../../reachability/hand_check.md`; the column name is the label the scripts use, and the check was done by a language-model rater, not by hand. "landmark structure" is the 9 landmark pairs meant to cross a bridge or other structure.

Rows, by router:

| Row label | What was asked | Data | Matched to edges by |
|---|---|---|---|
| this graph, wheelchair | This repository's search with the wheelchair profile: no steps, no street centreline, no crossing without a surveyed ramp within 5 m of each end, incline at most 8.3% up and 10% down | This graph: OSM ways, the DOT ramp survey, LiDAR incline | Its own edges |
| this graph, walk | This repository's search with the walking profile | This graph | Its own edges |
| ORS raw OSM, incline 10 kerb 0.06 | ORS 10.0.1 wheelchair, recommended weighting, `maximum_incline` 10, `maximum_sloped_kerb` 0.06. This is the reference | Plain OSM (arm A), elevation off | OSM way ids ORS returns |
| ORS raw OSM, incline 6 kerb 0.06 | The same with `maximum_incline` 6 | Arm A | OSM way ids |
| ORS raw OSM, no limits given | ORS wheelchair, recommended weighting, no restrictions in the request. ORS then applies no kerb, incline or width limit, but its encoder still excludes steps, unpaved surfaces, bad smoothness and major roads tagged `sidewalk=no` | Arm A | OSM way ids |
| ORS on this graph, strict kerbs, 10/0.06 and 6/0.06 | ORS wheelchair, recommended weighting, incline 10 or 6, kerb 0.06 | Arm B strict: this graph converted to OSM, with a crossing end that has no surveyed ramp tagged `kerb=raised`, `kerb:height=0.14`, and LiDAR incline as `incline` | This graph's edge ids |
| ORS on this graph, known kerbs only, 10/0.06 | The same at incline 10 | Arm B known: as strict, but ends without a surveyed ramp are left untagged | This graph's edge ids |
| ORS raw OSM, foot | ORS foot-walking, recommended weighting | Arm A | Geometry only |
| Valhalla wheelchair type | Valhalla 3.9.0 pedestrian costing, `type: wheelchair`, `use_hills` 0, built without elevation tiles, `sidewalk_factor` at its default 1.0. It has no kerb option, does not enforce `max_grade`, and penalises steps without forbidding them. It is a stair-avoiding foot baseline, not a wheelchair router | Arm A | Geometry only |
| Valhalla foot | Valhalla pedestrian costing as shipped | Arm A | Geometry only |

Rows matched by geometry only (ORS foot, both Valhalla rows) are not verified by the way-id method. Geometry matching overcounted steps for ORS before the way-id fix, so treat their steps, roadway and incline figures as less certain.

Tables, in order:

| Table | What it counts |
|---|---|
| Pairs | Pairs per area, pairs whose snapped ends lie more than 25 m apart between routers, and the rest (measured pairs) |
| Route found, all pairs / measured pairs | Share of pairs for which the router returned a route |
| Found by both / this graph only / the other only / neither | Counts over measured pairs, against this graph's wheelchair profile |
| Detour over the same engine's foot route | Route length over the same engine's foot route length: median, 90th percentile, share over 1.5 |
| Overlap with this graph's wheelchair route | For pairs both found: the median of the smaller of two shares (this route within 10 m of this graph's route, and the reverse), and the share of pairs where that is 0.9 or more (the same route) |
| Audit against this graph's data (five tables) | Share of the router's routes on measured pairs that this graph's data flags. "No surveyed ramp within reach" is a crossing without a surveyed ramp within 5 m of each end. "Over this profile's incline limits" is any non-street, non-step edge steeper than 8.3% up or 10% down in the direction of travel on this graph's LiDAR incline, short edges included. "Steps" is a steps edge. "Over 10 m in the roadway" is more than 10 m on edges this graph classes as streets, that is, street centrelines, including centrelines where OSM tags a sidewalk on the street (`sidewalk=*`), so it does not mean travel in the carriageway. "Any of the four" is any of these |
| Unramped crossings per km | Crossings with no surveyed ramp within 5 m, per km of route |
| Raw OSM audit (seven tables) | Share of the router's routes on measured pairs that use a way whose raw OSM tags a wheelchair router could refuse (`compare/osm_tags.py`, `barriers`). "incline": an `incline` tag over 8.3%, or up, down or steep. "kerb=raised": a `footway=crossing` way with any node tagged `kerb=raised`, which is a coarse way-level flag. "smoothness": intermediate or worse. "surface": a rough surface value. "steps": `highway=steps`. "wheelchair=no". "any": any of these |
| ORS settings matrix | Share of all pairs with a route for each ORS configuration in `compare/run_ors.py`. `foot` is foot-walking with `shortest`, `foot_rec` foot-walking as shipped, `default` wheelchair with no restrictions, `i6`/`i10` the incline limit, `k3`/`k6` the kerb limit in cm, `_w90` a minimum width of 0.9 m, `rec_` the recommended weighting (the others use `shortest`) |
| This graph against the reference | Same or different route, found by one only, or neither, and the cause of each disagreement: the first thing on the ORS route that this profile refuses |

The audit tables judge every router by this dataset's ramp survey and LiDAR incline, which only this graph and ORS arms B and C had. This graph's wheelchair profile scores 0% on them by construction. This graph's own walking profile scores 90.8% on "any of the four" in Brooklyn, against 91.3% for the ORS reference. On the raw OSM audit, which uses OSM's own tags, this graph's wheelchair routes score 43.7% in Brooklyn.

`kerb=raised` and ORS. ORS 10.0.1 does not read `kerb=raised` as a kerb height: on the tag probe fixture a crossing with `kerb=raised` passes every kerb limit (`../probes/ors_tag_probe.json`). By the way-level raw OSM audit below, 51.3% of the reference's Brooklyn routes (66.7% city-wide) use a crossing way with a node tagged `kerb=raised`. The flag is coarse: the raised node can sit anywhere on the crossing way, for example at a median, and a node-level count for routes was not made. Part of the gap between ORS and this graph is therefore in how ORS reads OSM's own kerb tags, and part is the ramp survey. Separately, a bare `kerb:height` of 0.15 m or more passes every kerb limit on the probe; that parsing is ORS issue #2293 (https://github.com/GIScience/openrouteservice/issues/2293).

### Pairs

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| pairs | 2000 | 2000 | 2000 | 2000 | 2000 | 2000 | 420 | 8 | 9 |
| snaps over 25 m apart | 50 | 126 | 71 | 198 | 139 | 112 | 58 | 1 | 1 |
| measured | 1950 | 1874 | 1929 | 1802 | 1861 | 1888 | 362 | 7 | 8 |

### Route found, all pairs

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 91.2% | 67.8% | 64.0% | 50.8% | 50.3% | 45.1% | 100.0% | 100.0% | 88.9% |
| ORS raw OSM, incline 10 kerb 0.06 | 98.8% | 98.1% | 97.7% | 96.6% | 98.2% | 97.9% | 100.0% | 100.0% | 88.9% |
| ORS raw OSM, incline 6 kerb 0.06 | 98.4% | 98.0% | 95.9% | 96.4% | 97.9% | 97.4% | 100.0% | 100.0% | 88.9% |
| ORS raw OSM, no limits given | 99.0% | 98.1% | 97.7% | 96.6% | 98.2% | 97.9% | 100.0% | 100.0% | 88.9% |
| ORS on this graph, strict kerbs, 10/0.06 | 95.4% | 81.5% | 67.0% | 84.2% | 81.0% | 54.9% | 100.0% | 100.0% | 88.9% |
| ORS on this graph, strict kerbs, 6/0.06 | 91.2% | 77.1% | 55.6% | 72.9% | 66.2% | 29.2% | 97.6% | 87.5% | 44.4% |
| ORS on this graph, known kerbs only, 10/0.06 | 97.4% | 83.5% | 70.2% | 88.8% | 89.6% | 56.6% | 100.0% | 100.0% | 88.9% |
| Valhalla wheelchair type | 98.2% | 96.8% | 94.2% | 97.0% | 97.7% | 96.7% | 100.0% | 100.0% | 100.0% |
| this graph, walk | 94.8% | 74.3% | 80.8% | 75.1% | 69.4% | 52.9% | 100.0% | 100.0% | 100.0% |
| ORS raw OSM, foot | 100.0% | 98.9% | 100.0% | 99.6% | 100.0% | 99.8% | 100.0% | 100.0% | 100.0% |
| Valhalla foot | 100.0% | 98.8% | 100.0% | 99.6% | 100.0% | 99.8% | 100.0% | 100.0% | 100.0% |

### Route found, measured pairs

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 92.6% | 71.3% | 65.9% | 54.7% | 53.3% | 47.1% | 100.0% | 100.0% | 87.5% |
| ORS raw OSM, incline 10 kerb 0.06 | 99.0% | 98.1% | 97.9% | 96.6% | 98.4% | 97.9% | 100.0% | 100.0% | 87.5% |
| ORS raw OSM, incline 6 kerb 0.06 | 98.6% | 98.1% | 96.0% | 96.3% | 98.0% | 97.5% | 100.0% | 100.0% | 87.5% |
| ORS raw OSM, no limits given | 99.1% | 98.1% | 97.9% | 96.6% | 98.4% | 97.9% | 100.0% | 100.0% | 87.5% |
| ORS on this graph, strict kerbs, 10/0.06 | 95.6% | 81.8% | 67.2% | 87.5% | 83.7% | 55.2% | 100.0% | 100.0% | 87.5% |
| ORS on this graph, strict kerbs, 6/0.06 | 91.5% | 78.0% | 55.8% | 76.1% | 69.0% | 29.7% | 97.2% | 85.7% | 37.5% |
| ORS on this graph, known kerbs only, 10/0.06 | 97.5% | 83.9% | 70.4% | 92.5% | 92.4% | 56.8% | 100.0% | 100.0% | 87.5% |
| Valhalla wheelchair type | 99.1% | 98.5% | 94.7% | 98.1% | 98.4% | 97.8% | 100.0% | 100.0% | 100.0% |
| this graph, walk | 96.2% | 76.8% | 81.3% | 81.0% | 70.9% | 54.5% | 100.0% | 100.0% | 100.0% |
| ORS raw OSM, foot | 100.0% | 98.8% | 100.0% | 99.8% | 100.0% | 99.7% | 100.0% | 100.0% | 100.0% |
| Valhalla foot | 100.0% | 98.8% | 100.0% | 100.0% | 100.0% | 99.7% | 100.0% | 100.0% | 100.0% |

### Found by both / this graph only / the other only / neither (measured pairs)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| ORS raw OSM, incline 10 kerb 0.06 | 1796 / 10 / 134 / 10 | 1335 / 1 / 503 / 35 | 1256 / 15 / 633 / 25 | 956 / 30 / 785 / 31 | 985 / 7 / 846 / 23 | 882 / 7 / 966 / 33 | 362 / 0 / 0 / 0 | 7 / 0 / 0 / 0 | 7 / 0 / 0 / 1 |
| ORS raw OSM, incline 6 kerb 0.06 | 1790 / 16 / 132 / 12 | 1335 / 1 / 503 / 35 | 1234 / 37 / 617 / 41 | 954 / 32 / 782 / 34 | 982 / 10 / 841 / 28 | 880 / 9 / 960 / 39 | 362 / 0 / 0 / 0 | 7 / 0 / 0 / 0 | 7 / 0 / 0 / 1 |
| ORS raw OSM, no limits given | 1798 / 8 / 135 / 9 | 1335 / 1 / 503 / 35 | 1256 / 15 / 633 / 25 | 956 / 30 / 785 / 31 | 985 / 7 / 846 / 23 | 882 / 7 / 967 / 32 | 362 / 0 / 0 / 0 | 7 / 0 / 0 / 0 | 7 / 0 / 0 / 1 |
| ORS on this graph, strict kerbs, 10/0.06 | 1769 / 37 / 96 / 48 | 1312 / 24 / 220 / 318 | 1193 / 78 / 104 / 554 | 927 / 59 / 649 / 167 | 949 / 43 / 608 / 261 | 853 / 36 / 189 / 810 | 362 / 0 / 0 / 0 | 7 / 0 / 0 / 0 | 6 / 1 / 1 / 0 |
| ORS on this graph, strict kerbs, 6/0.06 | 1729 / 77 / 56 / 88 | 1271 / 65 / 191 / 347 | 1046 / 225 / 31 / 627 | 852 / 134 / 520 / 296 | 857 / 135 / 428 / 441 | 482 / 407 / 79 / 920 | 352 / 10 / 0 / 0 | 6 / 1 / 0 / 0 | 3 / 4 / 0 / 1 |
| ORS on this graph, known kerbs only, 10/0.06 | 1789 / 17 / 112 / 32 | 1328 / 8 / 245 / 293 | 1218 / 53 / 140 / 518 | 956 / 30 / 710 / 106 | 975 / 17 / 744 / 125 | 866 / 23 / 207 / 792 | 362 / 0 / 0 / 0 | 7 / 0 / 0 / 0 | 6 / 1 / 1 / 0 |
| Valhalla wheelchair type | 1796 / 10 / 137 / 7 | 1335 / 1 / 511 / 27 | 1255 / 16 / 572 / 86 | 979 / 7 / 788 / 28 | 985 / 7 / 847 / 22 | 887 / 2 / 960 / 39 | 362 / 0 / 0 / 0 | 7 / 0 / 0 / 0 | 7 / 0 / 1 / 0 |

### Detour over the same engine's foot route: median (90th percentile; share over 1.5)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 1.01 (1.05; 0.5%) | 1.03 (1.14; 1.4%) | 1.04 (1.13; 2.0%) | 1.09 (1.36; 5.4%) | 1.08 (1.37; 6.1%) | 1.07 (1.52; 11.1%) | 1.00 (1.06; 2.2%) | 1.10 (1.27; 0.0%) | 3.17 (9.63; 85.7%) |
| ORS raw OSM, incline 10 kerb 0.06 | 1.01 (1.02; 0.1%) | 1.01 (1.12; 1.1%) | 1.01 (1.11; 5.2%) | 1.07 (1.21; 1.9%) | 1.03 (1.14; 0.4%) | 1.01 (1.13; 1.2%) | 1.00 (1.02; 2.2%) | 1.01 (1.05; 0.0%) | 1.01 (2.07; 28.6%) |
| ORS raw OSM, incline 6 kerb 0.06 | 1.01 (1.03; 0.1%) | 1.01 (1.12; 1.1%) | 1.01 (1.12; 5.4%) | 1.07 (1.22; 2.1%) | 1.03 (1.15; 0.4%) | 1.01 (1.13; 1.2%) | 1.00 (1.02; 2.2%) | 1.01 (1.05; 0.0%) | 1.01 (2.08; 28.6%) |
| ORS raw OSM, no limits given | 1.01 (1.02; 0.1%) | 1.01 (1.12; 1.1%) | 1.01 (1.11; 5.1%) | 1.07 (1.21; 1.9%) | 1.03 (1.14; 0.4%) | 1.01 (1.13; 1.2%) | 1.00 (1.02; 2.2%) | 1.01 (1.05; 0.0%) | 1.01 (2.07; 28.6%) |
| ORS on this graph, strict kerbs, 10/0.06 | 1.02 (1.05; 0.1%) | 1.03 (1.11; 0.1%) | 1.03 (1.13; 1.5%) | 1.12 (1.34; 3.5%) | 1.07 (1.29; 4.2%) | 1.07 (1.57; 12.4%) | 1.00 (1.06; 2.5%) | 1.05 (1.25; 0.0%) | 3.20 (9.07; 71.4%) |
| ORS on this graph, strict kerbs, 6/0.06 | 1.02 (1.05; 0.2%) | 1.05 (1.16; 0.1%) | 1.03 (1.13; 2.1%) | 1.16 (1.46; 8.7%) | 1.11 (1.41; 7.9%) | 1.04 (1.18; 1.8%) | 1.01 (1.09; 3.1%) | 1.07 (1.21; 0.0%) | 1.06 (7.93; 33.3%) |
| ORS on this graph, known kerbs only, 10/0.06 | 1.01 (1.02; 0.1%) | 1.01 (1.06; 0.1%) | 1.01 (1.06; 0.6%) | 1.07 (1.22; 1.1%) | 1.06 (1.25; 3.2%) | 1.04 (1.52; 10.5%) | 1.00 (1.02; 2.2%) | 1.02 (1.11; 0.0%) | 2.81 (7.61; 71.4%) |
| Valhalla wheelchair type | 1.00 (1.00; 0.1%) | 1.00 (1.00; 0.4%) | 1.00 (1.02; 1.3%) | 1.00 (1.03; 0.2%) | 1.00 (1.01; 0.1%) | 1.00 (1.01; 1.9%) | 1.00 (1.00; 1.9%) | 1.00 (1.01; 0.0%) | 1.00 (1.50; 12.5%) |

### Overlap with this graph's wheelchair route: median (share that are the same route)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| ORS raw OSM, incline 10 kerb 0.06 | 0.46 (5.2%) | 0.38 (3.2%) | 0.33 (3.0%) | 0.25 (2.6%) | 0.27 (2.7%) | 0.25 (1.1%) | 0.80 (35.9%) | 0.45 (0.0%) | 0.01 (0.0%) |
| ORS raw OSM, incline 6 kerb 0.06 | 0.45 (5.1%) | 0.38 (3.2%) | 0.32 (3.0%) | 0.25 (2.6%) | 0.27 (2.7%) | 0.25 (1.1%) | 0.80 (35.9%) | 0.45 (0.0%) | 0.01 (0.0%) |
| ORS raw OSM, no limits given | 0.45 (5.2%) | 0.38 (3.2%) | 0.32 (3.0%) | 0.25 (2.6%) | 0.27 (2.7%) | 0.24 (1.1%) | 0.80 (35.9%) | 0.45 (0.0%) | 0.01 (0.0%) |
| ORS on this graph, strict kerbs, 10/0.06 | 0.73 (22.7%) | 0.68 (11.9%) | 0.69 (15.8%) | 0.60 (12.5%) | 0.58 (9.9%) | 0.69 (7.5%) | 0.90 (49.7%) | 0.81 (28.6%) | 0.60 (16.7%) |
| ORS on this graph, strict kerbs, 6/0.06 | 0.69 (19.3%) | 0.65 (9.4%) | 0.65 (13.1%) | 0.36 (4.7%) | 0.53 (7.0%) | 0.62 (7.1%) | 0.84 (43.5%) | 0.84 (33.3%) | 0.39 (0.0%) |
| ORS on this graph, known kerbs only, 10/0.06 | 0.48 (6.5%) | 0.40 (4.1%) | 0.39 (4.0%) | 0.35 (4.0%) | 0.37 (3.7%) | 0.37 (1.3%) | 0.79 (37.8%) | 0.50 (0.0%) | 0.45 (0.0%) |
| Valhalla wheelchair type | 0.34 (4.3%) | 0.32 (3.9%) | 0.12 (2.0%) | 0.12 (1.6%) | 0.11 (2.6%) | 0.07 (0.9%) | 0.82 (41.2%) | 0.66 (0.0%) | 0.01 (0.0%) |
| this graph, walk | 0.57 (18.4%) | 0.45 (11.9%) | 0.33 (6.2%) | 0.38 (5.6%) | 0.38 (6.7%) | 0.22 (5.1%) | 1.00 (62.4%) | 0.61 (0.0%) | 0.01 (0.0%) |
| ORS raw OSM, foot | 0.52 (9.2%) | 0.38 (5.9%) | 0.24 (4.7%) | 0.20 (3.6%) | 0.19 (2.1%) | 0.15 (1.7%) | 0.90 (49.7%) | 0.60 (0.0%) | 0.01 (0.0%) |
| Valhalla foot | 0.34 (4.1%) | 0.31 (3.8%) | 0.11 (1.7%) | 0.11 (1.5%) | 0.11 (2.6%) | 0.06 (0.7%) | 0.81 (40.6%) | 0.61 (0.0%) | 0.01 (0.0%) |

### Routes with a crossing that has no surveyed ramp within reach (audit against this graph's data)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 87.6% | 94.7% | 93.5% | 95.1% | 94.4% | 98.3% | 41.7% | 100.0% | 85.7% |
| ORS raw OSM, incline 6 kerb 0.06 | 87.8% | 94.7% | 93.5% | 95.1% | 94.3% | 98.3% | 41.7% | 100.0% | 85.7% |
| ORS raw OSM, no limits given | 88.8% | 94.6% | 93.7% | 95.1% | 94.5% | 98.3% | 41.7% | 100.0% | 100.0% |
| ORS on this graph, strict kerbs, 10/0.06 | 14.1% | 33.7% | 25.0% | 41.0% | 44.5% | 34.4% | 1.9% | 57.1% | 14.3% |
| ORS on this graph, strict kerbs, 6/0.06 | 15.3% | 36.0% | 26.4% | 66.0% | 59.2% | 35.6% | 2.0% | 50.0% | 33.3% |
| ORS on this graph, known kerbs only, 10/0.06 | 89.2% | 93.7% | 94.0% | 95.7% | 93.6% | 97.7% | 41.2% | 100.0% | 100.0% |
| Valhalla wheelchair type | 69.0% | 69.3% | 80.3% | 73.0% | 61.0% | 87.7% | 35.9% | 71.4% | 75.0% |
| this graph, walk | 88.1% | 92.2% | 95.0% | 95.3% | 92.1% | 96.7% | 40.9% | 100.0% | 75.0% |
| ORS raw OSM, foot | 86.4% | 93.2% | 91.7% | 91.5% | 91.2% | 97.1% | 43.6% | 100.0% | 75.0% |
| Valhalla foot | 70.0% | 69.4% | 80.1% | 72.1% | 66.5% | 88.2% | 34.0% | 71.4% | 62.5% |

### Routes with an edge over this profile's incline limits (audit against this graph's data)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 34.4% | 65.6% | 65.2% | 77.0% | 86.8% | 85.3% | 2.5% | 42.9% | 85.7% |
| ORS raw OSM, incline 6 kerb 0.06 | 33.4% | 65.6% | 64.8% | 77.2% | 86.6% | 85.0% | 2.5% | 42.9% | 85.7% |
| ORS raw OSM, no limits given | 33.4% | 65.7% | 65.6% | 77.0% | 86.8% | 84.9% | 2.5% | 42.9% | 85.7% |
| ORS on this graph, strict kerbs, 10/0.06 | 7.7% | 32.7% | 22.4% | 57.8% | 70.3% | 39.2% | 1.4% | 14.3% | 0.0% |
| ORS on this graph, strict kerbs, 6/0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, known kerbs only, 10/0.06 | 10.9% | 27.1% | 18.6% | 51.9% | 71.3% | 36.8% | 1.4% | 14.3% | 0.0% |
| Valhalla wheelchair type | 27.4% | 36.8% | 62.6% | 48.7% | 44.2% | 76.6% | 8.3% | 28.6% | 87.5% |
| this graph, walk | 34.5% | 64.7% | 65.5% | 81.8% | 87.6% | 80.6% | 3.0% | 28.6% | 87.5% |
| ORS raw OSM, foot | 36.7% | 63.7% | 74.3% | 72.8% | 82.0% | 86.8% | 4.1% | 28.6% | 87.5% |
| Valhalla foot | 28.9% | 37.4% | 63.8% | 50.2% | 51.4% | 77.1% | 7.5% | 28.6% | 75.0% |

### Routes with steps (audit against this graph's data)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 6 kerb 0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, no limits given | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, strict kerbs, 10/0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, strict kerbs, 6/0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, known kerbs only, 10/0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| Valhalla wheelchair type | 4.5% | 0.3% | 8.0% | 0.0% | 0.2% | 3.8% | 0.0% | 0.0% | 12.5% |
| this graph, walk | 17.5% | 24.5% | 48.8% | 46.0% | 5.6% | 54.6% | 8.0% | 14.3% | 25.0% |
| ORS raw OSM, foot | 18.8% | 28.2% | 51.7% | 39.9% | 11.5% | 58.1% | 7.5% | 14.3% | 37.5% |
| Valhalla foot | 10.3% | 4.7% | 24.3% | 15.3% | 2.6% | 14.4% | 5.0% | 0.0% | 25.0% |

### Routes with over 10 m in the roadway (audit against this graph's data)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 13.4% | 39.7% | 34.6% | 69.6% | 72.6% | 52.2% | 1.9% | 14.3% | 28.6% |
| ORS raw OSM, incline 6 kerb 0.06 | 13.5% | 39.7% | 34.8% | 69.5% | 73.9% | 51.5% | 1.9% | 14.3% | 28.6% |
| ORS raw OSM, no limits given | 13.1% | 38.6% | 34.6% | 69.6% | 72.3% | 49.8% | 1.9% | 14.3% | 28.6% |
| ORS on this graph, strict kerbs, 10/0.06 | 23.9% | 53.6% | 32.6% | 72.0% | 80.0% | 59.9% | 1.9% | 57.1% | 57.1% |
| ORS on this graph, strict kerbs, 6/0.06 | 25.4% | 54.3% | 34.4% | 89.8% | 92.3% | 61.0% | 4.8% | 50.0% | 66.7% |
| ORS on this graph, known kerbs only, 10/0.06 | 8.4% | 28.1% | 4.5% | 49.8% | 67.4% | 37.4% | 1.9% | 0.0% | 14.3% |
| Valhalla wheelchair type | 89.5% | 96.2% | 84.2% | 97.1% | 97.9% | 97.7% | 35.4% | 57.1% | 62.5% |
| this graph, walk | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, foot | 85.8% | 94.8% | 66.6% | 94.1% | 97.5% | 96.5% | 53.3% | 42.9% | 62.5% |
| Valhalla foot | 88.8% | 96.3% | 84.5% | 96.8% | 98.1% | 97.3% | 33.1% | 42.9% | 62.5% |

### Routes with any of the four (audit against this graph's data)

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 91.3% | 96.7% | 96.9% | 98.2% | 98.1% | 99.1% | 44.2% | 100.0% | 100.0% |
| ORS raw OSM, incline 6 kerb 0.06 | 91.2% | 96.7% | 96.7% | 98.2% | 98.1% | 99.1% | 44.2% | 100.0% | 100.0% |
| ORS raw OSM, no limits given | 92.1% | 96.7% | 96.9% | 98.2% | 98.1% | 99.1% | 44.2% | 100.0% | 100.0% |
| ORS on this graph, strict kerbs, 10/0.06 | 29.2% | 69.0% | 48.6% | 85.5% | 92.7% | 75.5% | 5.2% | 57.1% | 57.1% |
| ORS on this graph, strict kerbs, 6/0.06 | 27.4% | 56.5% | 36.9% | 90.0% | 92.3% | 62.6% | 6.8% | 50.0% | 66.7% |
| ORS on this graph, known kerbs only, 10/0.06 | 90.3% | 95.4% | 94.7% | 97.7% | 97.4% | 98.2% | 42.3% | 100.0% | 100.0% |
| Valhalla wheelchair type | 96.4% | 98.8% | 98.2% | 99.4% | 99.1% | 99.5% | 53.0% | 100.0% | 100.0% |
| this graph, walk | 90.8% | 94.4% | 97.9% | 98.0% | 96.7% | 98.0% | 47.2% | 100.0% | 100.0% |
| ORS raw OSM, foot | 98.3% | 99.4% | 98.6% | 99.3% | 99.6% | 99.9% | 74.9% | 100.0% | 100.0% |
| Valhalla foot | 96.3% | 98.8% | 98.7% | 99.3% | 99.1% | 99.5% | 53.3% | 100.0% | 100.0% |

### Unramped crossings per km of route

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| ORS raw OSM, incline 10 kerb 0.06 | 0.765 | 0.779 | 1.094 | 1.022 | 0.961 | 0.708 | 0.669 | 2.085 | 1.021 |
| ORS raw OSM, incline 6 kerb 0.06 | 0.766 | 0.782 | 1.103 | 0.973 | 0.96 | 0.707 | 0.669 | 2.084 | 1.021 |
| ORS raw OSM, no limits given | 0.807 | 0.773 | 1.097 | 1.048 | 0.961 | 0.724 | 0.669 | 2.152 | 1.129 |
| ORS on this graph, strict kerbs, 10/0.06 | 0.024 | 0.06 | 0.055 | 0.096 | 0.076 | 0.029 | 0.014 | 0.273 | 0.018 |
| ORS on this graph, strict kerbs, 6/0.06 | 0.029 | 0.066 | 0.059 | 0.161 | 0.103 | 0.046 | 0.014 | 0.379 | 0.066 |
| ORS on this graph, known kerbs only, 10/0.06 | 0.799 | 0.715 | 1.015 | 1.146 | 0.916 | 0.891 | 0.699 | 2.481 | 1.327 |
| Valhalla wheelchair type | 0.228 | 0.159 | 0.302 | 0.328 | 0.144 | 0.115 | 0.483 | 0.536 | 0.594 |
| this graph, walk | 0.697 | 0.62 | 1.033 | 1.083 | 0.887 | 0.763 | 0.713 | 2.219 | 0.815 |
| ORS raw OSM, foot | 0.63 | 0.619 | 0.62 | 0.895 | 0.781 | 0.508 | 0.751 | 2.406 | 0.748 |
| Valhalla foot | 0.234 | 0.164 | 0.298 | 0.321 | 0.165 | 0.123 | 0.431 | 0.697 | 0.407 |

### Raw OSM audit: routes with any

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 43.7% | 25.6% | 56.9% | 26.2% | 38.0% | 65.2% | 8.3% | 42.9% | 71.4% |
| ORS raw OSM, incline 10 kerb 0.06 | 60.8% | 33.7% | 71.4% | 29.5% | 56.9% | 79.6% | 21.0% | 57.1% | 42.9% |
| ORS raw OSM, incline 6 kerb 0.06 | 59.9% | 33.1% | 69.4% | 25.8% | 56.3% | 77.6% | 21.0% | 57.1% | 42.9% |
| ORS raw OSM, no limits given | 62.9% | 35.1% | 71.4% | 29.3% | 57.0% | 79.9% | 21.0% | 57.1% | 42.9% |
| ORS on this graph, strict kerbs, 10/0.06 | 44.6% | 31.1% | 52.5% | 32.2% | 40.5% | 66.6% | 13.8% | 57.1% | 71.4% |
| ORS on this graph, strict kerbs, 6/0.06 | 44.1% | 23.2% | 51.8% | 28.7% | 30.3% | 38.1% | 13.6% | 50.0% | 66.7% |
| ORS on this graph, known kerbs only, 10/0.06 | 61.9% | 31.7% | 69.5% | 38.2% | 58.0% | 76.0% | 20.2% | 57.1% | 71.4% |
| Valhalla wheelchair type | 39.2% | 15.4% | 50.6% | 44.3% | 13.5% | 54.7% | 18.5% | 28.6% | 62.5% |
| this graph, walk | 64.9% | 44.0% | 80.0% | 70.8% | 72.5% | 81.6% | 26.5% | 42.9% | 37.5% |
| ORS raw OSM, foot | 63.3% | 50.2% | 81.9% | 70.0% | 72.8% | 87.0% | 26.2% | 57.1% | 62.5% |
| Valhalla foot | 44.0% | 23.1% | 64.0% | 57.5% | 33.6% | 63.5% | 19.3% | 28.6% | 50.0% |

### Raw OSM audit: routes with incline

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 2.4% | 5.4% | 7.1% | 4.8% | 0.3% | 3.6% | 0.0% | 14.3% | 14.3% |
| ORS raw OSM, incline 10 kerb 0.06 | 4.8% | 7.7% | 20.5% | 8.7% | 5.3% | 15.6% | 0.0% | 28.6% | 28.6% |
| ORS raw OSM, incline 6 kerb 0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, no limits given | 4.8% | 7.7% | 20.5% | 8.7% | 5.6% | 15.3% | 0.0% | 28.6% | 28.6% |
| ORS on this graph, strict kerbs, 10/0.06 | 2.3% | 10.3% | 6.9% | 5.6% | 0.8% | 3.5% | 0.0% | 14.3% | 14.3% |
| ORS on this graph, strict kerbs, 6/0.06 | 0.9% | 0.5% | 5.1% | 1.5% | 0.5% | 0.9% | 0.0% | 16.7% | 0.0% |
| ORS on this graph, known kerbs only, 10/0.06 | 3.3% | 5.9% | 9.3% | 5.7% | 0.9% | 9.6% | 0.0% | 14.3% | 14.3% |
| Valhalla wheelchair type | 6.5% | 7.3% | 13.0% | 10.2% | 4.5% | 23.0% | 0.0% | 0.0% | 12.5% |
| this graph, walk | 21.2% | 16.5% | 43.0% | 41.0% | 8.9% | 53.9% | 8.0% | 28.6% | 25.0% |
| ORS raw OSM, foot | 20.7% | 21.3% | 46.5% | 35.6% | 13.7% | 56.9% | 7.5% | 28.6% | 50.0% |
| Valhalla foot | 10.9% | 10.4% | 25.6% | 19.1% | 5.5% | 28.9% | 5.0% | 0.0% | 25.0% |

### Raw OSM audit: routes with kerb=raised

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 27.5% | 3.8% | 24.1% | 2.8% | 23.6% | 29.5% | 8.3% | 0.0% | 14.3% |
| ORS raw OSM, incline 10 kerb 0.06 | 51.3% | 25.2% | 54.7% | 8.0% | 45.0% | 66.7% | 21.0% | 42.9% | 28.6% |
| ORS raw OSM, incline 6 kerb 0.06 | 51.8% | 25.1% | 54.5% | 8.1% | 44.4% | 66.5% | 21.0% | 42.9% | 28.6% |
| ORS raw OSM, no limits given | 54.8% | 26.7% | 54.7% | 8.1% | 45.1% | 67.4% | 21.0% | 42.9% | 28.6% |
| ORS on this graph, strict kerbs, 10/0.06 | 28.4% | 8.2% | 34.2% | 4.5% | 22.4% | 32.2% | 13.8% | 0.0% | 14.3% |
| ORS on this graph, strict kerbs, 6/0.06 | 29.5% | 7.9% | 36.3% | 3.4% | 23.7% | 22.6% | 13.6% | 0.0% | 0.0% |
| ORS on this graph, known kerbs only, 10/0.06 | 53.0% | 21.2% | 53.2% | 6.5% | 42.4% | 57.2% | 20.2% | 42.9% | 42.9% |
| Valhalla wheelchair type | 25.6% | 3.9% | 10.7% | 1.0% | 8.6% | 14.5% | 18.5% | 0.0% | 0.0% |
| this graph, walk | 48.2% | 16.4% | 49.6% | 11.1% | 38.4% | 61.0% | 20.2% | 28.6% | 0.0% |
| ORS raw OSM, foot | 44.0% | 22.5% | 37.2% | 7.3% | 36.9% | 53.5% | 19.9% | 28.6% | 0.0% |
| Valhalla foot | 26.1% | 5.3% | 17.5% | 1.1% | 8.8% | 17.1% | 14.6% | 0.0% | 0.0% |

### Raw OSM audit: routes with smoothness

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 17.9% | 5.7% | 28.6% | 17.5% | 12.5% | 19.7% | 0.0% | 14.3% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 16.6% | 5.5% | 22.3% | 20.6% | 20.9% | 36.1% | 0.0% | 28.6% | 0.0% |
| ORS raw OSM, incline 6 kerb 0.06 | 16.2% | 5.5% | 22.5% | 17.6% | 20.9% | 36.1% | 0.0% | 28.6% | 0.0% |
| ORS raw OSM, no limits given | 16.2% | 5.5% | 22.3% | 20.4% | 20.9% | 36.0% | 0.0% | 28.6% | 0.0% |
| ORS on this graph, strict kerbs, 10/0.06 | 17.8% | 6.9% | 22.7% | 22.2% | 16.6% | 21.5% | 0.0% | 28.6% | 0.0% |
| ORS on this graph, strict kerbs, 6/0.06 | 18.3% | 5.7% | 20.9% | 24.0% | 4.7% | 13.2% | 0.0% | 33.3% | 0.0% |
| ORS on this graph, known kerbs only, 10/0.06 | 16.9% | 6.5% | 29.9% | 34.4% | 15.7% | 26.1% | 0.0% | 28.6% | 0.0% |
| Valhalla wheelchair type | 8.0% | 2.5% | 12.6% | 39.3% | 1.0% | 17.6% | 0.0% | 14.3% | 0.0% |
| this graph, walk | 21.0% | 6.9% | 28.7% | 35.8% | 17.7% | 29.6% | 0.0% | 14.3% | 0.0% |
| ORS raw OSM, foot | 18.7% | 6.3% | 12.0% | 36.9% | 10.7% | 30.9% | 0.0% | 14.3% | 0.0% |
| Valhalla foot | 7.3% | 2.9% | 12.9% | 42.8% | 1.1% | 17.5% | 0.0% | 14.3% | 0.0% |

### Raw OSM audit: routes with steps

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 6 kerb 0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, no limits given | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, strict kerbs, 10/0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, strict kerbs, 6/0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, known kerbs only, 10/0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| Valhalla wheelchair type | 4.5% | 0.3% | 8.0% | 0.0% | 0.2% | 3.8% | 0.0% | 0.0% | 12.5% |
| this graph, walk | 17.5% | 24.5% | 48.8% | 46.0% | 5.6% | 54.6% | 8.0% | 14.3% | 25.0% |
| ORS raw OSM, foot | 18.8% | 28.2% | 51.7% | 39.9% | 11.5% | 58.1% | 7.5% | 14.3% | 37.5% |
| Valhalla foot | 10.3% | 4.7% | 24.3% | 15.3% | 2.6% | 14.4% | 5.0% | 0.0% | 25.0% |

### Raw OSM audit: routes with surface

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 12.0% | 15.8% | 24.2% | 5.6% | 15.1% | 48.7% | 0.0% | 28.6% | 57.1% |
| ORS raw OSM, incline 10 kerb 0.06 | 8.5% | 10.0% | 30.9% | 7.9% | 8.2% | 18.0% | 0.0% | 28.6% | 28.6% |
| ORS raw OSM, incline 6 kerb 0.06 | 7.1% | 9.7% | 30.9% | 8.0% | 8.2% | 17.6% | 0.0% | 28.6% | 28.6% |
| ORS raw OSM, no limits given | 8.7% | 10.0% | 30.9% | 7.9% | 8.1% | 18.3% | 0.0% | 28.6% | 28.6% |
| ORS on this graph, strict kerbs, 10/0.06 | 9.1% | 19.1% | 21.2% | 6.7% | 8.0% | 48.9% | 0.0% | 28.6% | 71.4% |
| ORS on this graph, strict kerbs, 6/0.06 | 6.7% | 7.8% | 21.2% | 3.1% | 3.6% | 8.0% | 0.0% | 0.0% | 66.7% |
| ORS on this graph, known kerbs only, 10/0.06 | 8.6% | 10.2% | 23.8% | 3.9% | 8.9% | 47.4% | 0.0% | 28.6% | 71.4% |
| Valhalla wheelchair type | 10.6% | 3.2% | 31.8% | 4.8% | 2.7% | 21.3% | 0.0% | 28.6% | 50.0% |
| this graph, walk | 15.3% | 19.4% | 31.7% | 11.7% | 40.8% | 35.8% | 0.0% | 14.3% | 37.5% |
| ORS raw OSM, foot | 17.4% | 17.0% | 39.6% | 19.5% | 46.7% | 43.7% | 0.0% | 42.9% | 50.0% |
| Valhalla foot | 15.8% | 8.3% | 38.7% | 13.0% | 24.1% | 32.8% | 0.0% | 28.6% | 37.5% |

### Raw OSM audit: routes with wheelchair=no

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| this graph, wheelchair | 0.1% | 4.5% | 0.1% | 0.0% | 0.2% | 2.7% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 10 kerb 0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, incline 6 kerb 0.06 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, no limits given | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, strict kerbs, 10/0.06 | 0.1% | 3.9% | 0.5% | 0.1% | 0.2% | 1.9% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, strict kerbs, 6/0.06 | 0.1% | 3.5% | 0.0% | 0.1% | 0.0% | 2.9% | 0.0% | 0.0% | 0.0% |
| ORS on this graph, known kerbs only, 10/0.06 | 0.1% | 2.2% | 0.5% | 0.1% | 10.3% | 2.8% | 0.0% | 0.0% | 0.0% |
| Valhalla wheelchair type | 0.1% | 0.0% | 0.0% | 0.0% | 0.0% | 0.2% | 0.0% | 0.0% | 12.5% |
| this graph, walk | 1.9% | 2.3% | 0.7% | 0.1% | 3.3% | 18.5% | 0.0% | 0.0% | 0.0% |
| ORS raw OSM, foot | 1.8% | 2.1% | 0.6% | 3.4% | 3.4% | 7.7% | 0.0% | 0.0% | 12.5% |
| Valhalla foot | 0.5% | 0.5% | 0.4% | 1.4% | 0.8% | 2.4% | 0.0% | 0.0% | 12.5% |

### ORS settings matrix, raw OSM: share of all pairs with a route

| setting | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| default | 99.0% | 98.1% | 97.7% | 96.6% | 98.2% | 97.9% | 100.0% | 100.0% | 88.9% |
| foot | 100.0% | 98.9% | 100.0% | 99.6% | 100.0% | 99.8% | 100.0% | 100.0% | 100.0% |
| foot_rec | 100.0% | 98.9% | 100.0% | 99.6% | 100.0% | 99.8% | 100.0% | 100.0% | 100.0% |
| i10_k3 | 42.1% | 86.9% | 73.5% | 90.3% | 87.2% | 75.4% | 39.1% | 75.0% | 55.6% |
| i10_k3_w90 | 42.1% | 86.9% | 73.5% | 90.3% | 86.2% | 75.3% | 39.1% | 75.0% | 55.6% |
| i10_k6 | 98.8% | 98.1% | 97.7% | 96.6% | 98.2% | 97.9% | 100.0% | 100.0% | 88.9% |
| i10_k6_w90 | 98.8% | 98.1% | 97.7% | 96.6% | 97.1% | 97.7% | 100.0% | 100.0% | 88.9% |
| i6_k3 | 41.8% | 86.9% | 72.3% | 90.0% | 86.8% | 75.0% | 39.1% | 75.0% | 55.6% |
| i6_k3_w90 | 41.8% | 86.9% | 72.3% | 90.0% | 85.9% | 74.8% | 39.1% | 75.0% | 55.6% |
| i6_k6 | 98.4% | 98.0% | 95.9% | 96.4% | 97.9% | 97.4% | 100.0% | 100.0% | 88.9% |
| i6_k6_w90 | 98.4% | 98.0% | 95.9% | 96.4% | 96.7% | 97.2% | 100.0% | 100.0% | 88.9% |
| rec_i10_k6 | 98.8% | 98.1% | 97.7% | 96.6% | 98.2% | 97.9% | 100.0% | 100.0% | 88.9% |
| rec_i6_k6 | 98.4% | 98.0% | 95.9% | 96.4% | 97.9% | 97.4% | 100.0% | 100.0% | 88.9% |

### ORS settings matrix, ors_armB_strict: share of all pairs with a route

| setting | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| default | 100.0% | 98.0% | 82.1% | 96.6% | 98.9% | 64.2% | 100.0% | 100.0% | 100.0% |
| foot | 100.0% | 98.2% | 82.1% | 96.5% | 98.9% | 64.3% | 100.0% | 100.0% | 100.0% |
| foot_rec | 100.0% | 98.2% | 82.1% | 96.5% | 98.9% | 64.3% | 100.0% | 100.0% | 100.0% |
| i10_k3 | 27.8% | 25.1% | 24.0% | 30.0% | 22.4% | 10.2% | 31.4% | 25.0% | 55.6% |
| i10_k3_w90 | 27.8% | 25.1% | 24.0% | 30.0% | 22.4% | 10.3% | 31.4% | 25.0% | 55.6% |
| i10_k6 | 95.4% | 81.5% | 67.0% | 84.2% | 81.0% | 54.9% | 100.0% | 100.0% | 88.9% |
| i10_k6_w90 | 95.4% | 81.5% | 67.0% | 84.2% | 80.8% | 54.9% | 100.0% | 100.0% | 88.9% |
| i6_k3 | 25.1% | 22.7% | 17.9% | 23.8% | 16.8% | 8.5% | 28.8% | 25.0% | 44.4% |
| i6_k3_w90 | 25.1% | 22.7% | 17.9% | 23.8% | 16.7% | 8.6% | 28.8% | 25.0% | 44.4% |
| i6_k6 | 91.2% | 77.1% | 55.6% | 72.9% | 66.2% | 29.2% | 97.6% | 87.5% | 44.4% |
| i6_k6_w90 | 91.2% | 77.1% | 55.6% | 72.9% | 66.2% | 29.2% | 97.6% | 87.5% | 44.4% |
| rec_i10_k6 | 95.4% | 81.5% | 67.0% | 84.2% | 81.0% | 54.9% | 100.0% | 100.0% | 88.9% |
| rec_i6_k6 | 91.2% | 77.1% | 55.6% | 72.9% | 66.2% | 29.2% | 97.6% | 87.5% | 44.4% |

### ORS settings matrix, ors_armB_known: share of all pairs with a route

| setting | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| default | 100.0% | 98.0% | 82.1% | 96.6% | 98.9% | 64.2% | 100.0% | 100.0% | 100.0% |
| foot | 100.0% | 98.2% | 82.1% | 96.5% | 98.9% | 64.3% | 100.0% | 100.0% | 100.0% |
| i10_k3 | 38.6% | 33.1% | 40.8% | 42.9% | 36.1% | 14.4% | 43.1% | 75.0% | 55.6% |
| i10_k3_w90 | 38.6% | 33.1% | 40.8% | 42.9% | 36.2% | 14.5% | 43.1% | 75.0% | 55.6% |
| i10_k6 | 97.4% | 83.5% | 70.2% | 88.8% | 89.6% | 56.6% | 100.0% | 100.0% | 88.9% |
| i10_k6_w90 | 97.4% | 83.5% | 70.2% | 88.8% | 89.5% | 56.6% | 100.0% | 100.0% | 88.9% |
| i6_k3 | 35.6% | 30.0% | 32.7% | 35.9% | 27.2% | 12.2% | 40.0% | 75.0% | 44.4% |
| i6_k3_w90 | 35.6% | 30.0% | 32.7% | 35.9% | 27.2% | 12.3% | 40.0% | 75.0% | 44.4% |
| i6_k6 | 93.2% | 79.7% | 59.0% | 77.5% | 74.5% | 30.6% | 97.6% | 87.5% | 44.4% |
| i6_k6_w90 | 93.2% | 79.7% | 59.0% | 77.5% | 74.5% | 30.6% | 97.6% | 87.5% | 44.4% |
| rec_i10_k6 | 97.4% | 83.5% | 70.2% | 88.8% | 89.6% | 56.6% | 100.0% | 100.0% | 88.9% |
| rec_i6_k6 | 93.2% | 79.7% | 59.0% | 77.5% | 74.5% | 30.6% | 97.6% | 87.5% | 44.4% |

### This graph against the reference (ORS raw OSM, recommended weighting, incline 10, kerb 0.06), measured pairs

| | BK | QN | MN | BX | SI | citywide | Brownsville | landmark hand-checked | landmark structure |
|---|---|---|---|---|---|---|---|---|---|
| same route | 93 | 43 | 38 | 25 | 27 | 10 | 130 | 0 | 0 |
| different route | 1703 | 1292 | 1218 | 931 | 958 | 872 | 232 | 7 | 7 |
| reference only | 134 | 503 | 633 | 785 | 846 | 966 | 0 | 0 | 0 |
| ours only | 10 | 1 | 15 | 30 | 7 | 7 | 0 | 0 | 0 |
| neither | 10 | 35 | 25 | 31 | 23 | 33 | 0 | 0 | 1 |
| cause: kerb data | 1417 | 1307 | 1170 | 830 | 892 | 1215 | 141 | 6 | 4 |
| cause: incline data | 201 | 216 | 542 | 466 | 539 | 361 | 5 | 1 | 2 |
| cause: structure | 19 | 16 | 25 | 24 | 3 | 23 | 0 | 0 | 1 |
| cause: connectivity | 104 | 227 | 99 | 407 | 359 | 228 | 40 | 0 | 0 |
| cause: rule | 106 | 30 | 30 | 19 | 18 | 18 | 46 | 0 | 0 |
| ORS on this graph (strict, shortest) matches this graph | 11.4% | 21.6% | 33.1% | 10.5% | 15.2% | 43.5% | 67.4% | 28.6% | 0.0% |
