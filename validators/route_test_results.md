# Unweaver routing test results

Base URL: `http://127.0.0.1:5000`
Routes tested: 10 pairs × 2 profiles = 20

## Snapped landmarks (giant component)

| Landmark | Snap node | Snap distance |
|---|---|---|
| Penn Station | `n_131685` | 47.7 m |
| Grand Central | `n_121247` | 7.0 m |
| Times Square | `n_137781` | 5.0 m |
| Empire State Building | `n_474` | 67.9 m |
| Union Square | `n_210` | 5.6 m |
| Washington Sq Park | `n_453588` | 18.0 m |
| Brooklyn Bridge MN | `n_270609` | 18.8 m |
| DUMBO | `n_396381` | 11.6 m |
| Atlantic Av-Barclays | `n_344996` | 17.7 m |
| Prospect Park | `n_69895` | 218.4 m |
| Williamsburg Bridge MN | `n_134408` | 18.0 m |
| Williamsburg Bridge BK | `n_483440` | 57.4 m |
| Court Sq Queens | `n_461740` | 56.9 m |
| LIC Hunters Pt | `n_434104` | 31.0 m |
| Yankee Stadium | `n_560673` | 109.3 m |
| 161 St-Yankee Stadium | `n_85847` | 85.2 m |

## Per-route results

| Route | Profile | Status | Elapsed | Edges | Length (m) |
|---|---|---|---|---|---|
| Penn Station -> Grand Central | distance | Ok | 0.11s | 28 | 1956.4 |
| Penn Station -> Grand Central | wheelchair | Ok | 0.09s | 28 | 1956.4 |
| Times Square -> Empire State Building | distance | Ok | 0.05s | 24 | 1262.0 |
| Times Square -> Empire State Building | wheelchair | Ok | 0.06s | 24 | 1262.0 |
| Union Square -> Washington Sq Park | distance | Ok | 0.04s | 13 | 1153.6 |
| Union Square -> Washington Sq Park | wheelchair | Ok | 0.04s | 13 | 1153.6 |
| Brooklyn Bridge MN -> DUMBO | distance | Ok | 0.29s | 58 | 4060.7 |
| Brooklyn Bridge MN -> DUMBO | wheelchair | Ok | 0.29s | 58 | 4060.7 |
| Atlantic Av-Barclays -> Prospect Park | distance | Ok | 0.4s | 82 | 5773.2 |
| Atlantic Av-Barclays -> Prospect Park | wheelchair | NoPath | 6.23s | — | — |
| Williamsburg Bridge MN -> Williamsburg Bridge BK | distance | Ok | 0.68s | 112 | 8509.9 |
| Williamsburg Bridge MN -> Williamsburg Bridge BK | wheelchair | Ok | 0.68s | 112 | 8509.9 |
| Court Sq Queens -> LIC Hunters Point | distance | Ok | 0.03s | 25 | 859.4 |
| Court Sq Queens -> LIC Hunters Point | wheelchair | Ok | 0.03s | 29 | 910.8 |
| Yankee Stadium -> 161 St | distance | NoPath | 6.34s | — | — |
| Yankee Stadium -> 161 St | wheelchair | NoPath | 6.28s | — | — |
| Empire State Building -> DUMBO (cross-borough) | distance | Ok | 0.69s | 105 | 7870.3 |
| Empire State Building -> DUMBO (cross-borough) | wheelchair | Ok | 0.7s | 105 | 7870.3 |
| Grand Central -> Washington Sq Park (long Manhattan) | distance | Ok | 0.22s | 37 | 3459.8 |
| Grand Central -> Washington Sq Park (long Manhattan) | wheelchair | Ok | 0.22s | 37 | 3459.8 |

## Summary

- distance profile: **9 / 10** OK
- wheelchair profile: **8 / 10** OK