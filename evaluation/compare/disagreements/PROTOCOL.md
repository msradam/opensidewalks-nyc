# Rating protocol: what is at the marked point

Note 2026-10-03. This file is the instruction the raters received, unchanged below this note. The raters were Anthropic Claude models, run as separate instances through Claude Code, each given only this protocol and its sheets. The exact model versions were not recorded. No person rated any sheet, and nothing was checked on the ground. Agreement between instances measures consistency, not accuracy. The "2018 city survey" below is the DOT ramp survey, which a contractor (Cyclomedia) collected for NYC DOT from vehicle-mounted street-level imagery and LiDAR, with records from March 2017 to January 2020, mostly 2018. Every result is reported in `../../README.md`, section 6.

You are rating aerial photographs of street corners and paths in New York City. You are not told why each place was chosen. Rate only what the photograph shows.

## What a sheet shows

Each sheet is a square 70 m across, north up, from the city's March 2024 aerial imagery at 15 cm per pixel. Drawn over it:

- An **orange line** and sometimes a **blue line**. These are two computed walking routes. Which router drew which does not matter to you.
- **White squares**: positions where a 2018 city survey recorded a curb ramp. The survey is older than the photograph.
- A **yellow ring** about 4 m across: the marked point. Every question is about the place inside and just around this ring.

A header strip gives the sheet number.

## Questions

Answer all four for every sheet.

**Q1 `place`: what is at the marked point?** One of:

- `crossing`: the mark is on, or at the end of, a place where people walk across a roadway. A painted crosswalk counts. So does an unpainted crossing at a street corner.
- `sidewalk`: the mark is on a sidewalk beside a street, away from a corner.
- `roadway`: the mark is in the carriageway, a driveway or a parking area, not at a crossing.
- `path`: the mark is on a path through a park, plaza, campus or housing estate, away from a street.
- `structure`: the mark is on a bridge, viaduct, overpass, elevated walkway, a ramp leading onto one, or a stair.
- `unclear`: the photograph does not let you say.

**Q2 `ramps`: at a crossing, are curb ramps visible?** Answer only if Q1 is `crossing`; otherwise write `not applicable`. Look at the two kerb ends of the crossing that the mark is on. A ramp shows as a break in the kerb line, often with a small rectangular warning pad (red, yellow, white or dark grey) at the edge of the roadway. One of:

- `both`: a ramp or warning pad is visible at both ends.
- `one`: visible at one end only.
- `neither`: both kerb ends are visible and neither shows a ramp or pad.
- `cannot tell`: shadow, trees, vehicles or resolution hide one or both ends.

Do not use the white squares to answer this. They are the old survey, and the question is what the photograph shows.

**Q3 `sidewalk`: does the orange line follow a sidewalk or path near the mark?** Look at the orange line within about 15 m of the mark. One of:

- `yes`: it runs along a sidewalk, a path or a marked crossing.
- `no`: it runs along the carriageway, a driveway or a parking area where no sidewalk or path lies under it.
- `cannot tell`.

**Q4 `note`:** one short sentence on anything that decided your answers or made them uncertain.

## Rules

- Judge from the photograph alone. Do not guess from the colours or shapes of the overlays what the answer "should" be.
- If you cannot see it, say `cannot tell` or `unclear`. A confident wrong answer is worse than an honest unknown.
- Rate every sheet you are given, in order, and do not go back to change earlier answers after seeing later sheets.

## Output

Write one JSON file, a list with one object per sheet:

```json
[{"sheet": 1, "place": "crossing", "ramps": "both", "sidewalk": "yes", "note": "Red pads visible at both corners."}]
```
