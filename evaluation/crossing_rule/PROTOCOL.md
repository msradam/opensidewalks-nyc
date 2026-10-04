# Rating protocol: does each end of a crossing have a ramp that serves it?

This protocol is written so that a second rater can repeat the rating without talking to the first.

Note 2026-10-03. This file is the instruction the raters received. The raters were Anthropic Claude models, run as separate instances through Claude Code, each given only this protocol and its sheets. The exact model versions were not recorded. No person rated any sheet, and nothing was checked on the ground. On 2026-10-03 four things were changed in the last two sections, which tell the reader how the ratings are used: "ramped on the ground" became "rated ramped", "DOT's field survey" became a description of how the survey was collected, "the survey is from 2018" became "mostly from 2018", and a line on rater agreement was added. Nothing a rater was asked to do changed. The rest of the text is as the raters received it, so where it says 2018 is the year of the ramp survey, read it as the year most of the survey's records come from.

## What is being rated

The sample is 200 pedestrian crossings from the v0.3.2 graph, 40 drawn at random from each borough (`sample/sample.csv`, drawn by `sample.py`, seed 20261002). Each crossing has two ends, A and B. A is the western end.

Each crossing has one sheet, `sample/sheets/NNN.jpg`, with two panels of the same view, north up:

- Left panel: NYC orthoimagery flown in 2018, the year of the DOT ramp survey, about 15 cm per pixel. The two ends of the crossing, as the graph has them, are cyan rings 1.2 m in radius labelled A and B. A thin yellow line covers the middle third of the crossing so you can tell which crossing is meant.
- Right panel: the same view with the positions of DOT's surveyed ramps drawn as magenta squares (about 0.9 m across). These are the positions the survey recorded, not positions in the graph.

No sheet shows what any matching rule decided.

## What to decide, for each end

Look at where people walking this crossing step off the sidewalk at this end. On a marked crossing that is where the painted crosswalk meets the kerb. On an unmarked crossing it is where the straight continuation of the yellow line meets the kerb. The cyan ring is only where the graph puts the end, and it can be a few metres off. Judge by the crosswalk and the kerb, not by the ring.

Then answer three questions.

**1. `ramp_serves` (yes, no, unclear).** Is there a surveyed ramp (magenta square) that serves this crossing at this end?

- `yes`: a magenta square sits on the kerb line inside the width of the crosswalk, or within about 1.5 m of its edge. A square at the apex of the corner also counts if a person coming off it would enter this crosswalk (a diagonal corner ramp serving both crossings).
- `no`: there is no magenta square at this end, or the nearest squares are plainly placed for another crossing: round the corner on the other street's kerb, or along the same kerb but more than about 1.5 m outside the crosswalk.
- `unclear`: you cannot tell. For example the kerb is hidden by a tree or a shadow, the crossing is unmarked and it is not clear where it meets the kerb, or the end is in the middle of a wide paved area with no kerb to judge.

Do not use distance from the cyan ring to decide. A square 6 m from the ring can serve the crossing (the ring is misplaced) and a square 2 m from the ring can serve the other crossing at that corner.

**2. `seen_in_imagery` (ramp, kerb, cannot_tell).** Ignoring the magenta squares, what does the imagery itself show at the place where this crossing meets the kerb?

- `ramp`: a ramp is visible: a coloured or textured warning pad, or a clear flared cut in the kerb.
- `kerb`: an unbroken kerb is clearly visible there.
- `cannot_tell`: neither can be made out. At 15 cm per pixel this is the usual answer. Do not guess.

**3. `end_type` (corner, median, midblock, other).** Where is this end: at a street corner, on a median or traffic island, at a mid-block crossing, or somewhere else (a driveway, a park path, a parking lot).

Also record once per crossing: `marked` (yes, no): is a painted crosswalk visible for this crossing.

Add a short `note` when something is unusual (crossing not visible at all, construction, the sheet shows a place that is not a street crossing).

## How to record

One row per end, in a CSV with exactly these columns:

```
n,end,ramp_serves,seen_in_imagery,end_type,marked,note
```

`n` is the sheet number (1 to 200), `end` is A or B. Rate both ends of every sheet you are given. Rate each sheet on its own. Do not look at any other rater's file or at the rule outputs.

## What the rating is used for

A crossing counts as rated ramped when both ends are `yes`. This is what the 2018 imagery and the survey positions show to the rater, not a check on the ground. Each matching rule (ramp on the crossing's own node, ramp within 5 m of each end, and others) is scored against that: precision is the share of crossings a rule calls ramped that are rated ramped, and recall is the share of rated ramped crossings the rule finds. Ends rated `unclear` are left out of the scoring and counted.

## Limits to state with any result

- The survey is mostly from 2018 and the imagery is from 2018. The rating says whether a surveyed ramp was positioned to serve the crossing then. It does not say the ramp is there or usable today.
- The imagery rarely shows a ramp directly. The existence of each ramp rests on the DOT ramp survey, which a contractor (Cyclomedia) collected from vehicle-mounted street-level imagery and LiDAR, with records from March 2017 to January 2020, mostly 2018. The rating tests which crossing a ramp belongs to.
- Agreement between raters is agreement between instances of one language model. It measures consistency, not accuracy.
- A ramp that serves a crossing may still be too steep or lack a warning surface. That is a separate question (the survey's slope fields).
