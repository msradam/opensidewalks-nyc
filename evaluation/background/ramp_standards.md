Curb ramp running slope is limited to 1:12 (8.33 percent), not 5 percent; the 5 percent figure belongs to walkways and blended transitions.

# Curb ramp standards and what the NYC DOT survey fields measure

Retrieved 2026-10-02. Saved copies of the sources are kept in the project's working archive, which is not published; each source is named in the text.

## Federal limits, quoted

2010 ADA Standards for Accessible Design, as published by the US Access Board at https://www.access-board.gov/ada/ (saved as `ada_standards.html`):

| Section | Text | Percent |
|---|---|---|
| 406.1 | "Curb ramps on accessible routes shall comply with 406, 405.2 through 405.5, and 405.10." | |
| 405.2 | "Ramp runs shall have a running slope not steeper than 1:12." Exception for existing sites, Table 405.2: steeper than 1:12 but not steeper than 1:10 for a maximum rise of 6 inches; steeper than 1:10 but not steeper than 1:8 for a maximum rise of 3 inches. | 8.33 (10.0 and 12.5 under the exception) |
| 405.3 | "Cross slope of ramp runs shall not be steeper than 1:48." | 2.08 |
| 406.2 | "Counter slopes of adjoining gutters and road surfaces immediately adjacent to the curb ramp shall not be steeper than 1:20." | 5.0 |
| 406.3 | "Where provided, curb ramp flares shall not be steeper than 1:10." | 10.0 |
| 403.3 | "The running slope of walking surfaces shall not be steeper than 1:20." | 5.0 |
| 104.1.1 | "All dimensions are subject to conventional industry tolerances except where the requirement is stated as a range with specific minimum and maximum end points." | |

PROWAG (Accessibility Guidelines for Pedestrian Facilities in the Public Right-of-Way, 36 CFR Part 1190, final rule published 8 August 2023), from https://www.access-board.gov/prowag/complete.html (saved as `prowag_complete.html`):

| Section | Text |
|---|---|
| R304.2.1 (perpendicular ramps) | "The running slope of the curb ramp shall be 1:12 (8.3%) maximum. EXCEPTION: Where the curb ramp length must exceed 15 feet (4.6 m) to achieve a 1:12 (8.3%) running slope, the curb ramp length shall extend at least 15 feet (4.6 m) and may have a running slope greater than 1:12 (8.3%)." |
| R304.3.1 (parallel ramps) | Same 1:12 (8.3%) maximum and the same 15 foot exception. |
| R304.2.2, R304.3.2 | "The cross slope of a curb ramp run shall be 1:48 (2.1%) maximum." Exception at crosswalks: may equal the crosswalk cross slope under R302.5. |
| R304.4.1 | "The running slope of blended transitions shall be 1:20 (5.0%) maximum." |
| R302.4.1 | Pedestrian access route grade within a highway right-of-way "shall not exceed 1:20 (5.0%)", or the street grade where that is steeper. |
| R304.5.2 | Change of grade at the gutter "shall not exceed 13.3 percent", or a transitional space is provided. PROWAG has no separate 1:20 counter slope rule. |

Adoption: the Access Board page lists USDOT adopting PROWAG for transit stops on 18 December 2024 and GSA on 3 July 2024. Whether DOJ has adopted PROWAG for state and local government facilities as of October 2026 was not verified.

So 5 percent is the limit for a walking surface (403.3), a counter slope (406.2), a blended transition (R304.4.1) and a pedestrian access route grade (R302.4.1). It is not the limit for a curb ramp run under either standard.

## NYC DOT survey fields (`ufzp-rrqu`)

From the attached data dictionary, `Data_Dictionary_Pedestrian_Ramp_Locations_2021.xlsx`:

| Field | Meaning | Unit and sign |
|---|---|---|
| RAMP_RUNNING_SLOPE_TOTAL | "Longitudinal slope of ramp's entire extent" | percent, one decimal; "Slope direction from the road to landing" |
| RAMP_CROSS_SLOPE | "Cross slope of ramp" | percent, one decimal; signed "from left to right when facing ramp from the road" |
| COUNTER_SLOPE | "Slope of adjacent street at ramp interface (roadway grade)" | percent; signed road to landing |
| GUTTER_SLOPE, LND_CROSS_SLOPE, flares | slopes | percent, signed left to right |
| CURB_REVEAL, LND_WIDTH, LND_LENGTH, RAMP_WIDTH, RAMP_LENGTH | dimensions | inches |
| DWS_CONDITIONS | detectable warning surface condition | Defective, Good Condition, Missing, Off Ramp (Good or Defective), Not Applicable |
| GeoCyclora | capture date | 2017-03-27 to 2020-01-29; 216,220 of 217,679 rows in 2018 |

Because the cross slope is signed by tilt direction, a test of `value <= 2` passes every negative value. Magnitudes must be compared.

The dictionary does not define the codes 555, 777, 888 and 999. Counts in RAMP_RUNNING_SLOPE_TOTAL: 999 on 4,592 rows (mostly ramp type "Cut-Through", which has no ramp run), 888 on 792, 777 on 95 (`q_sentinel_counts.json`, `arcgis_sentinel_probe.jsonl`). 555 appears in DOT's ArcGIS layer only.

The published CSV appears to have RAMP_LEFT_FLARE and RAMP_LENGTH transposed: the column named RAMP_LEFT_FLARE has a median of 56.1 (plausible as inches of length) and RAMP_LENGTH has a median of -11.3 (plausible as a flare slope). The pipeline uses neither.

The dataset is "Historical" (update frequency in the dictionary); Socrata `rowsUpdatedAt` is 2021-10-27. It is a frozen 2017 to 2020 survey. Ramps rebuilt since are tracked by corner in `e7gc-ub6z` (Pedestrian Ramp Program Progress, rows updated 2026-10-01), which has construction status but no slopes.

The dataset description says: "measurements shown are not indicative of whether a particular ramp is compliant with design and construction standards pursuant to the Americans with Disabilities Act (ADA). DOT applies additional parameters in its compliance assessment".

## DOT's own compliance assessment

NYC DOT hosts a public ArcGIS feature service, `CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD` ("Cyclomedia survey with compliancy calculated, adjusted with tolerance", https://services.arcgis.com/wmZOI9vyUBq1zTZx/arcgis/rest/services/CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD/FeatureServer, data last edited 2020-12-28), with one row per ramp and a check field per attribute. Counts queried 2026-10-02 (`arcgis_groupby_*.json`):

| Check | Compliant | Non-Compliant | Compliant share of 217,679 | Observed rule (`arcgis_threshold_probe.jsonl`) |
|---|---|---|---|---|
| RAMP_RUNNING_SLOPE_CHECK | 173,631 | 44,048 | 79.8% | magnitude up to 9.3% passes; up to 12.8% passes where the rise is small |
| RAMP_CROSS_SLOPE_CHECK | 174,042 | 43,637 | 80.0% | magnitude up to 3.0% |
| COUNTER_SLOPE_CHECK | 166,324 | 51,355 | 76.4% | magnitude up to 6.0% |
| COMPLIANCY_TOTAL (all checks) | 3,366 | 214,200 (113 TBD) | 1.5% | every attribute must pass, including landing, flares, DWS, ponding |

The thresholds in the last column are inferred from the minimum and maximum values in each class, not from a DOT document. They are each one percentage point above the federal figure, which looks like a measurement tolerance.

## Recomputed shares

Raw survey CSV (217,679 rows), sentinels excluded, magnitudes compared (`evidence/ramps/recompute_raw.json`, `recompute_release.json`):

| Test | As published in QUALITY_REPORT | Recomputed, release curb nodes | DOT's own check |
|---|---|---|---|
| Running slope | 33.0% "compliant" at <= 5% | 73.8% within 1:12 (144,175 of 195,240) | 79.8% compliant |
| Cross slope | 82.6% at <= 2% (signed) | 66.4% within 1:48 (129,559 of 195,161) | 80.0% compliant |
| Counter slope | not reported | 68.0% within 1:20 (132,671 of 195,153) | 76.4% compliant |

The published statement that two-thirds of NYC's surveyed ramps exceed the ADA running slope limit is wrong. About one quarter exceed 1:12 on the raw measurement, and about one fifth fail DOT's own running slope check. The published cross slope share is also wrong, in the other direction.

What does hold: by DOT's all-attribute test, almost no surveyed ramp (1.5 percent) was fully compliant as of the 2018 survey. That is DOT's finding, not this project's.
