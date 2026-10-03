# Ramp backlog table: method

> Publication note (2026-10-03): this copy is published in `evaluation/ramp_backlog/`. Files named here under `sources/` and `raw/` are saved copies of public documents and API pages; they are not redistributed. `SOURCES.txt` in this folder gives the URL of each. The scripts named here (`fetch.py`, `build.py`, `sources/crosstab_codes.py`) are in the project's working archive and are not published.

Built 2026-10-02 from NYC DOT's public data only. The table is not published. It is a working file in this folder and nothing in it has been sent to DOT or to anyone else.

Every count below is in `backlog_summary.json`, `result.json`, `run.log`, one of the two CSVs or `sources/crosstab_codes.json`. Quotations are from files saved under `sources/`, named where they are used. `dot_code_documentation.md` has the full search for what DOT documents.

## What the table is

`backlog_by_council_district.csv` and `backlog_by_community_district.csv` count, per district, the surveyed pedestrian ramps at corners that DOT's progress data does not show as rebuilt since the ramp was surveyed. There are three counts, each with its number of distinct corners. The citywide row of both files reads:

| Count | Ramp column | Ramps | Distinct corners |
|---|---|---|---|
| Non-Compliant on `COMPLIANCY_STATUS`, the field DOT publishes | `compliancy_status_noncompliant_not_since_rebuilt` | 81,390 | 53,731 |
| Pending on `COMPLIANCY_STATUS` | `compliancy_status_pending_not_since_rebuilt` | 45,759 | 30,598 |
| Non-Compliant on `COMPLIANCY_TOTAL`, an unpublished roll-up | `compliancy_total_noncompliant_not_since_rebuilt` | 126,395 | 79,789 |

Each corner column has the ramp column's name with `_corners` added. `surveyed_ramps` is every surveyed ramp in the district, 217,679 citywide at 134,127 corners. Of those, 128,954 ramps at 80,642 corners are at corners not shown as rebuilt.

The older columns are unchanged. `backlog_ramps` is the same number as `compliancy_total_noncompliant_not_since_rebuilt`, and `noncompliant_ramps` and the `backlog_*` columns are on `COMPLIANCY_TOTAL`.

The first two counts never overlap, because a ramp has one `COMPLIANCY_STATUS` value. Their corner counts do overlap: a corner with one Non-Compliant ramp and one Pending ramp is in both. Together they are 127,149 ramps at 80,048 corners.

## Which figure to lead with

Lead with 81,390 ramps at 53,731 corners. It uses the classification DOT itself shows the public, so the claim is "DOT's map calls these ramps Non-Compliant and DOT's progress file does not show their corners as rebuilt". Its caveat is that it is the narrower count. In the data, every ramp published as Non-Compliant fails the detectable warning check, and a ramp that fails only a slope or landing check is published as Pending. That reading is an inference from the rows (see "What DOT does not document").

Give 126,395 ramps at 79,789 corners beside it. It uses `COMPLIANCY_TOTAL`, which fails a ramp if any one check fails. Its caveat is that DOT does not publish or describe this field, and that 98.4 percent of all surveyed ramps are Non-Compliant on it, so the figure is close to a count of every surveyed ramp at a corner not shown as rebuilt (128,954). Of the 126,395, DOT publishes 81,357 as Non-Compliant and 45,038 as Pending.

The 45,759 Pending ramps are ramps DOT has not classified. They are not counted as non-compliant in the lead figure and should not be added to it without saying so.

All three counts carry the survey's own caveat. The Open Data description of the survey (`sources/socrata_ufzp-rrqu_views.json`) says its "measurements shown are not indicative of whether a particular ramp is compliant with design and construction standards pursuant to the Americans with Disabilities Act (ADA)". None of these counts is a finding of ADA non-compliance.

## The two fields

Both are in DOT's ArcGIS layer `CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD`, one row per surveyed ramp.

`COMPLIANCY_STATUS` is the field DOT publishes. The site script that builds the Survey Assessment Map (`sources/nycpedramps_js_module4.js`) draws the map from this field, with the legend title "Assessment Status" and the labels "Compliant", "Pending Technical Review" (for the value `Pending`) and "Non-Compliant". The script never reads `COMPLIANCY_TOTAL`.

`COMPLIANCY_TOTAL` has no public description. Its name appears nowhere outside the layer's field list. The layer's own metadata (`sources/arcgis_compliance_layer0.json`) describes no field and no value.

| `COMPLIANCY_STATUS` | Ramps | Share of surveyed | | `COMPLIANCY_TOTAL` | Ramps | Share of surveyed |
|---|---|---|---|---|---|---|
| Non-Compliant | 145,890 | 67.0 percent | | Non-Compliant | 214,200 | 98.4 percent |
| Pending | 69,406 | 31.9 percent | | Compliant | 3,141 | |
| Compliant | 2,383 | | | Compliant < 60 | 225 | |
| | | | | TBD | 113 | |

The two fields against each other, all surveyed ramps:

| `COMPLIANCY_TOTAL` | Status Compliant | Status Non-Compliant | Status Pending |
|---|---|---|---|
| Compliant | 2,181 | 16 | 944 |
| Compliant < 60 | 202 | 21 | 2 |
| Non-Compliant | 0 | 145,853 | 68,347 |
| TBD | 0 | 0 | 113 |

The same table for ramps at corners not shown as rebuilt:

| `COMPLIANCY_TOTAL` | Status Compliant | Status Non-Compliant | Status Pending |
|---|---|---|---|
| Compliant | 1,622 | 16 | 640 |
| Compliant < 60 | 183 | 17 | 2 |
| Non-Compliant | 0 | 81,357 | 45,038 |
| TBD | 0 | 0 | 79 |

## What DOT documents

### Pending

DOT's map labels `Pending` as "Pending Technical Review". Three DOT texts define the phrase.

The survey page (`sources/nycpedramps_survey.html`, and the 2020-11-26 and 2021-01-25 captures in `sources/wayback_20201126_survey.html` and `sources/wayback_20210125_survey.html`):

> Pending Technical Review means that the survey data received for a particular ramp is being reviewed to determine its compliance with the 2010 American’s With Disabilities Act (ADA) Standards for Accessible Design.

The Open Data description of the survey (`sources/socrata_ufzp-rrqu_views.json`):

> DOT applies additional parameters in its compliance assessment of the data collected by Cyclomedia, including specific site constraints located at or near a pedestrian ramp, otherwise referred to as a technical infeasibility in the ADA. [...] As such, compliance determinations at some locations require further analysis and site inspection. These locations are noted as “Pending Technical Review” in the published assessment available at: https://www.nycpedramps.info/survey.

The 2023 Transition Plan (`sources/transition_plan_2023-07.pdf`, PDF page 9):

> DOT analyzed its entire inventory of 217,678 ramps and placed them into one of three categories: (1) ramps that are in full compliance with the ADA; (2) ramps that are not in compliance with the ADA; and (3) ramps that need further technical review to determine whether or not they are in compliance.

> The on-site analysis of these locations occurs when the street is resurfaced or another alteration triggers construction at the location.

The same pages describe the non-compliant test, without naming a field:

> When analyzing data collected in the initial survey, DOT classified ramps as non-compliant if they failed on any of the 13 elements described above, with the exception of curb reveal.

No source gives the rule that puts a ramp in `Pending` and not in `Non-Compliant`.

### Corner statuses

The Program Progress page (`sources/nycpedramps_program-progress.html`) defines the values of `Construction_Status_Value` in `e7gc-ub6z`. The dictionary attached to the dataset (`sources/socrata_e7gc-ub6z_Data_Dictionary_PedRamp_ProgramProgress_final.xlsx`) carries the same sentences.

> Corners classified as “Constructed” refer to locations that were constructed since July 2017.
> Corners classified as “Planned Construction” refer to locations that were recently resurfaced and are undergoing assessment and planning, or are part of other projects that trigger pedestrian ramp work.
> Corners classified as “Not Assigned” have not been resurfaced since July 2017.
> Corners classified as “Not Required” are locations where an upgrade or installation is not required.
> A complex corner requires the development of a unique design drawing prior to installation or upgrade of a pedestrian ramp due to existing site conditions and therefore may take longer to construct.

The dictionary's list of allowed values leaves out `Complex Planned Construction`, which 16,082 corners hold. No source says why a corner is "Not Required" or when a "Planned Construction" corner will be built.

## What DOT does not document

`TBD`, `Compliant < 60`, `Manual Check` and `Compliant-Exception` are not documented. None of these strings occurs in any page, script, metadata record, data dictionary or PDF saved under `sources/`. The "Where I looked" table in `dot_code_documentation.md` lists what was searched and what could not be reached.

The readings below are inferences from the layer's own rows. DOT states none of them. Each is a table or an assert in `sources/crosstab_codes.py`, with its output in `sources/crosstab_codes.json`.

Inference: `TBD` (113 ramps) is a ramp with no failed check and at least one `Manual Check`. A rule of "any failed check gives Non-Compliant, otherwise any Manual Check gives TBD, otherwise Compliant" reproduces `COMPLIANCY_TOTAL` on all 213,003 ramps that are not cut-throughs (210,100 Non-Compliant, 113 TBD, 2,790 Compliant). The manual check is `OBSTACLES_CHECK` on 105 of the 113 and `DWS_CHECK` on 8. All 113 are `Pending` on the published field.

Inference: `Compliant < 60` (225 ramps) is a cut-through that a consultant measured on site and that is under 59 inches wide. All 225 are cut-throughs with a consultant's curb reveal measurement. Their widths run from 32.6 to 58.9 inches, and the 351 cut-throughs marked plain `Compliant` run from 59 to 196.4 inches. Why DOT set the class at 60 inches is not checkable from the data.

Inference: `Manual Check` is an element that could not be decided from the imagery. It is a value of `OBSTACLES_CHECK` on 17,202 ramps and of `DWS_CHECK` on 144. Of the ramps that have one, 17,231 also fail another check and are Non-Compliant on `COMPLIANCY_TOTAL`.

`Compliant-Exception` is a value of `LANDING_CHECK` and `LANDING_LENGTH_CHECK` on 3,402 ramps. The data gives no reading of what the exception is.

Inference: the published field separates Non-Compliant from Pending on the detectable warning check. All 145,890 ramps published as Non-Compliant fail `DWS_CHECK`. Of the 69,406 published as Pending, 69,262 pass it and 144 have `Manual Check`. 66,588 ramps that are not cut-throughs fail some other check on `COMPLIANCY_TOTAL` and are published as Pending.

## How each code is treated in the count

| Code | Field | Ramps | Treatment | Why |
|---|---|---|---|---|
| Non-Compliant | `COMPLIANCY_STATUS` | 145,890 | Counted in the lead figure | It is DOT's published classification |
| Pending | `COMPLIANCY_STATUS` | 69,406 | Counted in its own column, never added to Non-Compliant | DOT says compliance at these ramps is not yet determined |
| Compliant | `COMPLIANCY_STATUS` | 2,383 | Not counted | DOT publishes them as compliant |
| Non-Compliant | `COMPLIANCY_TOTAL` | 214,200 | Counted in the second figure | It is the layer's own roll-up of the checks |
| Compliant | `COMPLIANCY_TOTAL` | 3,141 | Not counted in the second figure | No check failed |
| Compliant < 60 | `COMPLIANCY_TOTAL` | 225 | Not counted in the second figure, as compliant | The label says Compliant, and the data reads as a passed cut-through (inference) |
| TBD | `COMPLIANCY_TOTAL` | 113 | Not counted in the second figure | No check failed (inference). All 113 are Pending on the published field, so they are in the Pending column |
| Manual Check | `OBSTACLES_CHECK`, `DWS_CHECK` | 17,202 and 144 | Not used directly | It is a per-element value. It reaches the counts only through the two fields above |
| Compliant-Exception | `LANDING_CHECK`, `LANDING_LENGTH_CHECK` | 3,402 | Not used directly | Same reason |

Each figure follows its own field and nothing else. So the 16 `Compliant` and 17 `Compliant < 60` ramps at corners not shown as rebuilt that DOT publishes as Non-Compliant are in the lead figure and not in the second one.

No slope threshold of this project's is applied anywhere. The running, cross and counter slope columns in the CSVs use DOT's `RAMP_RUNNING_SLOPE_CHECK`, `RAMP_CROSS_SLOPE_CHECK` and `COUNTER_SLOPE_CHECK` as they stand.

## Rebuilt since the survey

A ramp's corner is rebuilt since the survey when its `Construction_Status_Value` is `Constructed` or `Complex Constructed` and its `Construction_End_Date` is later than the ramp's survey date (`GEOCYCLORAMA_DATE`, taken as a UTC calendar day). Every other ramp is at a corner not shown as rebuilt. Status alone decides for the four unbuilt statuses, even where a date is present.

The date comparison matters for the ramps at built corners whose end date is on or before the survey date: 807 published Non-Compliant, 2,215 Pending and 2,979 Non-Compliant on `COMPLIANCY_TOTAL`, 13 of the last group on the same day. Ignoring dates and using status alone gives 123,416 on `COMPLIANCY_TOTAL`.

## How per-corner progress limits a per-ramp count

DOT records progress per corner. The survey is per ramp. The Program Progress page says "A corner can have one or more ramps." In the survey, 51,123 corners have 1 ramp, 82,475 have 2, 510 have 3 and 19 have 4.

DOT's progress reports changed unit. The reports issued through February 12, 2021 list "Total number of pedestrian ramps installed." From the reports issued August 13, 2021 the same line reads "Total number of corners installed." (`sources/text_extracts/dot_Ped_Ramp_Semi_Annual_Report_FY21_Pt1.txt` and `dot_Ped_Ramp_Semi_Annual_Report_FY21_Pt2.txt`). No report explains the change. The FY 2026 annual report (`sources/dot_Ped_Ramp_Annual_Report_FY26.pdf`) says "Corners Constructed tabulation is based on the most recent construction activity at each corner."

DOT does hold a ramp count per constructed corner. The service behind its progress dashboard has an integer field `Ramps_Constructed` (`sources/dotweb01_Corner_Construction_Report_layers.json`). That field is not in the public file `e7gc-ub6z`.

This limits the count in three ways.

A "Constructed" corner does not say which of its ramps were rebuilt. So every ramp at a corner rebuilt after the survey is excluded: 88,725 surveyed ramps at 53,513 corners, of which 64,500 are published Non-Compliant, 23,647 are Pending and 87,805 are Non-Compliant on `COMPLIANCY_TOTAL`. Of those 88,725 ramps, 70,315 are at the 35,103 corners with more than one surveyed ramp. Where only some ramps at such a corner were rebuilt, the others are missing from the count, and the count understates the backlog. The public file cannot show how often that happens.

The count can also overstate the backlog. A corner that was rebuilt but is not yet "Constructed" in the public file keeps its ramps in the count. The progress file here was retrieved on 2026-10-02. It holds corners with an unbuilt status and an end date all the same (438 Planned Construction, 287 Not Assigned, 78 Complex Planned Construction, 65 Not Required). If an end date after the survey overrode the status, the `COMPLIANCY_TOTAL` figure falls to 126,275.

"Not Required" has no stated reason. DOT's sentence is "locations where an upgrade or installation is not required", and nothing says whether the corner is already compliant, has no crossing or is outside DOT's work. The 2,221 surveyed ramps at such corners stay in the count because the data does not say they were rebuilt.

Surveyed ramps by the status of their corner in `e7gc-ub6z`. "Counted" is the ramps at corners not shown as rebuilt.

| Corner status | Corners in the progress file | Surveyed ramps | Published Non-Compliant | Counted | Pending | Counted | `COMPLIANCY_TOTAL` Non-Compliant | Counted |
|---|---|---|---|---|---|---|---|---|
| Planned Construction | 61,116 | 78,929 | 49,679 | 49,679 | 28,257 | 28,257 | 77,512 | 77,512 |
| Constructed | 54,820 | 85,846 | 61,557 | 650 | 23,607 | 1,803 | 84,808 | 2,411 |
| Not Assigned | 25,933 | 24,019 | 15,158 | 15,158 | 8,590 | 8,590 | 23,568 | 23,568 |
| Not Required | 25,207 | 2,221 | 1,712 | 1,712 | 470 | 470 | 2,174 | 2,174 |
| Complex Planned Construction | 16,082 | 20,550 | 14,027 | 14,027 | 6,211 | 6,211 | 20,139 | 20,139 |
| Complex Constructed | 4,538 | 6,091 | 3,750 | 157 | 2,255 | 412 | 5,976 | 568 |
| No progress record | 0 | 23 | 7 | 7 | 16 | 16 | 23 | 23 |

The rule is applied per ramp against that ramp's own survey date. At 28 corners it splits the corner, because the corner's ramps were surveyed on different days.

Nothing in either source says whether a rebuilt ramp now passes. Corners with no ramp at survey time are absent from the survey, so missing ramps are not counted. Conditions may have changed since the survey in ways neither dataset records.

A corner whose ramps fall in two districts is counted in each district's corner column. So the district corner columns sum to more than the citywide row: by 55, 30 and 96 corners across council districts and by 41, 26 and 79 across community districts, in the order of the first table.

## Sources and dates

| Source | Date of the data | Retrieved (UTC) | Use |
|---|---|---|---|
| DOT ArcGIS layer `CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD`, 217,679 rows | Last edited 2020-12-28. Survey dates run from 2017-03-27 to 2020-01-29, with 216,220 ramps surveyed in 2018 | 2026-10-02T16:55:09Z to 2026-10-02T16:58:24Z | One row per ramp: both compliance fields, the check fields, survey date, corner ID, location |
| DOT Pedestrian Ramp Program Progress, Socrata `e7gc-ub6z`, 187,696 corners | Construction "since July 2017". Rows last updated 2026-10-01 (`sources/socrata_e7gc-ub6z_views.json`) | 2026-10-02T16:06:50Z | Corner status and construction end date |
| DOT ramp survey, Socrata `ufzp-rrqu` | Same survey as the layer. Rows last updated 2021-10-27 (`sources/socrata_ufzp-rrqu_views.json`) | 2026-10-02T15:04:55Z | Cross-check only: survey date and corner ID agree with the layer on all 217,679 ramp IDs |
| DCP City Council Districts `872g-cjhh` and Community Districts `5crt-au7u` | Rows last updated 2026-05-26 (the two `.meta.json` files under `raw/`) | 2026-10-02T16:58:31Z and 2026-10-02T16:58:39Z | District polygons |
| DOT pages, scripts, metadata, dictionaries and PDFs under `sources/` | Stated in each file name or in the text quoted | 2026-10-02, logged in `dot_code_documentation.md` | What DOT documents about the codes |

## Join

The join key is the corner ID. The layer holds it as an integer and the progress file as a seven digit string with no whitespace, so both were cast to the same decimal string. Of 217,679 ramps, 217,656 match a progress corner and 23 do not. That is 134,108 of 134,127 distinct corners. In the other direction, 134,108 of 187,696 progress corners have a surveyed ramp. Those without one are mostly `Not Required` (23,474), `Planned Construction` (12,882) and `Not Assigned` (10,304). Neither side has a duplicate key.

## Malformed dates

Each `Construction_End_Date` is parsed strictly as year/month/day. Three values fail: `3505/20/72` and `3668/40/72` are not calendar dates, and `0205/05/29` has a year before 1990. They are treated as unusable. All three are on corners with an unbuilt status, so they change no count. A further 139 distinct values on 315 corners are real dates earlier than the first survey day, 2017-03-27. They are kept as usable dates, and all of them are also on unbuilt corners. `malformed_dates.csv` lists every such value with its class, status and ramp counts.

## Districts

Neither DOT source has a council or community district field, so each ramp is assigned by point in polygon on its own location from the layer. 86 ramps fall outside every polygon, at most 116.4 feet out, and take the nearest district. There are 51 council districts and 71 community district polygons. Council districts that cross a borough line list DOT's borough values with ramp counts. Community district rows include the joint interest areas (parks and airports), flagged in `district_type`. For 74 ramps DOT's borough differs from the borough of the community district polygon.

## Reproduce

```
.venv/bin/python research_notes/ramp_backlog/fetch.py
.venv/bin/python research_notes/ramp_backlog/build.py
.venv/bin/python research_notes/ramp_backlog/sources/crosstab_codes.py
```

`fetch.py` downloads anything missing under `raw/` and never repeats a saved page. `build.py` reads only cached files and writes the two CSVs, `backlog_summary.json`, `result.json` and `malformed_dates.csv`. `crosstab_codes.py` reads `raw/compliance/` and rewrites `sources/crosstab_codes.json`. `run.log` holds the first run of `fetch.py` and `build.py` and, below it, the rerun of `build.py` that added the published-field columns. `rederive/` is an independent second derivation of the `COMPLIANCY_TOTAL` figure (126,395 ramps at 79,789 corners).
