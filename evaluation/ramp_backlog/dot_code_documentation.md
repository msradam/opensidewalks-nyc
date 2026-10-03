# What DOT documents about the ramp compliance codes

> Publication note (2026-10-03): this copy is published in `evaluation/ramp_backlog/`. Files named here under `sources/` and `raw/` are saved copies of public documents and API pages; they are not redistributed. `SOURCES.txt` in this folder gives the URL of each. The scripts named here (`fetch.py`, `build.py`, `sources/crosstab_codes.py`) are in the project's working archive and are not published.

Researched 2026-10-02. Every source below was retrieved on 2026-10-02 (21:21 to 21:30 UTC) and is saved under `sources/`. The layer rows are the cache that `fetch.py` wrote to `raw/compliance/` on 2026-10-02 16:55 to 16:58 UTC.

## Answer in one table

| Code | Field | Ramps | Documented by DOT? |
|---|---|---|---|
| `Pending` | `COMPLIANCY_STATUS` | 69,406 | Yes. DOT's own map labels it "Pending Technical Review" and three DOT texts define that phrase. |
| `TBD` | `COMPLIANCY_TOTAL` | 113 | No. The string appears in no DOT document, page, script or metadata record that I could reach. |
| `Compliant < 60` | `COMPLIANCY_TOTAL` and `CUT_THROUGH_CHECK` | 225 | No. Same result. The data itself shows it is a cut-through under 59 inches wide (inference, section C). |
| `Manual Check` | `OBSTACLES_CHECK`, `DWS_CHECK` | 17,202 and 144 | No. |
| `Compliant-Exception` | `LANDING_CHECK`, `LANDING_LENGTH_CHECK` | 3,402 | No. |
| Corner statuses | `Construction_Status_Value` in `e7gc-ub6z` | 187,696 corners | Yes, on the Program Progress page and in the Open Data dictionary. The dictionary's allowed-value list omits `Complex Planned Construction`. |

One finding changes how `METHOD.md` should describe the two fields. `METHOD.md` calls `COMPLIANCY_STATUS` "undocumented and narrower". It is the other way round: `COMPLIANCY_STATUS` is the field DOT publishes on its Survey Assessment Map and its three values match the three categories in the 2023 Transition Plan. `COMPLIANCY_TOTAL` is the field with no public description. It is not drawn on DOT's map and its name appears nowhere outside the layer schema.

## Counts in the layer

217,679 rows, counted from `raw/compliance/page_*.json` by `sources/crosstab_codes.py` (output in `sources/crosstab_codes.json`).

| `COMPLIANCY_TOTAL` | Ramps | | `COMPLIANCY_STATUS` | Ramps |
|---|---|---|---|---|
| Non-Compliant | 214,200 | | Non-Compliant | 145,890 |
| Compliant | 3,141 | | Pending | 69,406 |
| Compliant < 60 | 225 | | Compliant | 2,383 |
| TBD | 113 | | | |

The two fields against each other:

| `COMPLIANCY_TOTAL` | Status Compliant | Status Non-Compliant | Status Pending |
|---|---|---|---|
| Compliant | 2,181 | 16 | 944 |
| Compliant < 60 | 202 | 21 | 2 |
| Non-Compliant | 0 | 145,853 | 68,347 |
| TBD | 0 | 0 | 113 |

The 19 per-element check fields hold these values:

| Field | Compliant | Non-Compliant | Other values |
|---|---|---|---|
| `RUNNING_COUNTER_SLOPE_CHECK` | 180,812 | 36,867 | |
| `CURB_DEFECTIVE_CHECK` | 144,415 | 73,264 | |
| `SIDEWALK_DEFECTIVE_CHECK` | 169,525 | 48,154 | |
| `PONDING_CHECK` | 172,538 | 45,141 | |
| `CROSSWALK_LOCATION_CHECK` | 216,095 | 1,584 | |
| `GUTTER_SLOPE_CHECK` | 173,472 | 44,207 | |
| `FLARE_SLOPE_CHECK` | 68,660 | 149,019 | |
| `RAMP_CROSS_SLOPE_CHECK` | 174,042 | 43,637 | |
| `RAMP_WIDTH_CHECK` | 211,324 | 6,355 | |
| `OBSTACLES_CHECK` | 200,459 | 18 | Manual Check 17,202 |
| `DWS_CHECK` | 71,121 | 146,414 | Manual Check 144 |
| `RAMP_RUNNING_SLOPE_CHECK` | 173,631 | 44,048 | |
| `APEX_TURNING_SPACE_CHECK` | 1,332 | 14,222 | Not Applicable 202,125 |
| `CUT_THROUGH_CHECK` | 351 | 4,100 | Not Applicable 213,003, Compliant < 60 225 |
| `LANDING_LENGTH_CHECK` | 192,800 | 21,477 | Compliant-Exception 3,402 |
| `LANDING_WIDTH_CHECK` | 133,747 | 73,279 | Not Applicable 10,653 |
| `LANDING_SLOPE_CHECK` | 47,044 | 170,635 | |
| `LANDING_CHECK` | 30,425 | 183,852 | Compliant-Exception 3,402 |
| `COUNTER_SLOPE_CHECK` | 166,324 | 51,355 | |

## A. What DOT documents

### The layer's own metadata documents nothing about values

`sources/arcgis_compliance_layer0.json` (https://services.arcgis.com/wmZOI9vyUBq1zTZx/arcgis/rest/services/CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD/FeatureServer/0?f=json) has 66 fields. Every alias equals the field name. No field has a coded-value domain or a description. The layer has no subtypes and a simple renderer. Its description is the layer name. `lastEditDate` is 2020-12-28T21:53:12Z. The service has one layer and no tables.

The ArcGIS item record (`sources/arcgis_item_996cb79f.json`, https://www.arcgis.com/sharing/rest/content/items/996cb79f2b964b269577e2db17008651?f=json) has the only prose DOT attached to the layer. The summary reads:

> Cyclomedia survey with compliancy calculated, adjusted with tolerance.

The description reads:

> This is the source for "Survey Map" and "Survey Assessment Map" in https://www.nycpedramps.info/survey

The item's formal metadata (`sources/arcgis_item_996cb79f_metadata.xml`) repeats those two sentences and adds nothing. The item's saved popup configuration (`sources/arcgis_item_996cb79f_data.json`) labels every field with its own name. No source says what the tolerance is. Section C shows two places where the data implies one inch.

The sibling services owned by the same DOT accounts are `RAMPS_SURVEY_2019`, `CORNERS_SURVEY_2019`, `PRP_Corner_Activity`, `PRP_Corner_Activity_View` and `PRP_Corner_Activity_Test`. Their metadata is saved as `sources/arcgis_*_layers_all.json`. None has a compliance field. The only coded-value domain among them is the borough code on `RAMPS_SURVEY_2019` (1 Manhattan, 2 Bronx, 3 Brooklyn, 4 Queens, 5 Staten Island).

### `Pending` is documented as "Pending Technical Review"

The link from the code to the phrase is in the site's own map script. `sources/nycpedramps_js_module4.js` (https://www.nycpedramps.info/themes/DOT/js/module4.js) builds the Survey Assessment Map from this layer with a renderer on `COMPLIANCY_STATUS`:

```
legendOptions:{title:"Assessment Status"},field:"COMPLIANCY_STATUS",uniqueValueInfos:[{value:"Compliant",...,label:"Compliant"},{value:"Pending",...,label:"Pending Technical Review"},{value:"Non-Compliant",...,label:"Non-Compliant"}]
```

The script never reads `COMPLIANCY_TOTAL`.

DOT defines the phrase in three places, and the definitions are not identical.

The survey page (`sources/nycpedramps_survey.html`, https://www.nycpedramps.info/survey) says:

> Pending Technical Review means that the survey data received for a particular ramp is being reviewed to determine its compliance with the 2010 American’s With Disabilities Act (ADA) Standards for Accessible Design.

The same sentence is on the Wayback captures of 2020-11-26 and 2021-01-25 (`sources/wayback_20201126_survey.html`, `sources/wayback_20210125_survey.html`), so it predates and survives the layer's last edit.

The Open Data description of `ufzp-rrqu` (`sources/socrata_ufzp-rrqu_views.json`, https://data.cityofnewyork.us/api/views/ufzp-rrqu.json, repeated in the attached dictionary) says:

> DOT applies additional parameters in its compliance assessment of the data collected by Cyclomedia, including specific site constraints located at or near a pedestrian ramp, otherwise referred to as a technical infeasibility in the ADA. The constraints that constitute a technical infeasibility can include but are not limited to elements such as underground vaults, transit facilities, steep terrain conditions, and limited public right-of-way, which are not readily apparent through the data and imagery collected. As such, compliance determinations at some locations require further analysis and site inspection. These locations are noted as “Pending Technical Review” in the published assessment available at: https://www.nycpedramps.info/survey.

The 2023 Transition Plan (`sources/transition_plan_2023-07.pdf`, https://www.nycpedramps.info/sites/default/files/2023-07/transition-plan-pedestrian-ramps-2023.pdf, pages 9 and 10) says:

> DOT analyzed its entire inventory of 217,678 ramps and placed them into one of three categories: (1) ramps that are in full compliance with the ADA; (2) ramps that are not in compliance with the ADA; and (3) ramps that need further technical review to determine whether or not they are in compliance.
>
> Regarding category three, while these ramps were preliminarily analyzed, they require further analysis. The pending assessments may be due to existing site constraints where the current ramp exists and therefore require determinations as to technical infeasibility, as defined in the ADA.

and, on when the review happens:

> The on-site analysis of these locations occurs when the street is resurfaced or another alteration triggers construction at the location. At that time individual ramps may be deemed compliant to the maximum extent feasible, may be categorized for construction through DOT in-house crews, or may be categorized as a complex corner

So `Pending` is documented as a ramp DOT has not classified, awaiting a site review that happens when construction is triggered. No source gives the rule that puts a ramp in `Pending` instead of `Non-Compliant`. Section C shows what the data implies.

The same Transition Plan page describes the non-compliant test:

> When analyzing data collected in the initial survey, DOT classified ramps as non-compliant if they failed on any of the 13 elements described above, with the exception of curb reveal. For those ramps that were partially surveyed due to blocked imagery or construction at the location during the time of the survey, DOT utilized a strict criteria such that if the ramp width requirement was not met or if there was no detectable warning surface present, the ramp was categorized as non-compliant and no additional survey was performed.

The plan does not name a field, so it does not say whether this sentence describes `COMPLIANCY_TOTAL` or `COMPLIANCY_STATUS`.

### `TBD` is not documented

No source defines it. The whole-word string `TBD` occurs in none of the saved pages, scripts, metadata records, dictionaries or PDF text extracts. It exists only as a value in the layer's rows.

### `Compliant < 60` is not documented

No source defines it. The strings `Compliant < 60` and `< 60` occur in none of the saved sources. The 2021 Transition Plan appendix (`sources/transition_plan_appendix_A_2021-12.pdf`, page 9) has a cut-through width criterion, but it is a prioritisation score with a 36 inch threshold, not a compliance class:

> 5. CUT-THROUGH RAMP WIDTH ... ≥ 36" 0% ... < 36" 100%

DOT's current design standard H-1011 (`sources/ddc_SB22-004_NYCDOT_Standard_Details_2022-06-06.pdf`, PDF page 14) gives new cut-throughs a width of 8 to 10 feet ("CUT THROUGH WIDTH VARIES 8'-0" TO 10'-0", SEE NOTE 11"), so 60 inches is not DOT's construction width either.

### `Manual Check` and `Compliant-Exception` are not documented

Neither string occurs in any saved source.

### The settlement and the monitor

The 2019 settlement (`sources/settlement_agreement_2019-07-23.pdf`, https://www.nycpedramps.info/sites/default/files/2019-07/Pedestrian%20Ramp%20Settlement%20Agreement--Final%20Approved%207-23-2019.pdf, 84 scanned pages searched through `pdftotext` output) lists the 13 surveyed elements in section 9.1 and requires in section 9.4 that the website post "the City's assessment of whether those pedestrian ramps are compliant with the Accessibility Laws". It defines no assessment categories and no data codes.

Section 22.4.1 says where monitor reports go:

> Within 30 days of the annual review, the Monitor will report the assessment of compliance to Defendants' Counsel, Class Counsel and the Court.

I found no monitor report. The CourtListener copies of both dockets (`sources/courtlistener_docket_6785197_epva.html`, `sources/courtlistener_docket_6792663_cidny.html`) show the monitor's appointment (Harold Fink, P.E., order of 2020-07-05, order of appointment 2022-02-03). Entries 257 to 271 on the EPVA docket, which run from that order to 2026-06-29, are appearances, withdrawals, a dismissal and a fee stipulation. None is a monitor report. The Disability Rights Advocates case page (`sources/dra_case_page_cidny_v_city.html`) links the complaint and settlement papers only. Monitor reports may exist off the public docket. I could not check PACER.

## B. Corner status values in `e7gc-ub6z`

Counts from the cached file `research_notes/evidence/raw/e7gc-ub6z_rows.csv` (retrieved 2026-10-02T16:06:50Z): Planned Construction 61,116, Constructed 54,820, Not Assigned 25,933, Not Required 25,207, Complex Planned Construction 16,082, Complex Constructed 4,538.

The Program Progress page (`sources/nycpedramps_program-progress.html`, https://www.nycpedramps.info/program-progress) defines them:

> Corners classified as “Constructed” refer to locations that were constructed since July 2017.
> Corners classified as “Planned Construction” refer to locations that were recently resurfaced and are undergoing assessment and planning, or are part of other projects that trigger pedestrian ramp work.
> Corners classified as “Not Assigned” have not been resurfaced since July 2017.
> Corners classified as “Not Required” are locations where an upgrade or installation is not required.
> A complex corner requires the development of a unique design drawing prior to installation or upgrade of a pedestrian ramp due to existing site conditions and therefore may take longer to construct.

The Open Data dictionary attached to `e7gc-ub6z` (`sources/socrata_e7gc-ub6z_Data_Dictionary_PedRamp_ProgramProgress_final.xlsx`, listed in https://data.cityofnewyork.us/api/views/e7gc-ub6z.json) carries the same five sentences in the notes for `Construction_Status_Value`. Its allowed-value list is "Constructed / Planned Construction / Not Assigned / Not Required / Complex Constructed". `Complex Planned Construction` is missing from that list although 16,082 corners hold it. The Socrata column description is only "The status for the corner."

Three things are not said anywhere. "Not Required" has no stated reason (already compliant, no crossing, outside DOT jurisdiction). "Planned Construction" has no stated schedule. The site script also draws a `Compliant` status that no row in the Open Data file holds.

On the unit of tracking, DOT says corner, in several places.

The dictionary's dataset sheet: "Each row is a... Corner Point", and under limitations:

> This data indicates the status of pedestrian ramp construction at corners since July 2017.

The dataset description and the Program Progress page:

> The term corner refers to intersection corners (space on the sidewalk at the intersection of two streets), midblocks (a crossing that is not at an intersection, usually in between two streets), tops of T-shaped intersections, medians or islands (a small section of raised concrete in the street). A corner can have one or more ramps.

The FY 2026 Annual Progress Report (`sources/dot_Ped_Ramp_Annual_Report_FY26.pdf`, https://www.nycpedramps.info/sites/default/files/2026-08/Ped_Ramp_Annual_Report_FY26.pdf, page 6, footnote 1):

> Corners Constructed tabulation is based on the most recent construction activity at each corner.

The reports changed unit. The settlement's Exhibit D asks for "Total Numbers of Pedestrian Ramps Installed" and four more ramp counts. The reports issued 2020-02-14 through 2021-02-12 list "Total number of pedestrian ramps installed." From the report issued 2021-08-13 onward the list reads "Total number of corners installed." and the chart title is "Corners Constructed". No report explains the change. I found no DOT sentence that says in so many words "progress is tracked per corner, not per ramp". The statements above are the nearest.

DOT's internal service behind the progress dashboard (`sources/dotweb01_Corner_Construction_Report_layers.json`) has a `Ramps_Constructed` integer per corner, so DOT does hold a ramp count per constructed corner. That field is not in `e7gc-ub6z`.

The settlement defines the complex and standard corner (section I, E and AA; the scan reads "comer" for "corner" in places):

> "Complex Corner" means a corner for which a unique design drawing must be prepared in order to install or Upgrade a pedestrian ramp(s) at that [corner], due to an unusual site condition

> Each [corner] will be considered to be a Standard [corner] unless it is expressly designated as a Complex [corner] pursuant to Section I(E).

## C. Inferences from the data (not DOT documentation)

Everything in this section is my reading of the layer's own rows. DOT states none of it. Each claim is an assert or a table in `sources/crosstab_codes.py`, which reads only `raw/compliance/`.

### `Compliant < 60` is a field-checked cut-through narrower than 59 inches

All 225 rows are `RAMP_TYPE` Cut-Through at `CORNER_TYPE` Island/Median. On all 4,676 cut-throughs `COMPLIANCY_TOTAL` equals `CUT_THROUGH_CHECK`, value for value. `CUT_THROUGH_CHECK` against `RAMP_WIDTH` (inches):

| `CUT_THROUGH_CHECK` | under 35 | 35 to under 59 | 59 to under 60 | 60 or more | 999 sentinel |
|---|---|---|---|---|---|
| Compliant < 60 | 1 | 224 | 0 | 0 | 0 |
| Compliant | 0 | 0 | 12 | 339 | 0 |
| Non-Compliant | 22 | 673 | 68 | 3,318 | 19 |

The widest `Compliant < 60` is 58.9 and the narrowest `Compliant` is 59.0. The two compliant classes split exactly at 59 inches, which is 60 inches less one inch. That fits the item summary's "adjusted with tolerance". `RAMP_WIDTH_CHECK` on cut-throughs splits the same way at 35 inches (36 less one). All 576 cut-throughs in either compliant class have `CURB_REVEAL_SOURCE` AKRF, and all 4,100 Non-Compliant cut-throughs have none, so both compliant classes are ramps a consultant measured on site.

So the checkable part is: `Compliant < 60` means a cut-through that DOT's assessment passed and whose measured width is under 59 inches. The guess in the task brief (ramp width or landing dimension under 60 inches) is right about width and wrong about landings: 222 of the 225 have the 999 sentinel in `LND_LENGTH`.

Why 60 inches is a further step and is not checkable from the data. Two published standards differ on island crossings. The 2010 ADA Standards section 406.7 requires a level area "48 inches (1220 mm) long minimum by 36 inches (915 mm) wide minimum". PROWAG R302.2.1 says "The clear width of pedestrian access routes crossing medians and pedestrian refuge islands shall be 60 inches (1525 mm) minimum". Both texts are cached in `research_notes/evidence/standards/` (retrieved 2026-10-02T15:28:27Z from access-board.gov). A plausible reading is that the class is for cut-throughs that pass the 2010 ADA test but fall short of the 60 inch PROWAG width. DOT does not say this.

### `TBD` is a ramp with no failed check and at least one `Manual Check`

For the 213,003 ramps that are not cut-throughs, this rule reproduces `COMPLIANCY_TOTAL` on every row. Take the 15 check fields other than `CUT_THROUGH_CHECK` and the three landing sub-checks (which roll up into `LANDING_CHECK`). If any is Non-Compliant the total is Non-Compliant. Otherwise, if any is Manual Check the total is TBD. Otherwise it is Compliant.

| Rule output | Actual Non-Compliant | Actual TBD | Actual Compliant |
|---|---|---|---|
| Non-Compliant | 210,100 | 0 | 0 |
| TBD | 0 | 113 | 0 |
| Compliant | 0 | 0 | 2,790 |

Of the 113, the manual check is `OBSTACLES_CHECK` on 105 and `DWS_CHECK` on 8. The other 17,231 ramps with a Manual Check also fail some other check and are Non-Compliant. All 113 are `Pending` in `COMPLIANCY_STATUS`. So `TBD` reads as "would be Compliant, but one element could not be decided from the imagery". The 144 `DWS_CHECK` manual rows all have `DWS_CONDITION` Not Applicable.

### What separates `Pending` from `Non-Compliant` in `COMPLIANCY_STATUS`

| `COMPLIANCY_STATUS` | `DWS_CHECK` Non-Compliant | `DWS_CHECK` Compliant | `DWS_CHECK` Manual Check |
|---|---|---|---|
| Non-Compliant | 145,890 | 0 | 0 |
| Pending | 0 | 69,262 | 144 |
| Compliant | 524 | 1,859 | 0 |

Every published Non-Compliant ramp fails the detectable warning check, and no ramp that passes it is published as Non-Compliant. The 524 exceptions are field-checked cut-throughs. 66,588 ramps that are not cut-throughs fail at least one other element in `COMPLIANCY_TOTAL` and are published as Pending. So on DOT's public map a ramp with a failed slope and a sound warning surface is "Pending Technical Review", not "Non-Compliant". This is consistent with the Transition Plan's "no detectable warning surface present" sentence and with site constraints mattering for slopes and landings, but DOT does not state the rule.

Published `Compliant` goes with a consultant's curb reveal measurement: all 2,383 have `CURB_REVEAL_SOURCE` AKRF or GPI. 769 ramps with a consultant measurement are not published Compliant (37 Non-Compliant, 732 Pending). This fits the Transition Plan's statement that each ramp identified as compliant "was sent to an independent consultant for additional verification".

### Consequence for the backlog table

The headline in `METHOD.md` uses `COMPLIANCY_TOTAL`, which is the stricter, unpublished roll-up. The second figure (81,390 on `COMPLIANCY_STATUS`) is the one that matches what DOT publishes as Non-Compliant. The gap between the two figures (45,005) is ramps DOT publishes as Pending Technical Review, less at most 37 ramps that are Non-Compliant on the published field only. Treating `Compliant < 60` (225) as compliant and `TBD` (113) as undetermined, as `METHOD.md` does, agrees with the data patterns above.

Note added 2026-10-03: `METHOD.md` has since been revised to lead with 81,390 on `COMPLIANCY_STATUS` and to give 126,395 on `COMPLIANCY_TOTAL` beside it.

## Where I looked

| Source | Saved as (under `sources/`) | Result for `TBD`, `Compliant < 60`, `Pending` |
|---|---|---|
| Compliance layer and service REST metadata | `arcgis_compliance_layer0.json`, `arcgis_compliance_featureserver.json`, `arcgis_compliance_layers_all.json` | No aliases, domains or descriptions |
| ArcGIS item record, popup config, formal metadata, related items | `arcgis_item_996cb79f*.json`, `arcgis_item_996cb79f_metadata.xml` | "adjusted with tolerance" only |
| Other public items of the same owner | `arcgis_search_nycpedramps.json` | Service definitions with the same text |
| Sibling services and their items | `arcgis_RAMPS_SURVEY_2019_*`, `arcgis_CORNERS_SURVEY_2019_*`, `arcgis_PRP_Corner_Activity*`, `arcgis_item_*.json` | No compliance fields |
| DOT org service list (212 services) | `arcgis_org_services_root.json` | No other ramp compliance layer |
| Progress dashboard, its web map and DOT map service | `arcgis_item_dashboard_8fead016*.json`, `arcgis_item_webmap_1a502027*.json`, `dotweb01_Corner_Construction_Report_*.json` | Construction fields only |
| nycpedramps.info home, survey, program progress, FAQs, about, resources | `nycpedramps_*.html` | `Pending` defined on survey page. No glossary or "about the data" page exists. |
| Site map scripts | `nycpedramps_js_module4.js`, `nycpedramps_js_map_library.js`, `nycpedramps_js_script.js` | `Pending` mapped to its label. `map.js` is an empty file. |
| Wayback captures of the survey page | `wayback_cdx_survey.txt`, `wayback_2020*.html`, `wayback_2021*.html` | Same text as today |
| Socrata metadata for `e7gc-ub6z`, `ufzp-rrqu`, and the related `jagj-gttd`, `u7ws-2dus`, `r94j-3cph` | `socrata_*_views.json`, `socrata_catalog_pedestrian_ramp.json` | `Pending` in the `ufzp-rrqu` description |
| Both attached data dictionaries | `socrata_*.xlsx` (byte-identical to the copies in `research_notes/evidence/ramp_dictionary/`) | Corner statuses defined. The survey dictionary has no compliance column. |
| Settlement agreement, 2019 | `settlement_agreement_2019-07-23.pdf` | No codes |
| Transition Plan 2023 and Appendix A 2021 | `transition_plan_2023-07.pdf`, `transition_plan_appendix_A_2021-12.pdf` | Three categories and the non-compliant test |
| All 21 progress reports, FY20 to FY26 | `dot_Ped_Ramp_*Report*.pdf` | No codes. Corner unit. |
| Program overview, 2021 design guidance, construction triggers, PRISM guide, TI 22-001, TI 23-001, TB 22-001, BPP FAQ, H-1011 standard details | `dot_*.pdf`, `ddc_SB22-004_*.pdf` | No codes |
| nyc.gov DOT pedestrian ramps page | `nycgov_dot_pedramps.shtml` | No codes |
| Court dockets and class counsel's case page | `courtlistener_*.html`, `courtlistener_*.json`, `dra_case_page_cidny_v_city.html` | No monitor report found |
| Web search for the layer name and the code strings | not saved | Only mirrors of the `ufzp-rrqu` description |

Text extracts of every PDF are in `sources/text_extracts/`. The URL of each DOT PDF is in `sources/_pdf_urls.txt`.

## Access failures and limits

These are not findings about the data.

- `https://a841-dotweb01.nyc.gov/arcgis/rest/services/PRP?f=json` reset the connection twice, so I could not list other services in DOT's PRP folder. The one service I had a direct URL for answered.
- The Wayback availability API returned no snapshot for the survey page while the CDX index listed 56. I used the CDX index. I did not fetch archived copies of the map script.
- PACER documents were not fetched. The monitor's order of appointment and any sealed or unfiled monitor reports are unread.
- The settlement PDF is a scan. Its text extract has OCR errors ("comer" for "corner"), so a search miss in it is weaker evidence than a miss in the born-digital PDFs.
- Chart images inside the progress reports were not read, only their text layers.
- `ruff` is not installed here, so `sources/crosstab_codes.py` was run but not linted.

## Reproduce

`python3 sources/crosstab_codes.py` reads `raw/compliance/` and rewrites `sources/crosstab_codes.json`. It stops on an assert if the roll-up rule, the cut-through rule or the 59 inch split no longer holds.
