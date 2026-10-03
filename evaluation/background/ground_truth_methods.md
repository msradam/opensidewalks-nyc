---
type: research
---

# Ground truth for the Brownsville demo: how others verify, and a plan

Retrieved and computed 2026-10-03. Sources were read with WebFetch and curl only; no browser automation. The literature for sections 1, 2 and 4 was read by language-model instances, each of which fetched the paper or official page itself. Where only an abstract could be seen, the text says so. Web search was limited, so papers were found through arXiv, OpenAlex, PubMed and Europe PMC, and some sources were never reached (listed under open questions). Brownsville counts come from this repo's v0.3.3 FlatGeobuf, `evaluation/ramp_backlog/backlog_by_community_district.csv` and a fresh pull of `e7gc-ub6z`, clipped to Community District 316 (NYC Open Data `5crt-au7u`).

## Verdict

A field audit by hand is the only check that wheelchair users, city staff and researchers will all accept. Run it with a written protocol, two measurers, and wheelchair users as paid co-researchers who travel the routes. Nothing remote can stand in for it, for four reasons.

1. **The ramp attributes have never been field checked.** DOT's 2023 Transition Plan says the ramp survey used "street level imagery and mobile LiDAR technology" from car-mounted equipment, captured March 2017 to October 2018. The settlement says values were taken by placing key points on imagery and computing with "a software calculator". So the ramp slopes in the graph are LiDAR extractions, not measurements with a level. DOT names a QA consultant but publishes no QA method, sample or error rate.
2. **The graph's incline cannot settle a ramp's slope.** It comes from the 2017 airborne LiDAR, which has 0.074 m vertical accuracy at 95% and about 11 points per m². Across a 1.5 m ramp, that error swamps the difference between 8.3% and 12%.
3. **Imagery cannot show what matters.** Orthoimagery shows where a ramp is but not its slope or condition (the crossing check saw a ramp directly at only 9 of 400 ends). Google's terms forbid "creating data from Street View images" for "academic, nonprofit, and commercial projects".
4. **No comparable work has done this.** No NYC sidewalk or slope dataset has published a ground truth check. Gridnberg says so in plain words, and NYCWalks reports only a check against aerial imagery. A small, honest field audit in Brownsville would be the first.

The plan in section 5 has four parts: 70 corners (about 125 ramps), 40 block faces, and 12 origin and destination pairs travelled both ways blind, 24 traversals in all. That is about two weeks of fieldwork and 80 to 100 hours of paid co-researcher time. Its claims hold for Brownsville, at stated confidence, and for no other neighbourhood.

## 1. How others validate pedestrian and accessibility networks

The pattern across the published work is consistent. Presence and position of curb ramps reach agreement near 0.9. Condition and severity reach 0.2 to 0.6. Slope from remote sensing agrees with a level less often than presence does, and almost nobody reports it.

| Work | What was checked, against what | Sample | Statistic | Source |
|---|---|---|---|---|
| OpenSidewalks Washington pathways, Zhang, Howe, Caspi (arXiv 2410.19762) | Segmentation against a held-out benchmark. The graph was scored against "independent expert mappers" with edge F1 and TraversabilitySimilarity (Jaccard of connected boundary pairs per intersection polygon). | 3 King County sites. Mappers were 5 undergraduate interns after 2 hours of training. | mIoU 0.69 (sidewalk 0.63, crossing 0.59, corner 0.48). Traversability 0.31 to 0.47 for automated graphs, 0.84 to 0.88 after human edits. Edge F1 0.96 to 0.99. No inter-mapper agreement is reported; the authors list it as future work. | https://arxiv.org/html/2410.19762 |
| PathwayBench (arXiv 2407.16875) | Same metrics, 8 cities | about 3,000 km² | Tile2Net traversability 0.04 to 0.35, edge F1 0.76 to 0.90 | https://arxiv.org/html/2407.16875 |
| TDEI quality metric code | `xn` scores internal connectivity per Voronoi polygon. It needs no reference, so it is not ground truth. | n/a | none | https://github.com/TaskarCenterAtUW/TDEI-python-osw-quality-metric |
| Project Sidewalk, Saha et al. CHI 2019 | Crowd labels scored against researcher ground truth, per street segment | 625 segments in DC: 3,212 curb ramps, 87 missing ramps | Coders reached Krippendorff α 0.6 after 7 rounds. Curb ramp recall 86.0%, precision 95.4%. Missing ramp recall 69.3%, precision 20.5%. | https://makeabilitylab.cs.washington.edu/media/publications/Saha_ProjectSidewalkAWebBasedCrowdsourcingToolForCollectingSidewalkAccessibilityDataAtScale_CHI2019.pdf |
| Askari et al., Urban Science 2025 | Street View audit against government field data | 9 areas each in Seattle and DuPage County | Ramp presence agreement 89.9% (n=193) and 93.5% (n=93). Severity agreement 63.8% and 20.8%. | https://makeabilitylab.cs.washington.edu/media/publications/Askari_ValidatingPedestrianInfrastructureDataHowWellDoStreetViewImageryAuditsCompareToGovernmentFieldData_UrbanSci2025.pdf |
| LabelAId, CHI 2024 | Project Sidewalk peer validation | 3,574 labels, 34 people | Precision up 19.2%. The authors warn that peer validation lets "repeated errors pervade the system". | https://arxiv.org/html/2403.09810 |
| RampNet (arXiv 2508.09415) | Rated government ramp datasets for location precision against manual labels on panoramas | 1,000 panoramas, 3,919 ramps | NYC's survey rated "Good". Label precision 94.0%, recall 92.5%. 17% of sampled errors were disagreements with government data. | https://arxiv.org/html/2508.09415 |
| Hara et al. UIST 2014 (pre-2018, the classic field comparison) | Street View against a physical audit | 273 intersections, about 25 field hours | Spearman ρ 0.996 for ramp counts and 0.977 for missing ramps | https://makeabilitylab.cs.washington.edu/media/publications/Hara_TohmeDetectingCurbRampsInGoogleStreetViewUsingCrowdsourcingComputerVisionAndMachineLearning_UIST2014.pdf |
| tile2net, CEUS 2023 | Not read (publisher and SSRN returned 403, no arXiv). PathwayBench says it was scored by mIoU and by edges within 4 m of ground truth, in Cambridge, Boston and Manhattan. | | | https://doi.org/10.1016/j.compenvurbsys.2023.101950 |
| NYCWalks, Nature Cities 2026 | Supplementary files say the network was "humanly verified... against 2019 aerial imagery". The main text was paywalled. No geometry accuracy statistic was found; the validation is of the foot traffic model against 1,011 counts. | | | https://www.nature.com/articles/s44284-025-00383-y |
| Gridnberg (arXiv 2607.22523) | Internal checks only: "no systematic comparison with survey-grade sidewalk profiles or field observations has established error rates". 470 segments read a grade of 100% or more. | | | https://arxiv.org/html/2607.22523 |
| MAPS (Millstein 2013) | In-person audit reliability | 516 segment pairs, 319 crossing pairs | Crossing impediments subscale (includes "no curb ramp") ICC 0.728. 28.5 min per residential route. | https://pmc.ncbi.nlm.nih.gov/articles/PMC3728214/ |
| MAPS online against in-person (Phillips 2017) | Virtual against field | 120 routes. Raters trained 15 hours or more and certified at 95% agreement. | Grand score ICC 0.93 | https://pmc.ncbi.nlm.nih.gov/articles/PMC5545045/ |
| CANVAS (Bader 2015) | Virtual audit reliability | 150 segments, 3 auditors | Curb cuts κ 0.514. 17.1 min per segment. | https://pmc.ncbi.nlm.nih.gov/articles/PMC4315325/ |
| CANVAS NYC (Mooney 2020) | Virtual audit of NYC intersections | 111 intersections | κ from -0.01 to 0.92 | https://pmc.ncbi.nlm.nih.gov/articles/PMC7002252/ |
| Point cloud ramp compliance (arXiv 2505.05752) | Mobile point cloud against smart level | 16 ramps | 87.9% of compliance calls agreed. Over half of detected ramps failed QC because the cloud was sparse. | https://arxiv.org/html/2505.05752 |
| OmniPath (arXiv 2606.24129) | Aerial LiDAR ADA audit against field surveys | 200 stratified sites | F1 0.60 and 0.58 for severe and critical barriers (abstract only) | https://arxiv.org/abs/2606.24129 |

PEDS (Clifton 2007) is closed access and was not read.

City and federal practice:

- **PROWAG.** The final rule (36 CFR 1190, 2023) sets ramp running slope at 8.3% max and cross slope at 2.1% max (R304.2.1, R304.2.2). It refused to codify tolerances, and it sends questions on how to measure "to the enforcing agencies". Source: https://www.ecfr.gov/current/title-36/chapter-XI/part-1190.
- **Access Board protocol.** Its research report on dimensional tolerances (Ballast, 2011) is the nearest thing to a field protocol. It asks for 1/16 in precision on distances and 0.1° on slope. Running slope is read with a 24 in digital inclinometer stepped 12 in at a time, and cross slope every 4 ft. It suggests a +0.5% tolerance, with at least 80% of local readings at or below 8.3% and none above 10%. This is research guidance, not a rule. Source: https://www.access-board.gov/research/building/dimensional-tolerances/.
- **MnDOT.** Its curb ramp checklist takes the steepest running slope with a smart level on a 10 ft straightedge. Source: https://www.dot.state.mn.us/ada/pdf/CurbRampChecklistGuidance.pdf.
- **NYC DOT.** The survey was mobile LiDAR and imagery from Cyclomedia, with 13 elements per ramp (curb reveal, running slope, warning surface, gutter slope, landing size, landing cross slope, road grade, width, flare slope, length, cross slope, ponding, obstruction). A ramp found compliant was "sent to an independent consultant for additional verification". The plan is at https://www.nycpedramps.info/sites/default/files/2023-07/transition-plan-pedestrian-ramps-2023.pdf, pages 8 to 9; I read that passage myself.
- **Settlement oversight.** The settlement (approved 2019-07-23) appoints a Monitor who must be a licensed PE. The Monitor's reports were not found on public pages; they may be on PACER. Source: https://www.nycpedramps.info/sites/default/files/2019-07/Pedestrian%20Ramp%20Settlement%20Agreement--Final%20Approved%207-23-2019.pdf.

What this means for the project. The 105-feature sample and the 200-crossing check match what the literature does for presence and position, and kappa 0.81 is above what Project Sidewalk's coders reached. What no one has done for NYC is check slope and condition against a level in the field. That is the gap the Brownsville audit fills.

## 2. Field verification

### What to measure, and how

Measure the same elements DOT extracted, so each field value pairs with a survey value. Use the Access Board's method so a city engineer recognises it.

| Element | Graph field | Field method |
|---|---|---|
| Ramp present, and serves which crossing | `kerb=lowered`, crossing links | Look, photograph, record which crossing each ramp leads to |
| Running slope | `ext:running_slope_pct` | 24 in digital level stepped up the ramp at 12 in. Record each reading and the maximum. |
| Cross slope | `ext:cross_slope_pct` | Level across the ramp, at least 2 readings |
| Counter slope (gutter) | `ext:counter_slope_pct` | Level in the gutter at the ramp foot, with a spotter, during the walk phase |
| Ramp width | not carried | Tape at the narrowest point, to 1/16 in |
| Detectable warning | `tactile_paving` | Present, missing or defective (DOT's own categories) |
| Lip at the gutter | not carried | Tape or gauge on the height of the step; this is what stops a chair |
| Ponding, obstruction | not carried | Obstruction yes or no. Ponding only after rain, so record it as not assessed. |
| Sidewalk clear width | `width` (curb to property line from planimetric data) | Narrowest clear width on the block face, after poles, trees and stoops |
| Sidewalk running slope | `incline` (2017 LiDAR) | Level at 3 points along the block face, plus a rise over run from tape and level |
| Surface | `surface` | Category, plus a count of vertical changes over about 1/2 in |

### Instruments

| Device | Stated accuracy | In % grade | Source |
|---|---|---|---|
| Stabila TECH 196 DL digital level | ±0.05° at 0° and 90°, ±0.10° from 1° to 89° | about 0.17% | https://www.stabila.com/en/products/details/tech-196-dl-digital-spirit-level.html |
| Bosch GIM 60 | ±0.2° from 1° to 89° | about 0.35%, marginal under the Access Board's one-third rule | https://www.bosch-professional.com/gb/en/products/gim-60-0601076700 |
| Phone inclinometer apps | No accuracy study found | | unverified |

Use a level of about ±0.1°, and check it each morning by reversing it 180° on the same spot. Use a phone only for photographs and position. Prices were not checked.

### Sampling

These are standard proportion formulas (NIST e-Handbook 7.2.4.2, https://www.itl.nist.gov/div898/handbook/prc/section2/prc242.htm). The intervals are Wilson intervals I computed.

| Goal at 95% | Sites |
|---|---|
| Agreement rate within ±10 points, worst case | 97 |
| Within ±10 points if agreement is near 90% | 35 |
| Zero errors seen: upper bound on the error rate | 30 sites give 9.5%, 60 give 4.9%, 100 give 3.0% |
| 57 of 60 agree | interval 86.3% to 98.3% |
| 20 of 24 routes pass | interval 64.1% to 93.3% |

Sample by corner, and measure every ramp at a sampled corner. Fieldwork goes faster, and the analysis must account for ramps clustering within corners. Stratify by DOT's construction status, because that sets what the survey can still claim. The Special Master's 2017 report rejected an earlier extrapolation from a sample "mostly in Lower Manhattan" (https://dralegal.org/wp-content/uploads/2017/08/Special_Master_Report_Accessible.pdf). A sample that a city lawyer has already called unrepresentative is the one to avoid.

### Safety and permissions

- **Permits.** NYC DOT permits cover work that "will impact the street or occupy it with equipment" (https://streetworksmanual.nyc/chapter-three/permits-and-approvals). No permit category for measuring alone was found. This is absence of evidence, not a ruling. Hand-held measurement with no cones or tripods stays clear of "occupy".
- **Gutter readings.** Take them during the walk phase. One person measures while a second faces traffic.
- **Vests.** Wear high-visibility vests. MUTCD 11th edition 6C.05 requires ANSI 107 Class 2 or 3 apparel for workers in a work zone (https://mutcd.fhwa.dot.gov/pdfs/11th_Edition/part6.pdf). An audit is not a work zone, but this is the norm a city reviewer will look for.
- **Hours and photographs.** Work in daylight. Photograph the level reading with the ramp in frame, and keep faces and plates out of shot.

### Wheelchair users as co-researchers

- **What the published studies did.** Wheelchair users appear as participants: Rosenberg et al. 2013 (35 mobility device users, GPS and interviews, PubMed 23010096); Meyers et al. 2002 (25 wheelchair users reporting barriers daily, PubMed 12231020); Tannert et al. 2019 (two wheelchair users with accessibility expertise travelled every route as the ground truth, section 4).
- **What could not be reached.** No verified guidance on disability-led research practice or on standard pay rates was found. NIDILRR's participatory action research guidance was never reached.
- **The design here.** It follows Tannert: wheelchair users are the raters whose judgement is the ground truth for routes, not subjects of the study. Recruit them through a disability-led organisation. Pay for all time, including training and travel, in cash or equivalent, at a rate agreed with that organisation. The legal floor in NYC is $17.00 an hour (NY DOL, https://dol.ny.gov/minimum-wage-0, fetched 2026-10-03). An expert rater is closer to a consultant than to a minimum-wage worker. Cover accessible transport to start points.
- **Consent and ethics review.** The Common Rule binds only federally conducted or supported research (45 CFR 46.101(a)), so this project is not legally required to have IRB review. Researchers will still ask for it, so get an independent review or partner with a university. Write consent in plain language and offer it in large print and screen-readable form. Co-researchers decide whether they are named or credited.
- **Who sets the criteria.** Agree the pass and fail criteria for routes with the co-researchers before fieldwork, and freeze them. It is their judgement of "passable" that the claim rests on.

### NYC organisations (public information only; nobody was contacted)

| Organisation | Public role | Source |
|---|---|---|
| CIDNY | Named plaintiff in the curb cut case. Says it ran surveys and gave participants' statements to the court. | https://www.cidny.org/litigation-updates/curb-cut-case/ |
| Disability Rights Advocates | Class counsel in the ramp case | https://dralegal.org/case/center-independence-disabled-new-york-cidny-et-al-v-city-new-york-et-al/ |
| Brooklyn Center for Independence of the Disabled | Brooklyn independent living centre. Was an objector at the 2016 fairness hearing. | https://www.bcid.org (summary only) |
| Brooklyn Community Board 16 | Has a Transportation and Franchises Committee | https://www.nyc.gov/site/brooklyncb16/committees/committees.page |
| Brownsville Community Justice Center | Placemaking and Open Streets on Belmont, Osborn, Watkins and Thatford | https://www.innovatingjustice.org/programs/brownsville-community-justice-center (summary only) |

The Mayor's Office for People with Disabilities, TransitCenter, Riders Alliance and the Brownsville Partnership showed no relevant public sidewalk or ramp work on the pages fetched. That is unverified, not a finding.

## 3. Remote and imagery sources, and their terms

| Source | What it shows | Terms | Independent key? |
|---|---|---|---|
| NYC orthoimagery 2024 (OTI) | 6 in (15 cm), captured 14 to 24 March 2024; also 2018, 2020, 2022 | CC BY 4.0 (NYC metadata, https://github.com/CityOfNewYork/nyc-geo-metadata/blob/master/Metadata/Metadata_AerialImagery.md, edited 2026-03-23) | Yes, for presence and position of crossings, corners and the larger obstructions as of March 2024. No, for slope, lip or ramp condition. The crossing check saw a ramp directly at 9 of 400 ends. |
| NY State orthoimagery | The `Latest` service blends 2022 to 2025 at about 12 in. Kings was flown in the 2024 lot, and the 2026 lot lists Kings. | No licence statement found on https://gis.ny.gov/orthoimagery or the service; copyright text "NYS ITS Geospatial Services" | Yes for position, at coarser resolution than the city's |
| NYC 2017 LiDAR | NVA 0.074 m at 95%, relative 0.026 m, about 10.75 first returns per m², flown 3 to 17 May 2017 (https://github.com/CityOfNewYork/nyc-geo-metadata/blob/master/Metadata/Metadata_TopobathymetricClassifiedPointCloud.md) | Public | No. It is the graph's incline input, and too coarse for a ramp. |
| Mapillary | Street-level photos, crowd captured | User content under CC BY-SA. Section 13 permits use "to develop, edit and contribute content to OpenStreetMap or the Overture Maps Foundation". Commercial use is limited to the purposes in section 12. Terms effective 2024-02-15 (https://www.mapillary.com/terms). | Yes, to look at a corner and record a judgement, with attribution. Publishing photo crops brings in ShareAlike. Brownsville coverage and capture dates were not checked (the API needs a token). |
| KartaView | Street-level photos | Images CC BY-SA 4.0 (OSM wiki, edited 2024-11-15). The terms page did not render without a browser. | Same as Mapillary, and coverage is likely thinner (unverified) |
| Google Street View | Street-level photos, often with a date | Prohibits "Creating data from Street View images, such as digitizing or tracing information from the imagery" and "Using applications to analyze and extract information", which "apply to all academic, nonprofit, and commercial projects" (https://about.google/brand-resource-center/products-and-services/geo-guidelines/). The Maps terms (modified 2026-01-27) bar using Maps "to create or augment any other mapping-related dataset" (https://www.google.com/help/terms_maps/). The Platform terms (modified 2026-08-26) bar creating content from Maps Content and using it to "train, test, validate" models (section 3.2.3(c)). | No. Do not record labels from it. Field raters may still use it privately to plan a visit. |
| DOT corner progress `e7gc-ub6z` | Per-corner status and construction date. 43,625 Brooklyn corners. Rows updated 2026-10-01, monthly. | NYC Open Data; no licence field (Administrative Code 23-502(d)) | No. It is DOT's own record, so it says whether the survey is stale, not whether a ramp is usable. It is the sampling frame for the field audit. |

So: no remote source independently checks slope, lip or condition, and the one with the best currency (Street View) is barred. NYC's 2024 orthos are the clean remote key for position. Mapillary is the clean street-level one, if it covers Brownsville.

## 4. Route-level ground truth

| Work | Method | Sample | Outcome | Source |
|---|---|---|---|---|
| Tannert, Kirkham, Schöning 2019 | OpenRouteService wheelchair and Routino against Google walking routes in Bremen. Routes were sampled 15 each at high, middle and low overlap with the walking route. Two wheelchair users with accessibility expertise travelled every route independently with a checklist and resolved disagreements together. | 45 routes per router, 160 km | Classes: passable by power chair, by manual chair only, or impassable. Type I error = detour around a barrier that is not there. Type II error = a route called accessible that is impassable. ORS: 39 of 45 passable by manual chair. Routino: 34 of 45. No blinding. | https://link.springer.com/chapter/10.1007/978-3-030-29381-9_13 |
| Bolten thesis 2020 (AccessMap) | Lab session. People rated each route from 1 to 5 for confidence. | 4 manual and 11 power chair users, one origin and destination | Personalised routes rated highest. People reported up to 4 hours of planning for one trip. | https://digital.lib.washington.edu/bitstreams/9ca04291-d923-490a-ad65-c76a252440d0/download |
| Bolten and Caspi 2021 | Graph metrics only (sidewalk reach by profile). Profile limits 8% up and 10% down for manual chairs. | n/a | n/a | https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0248399 |
| Kasemsuppakorn et al. 2015 | Personalised routes against the shortest feasible route, with sessions before and after | 5 manual wheelchair users | Personalised routes 14.64% longer, better slope and surface (abstract only) | https://pubmed.ncbi.nlm.nih.gov/24649869 |
| Basiri 2020 | 15 days of phone GPS, 7 unguided and 8 following suggested paths. Traces compared by Hausdorff distance and LCSS. | 19 people, 13 of them manual wheelchair users | At least 76.4% similarity. 14 of 19 said the paths matched their usual routes. | https://arxiv.org/abs/2011.03850 |
| Iwasawa et al. 2016; Watanabe et al. 2019 | Accelerometer on the chair, labelled from video | 9 users each | F-score 0.63 for curbs and 0.50 for slopes. Sensors are a research topic, not a ground truth. | https://www.jstage.jst.go.jp/article/transinf/E99.D/4/E99.D_2015EDP7278/_pdf |
| OpenPaths (arXiv 2606.07486), NYC | Desk audit of 189 pairs against the same criteria for both systems. Only 30.4% of DOT ramps pass its filter. | 189 pairs | All OpenPaths routes passed its own audit, against 50.3% of Google transit routes. No field check. | https://arxiv.org/abs/2606.07486 |
| Gridnberg | Two illustrative routes: length change, elevation gain, maximum grade. Says its profile is "accessibility-sensitive rather than barrier-free". | 2 | n/a | https://arxiv.org/html/2607.22523 |

No study was found that compares router output with routes people travelled, by go-along interview or travel diary, other than Basiri's GPS work. No protocol for expert review of wheelchair routes by orientation and mobility specialists was found. OpenPaths is a direct NYC comparator that has made route claims from DOT's data with no field check. A Brownsville field check would answer what it leaves open.

## 5. Brownsville verification plan

### The frame (computed 2026-10-03)

Community District 316 covers 4.81 km². Inside it, the v0.3.3 graph has:

- 2,363 curb ramp nodes at 1,329 corner IDs, of which 2,259 have a running slope
- 4,243 sidewalk edges (about 142 km)
- 2,541 crossing edges (15.2 km)
- 66 directed steps edges

DOT's progress data lists 1,607 corners:

| Status | Corners |
|---|---|
| Planned Construction | 707 |
| Constructed | 521 |
| Complex Planned | 132 |
| Not Required | 98 |
| Not Assigned | 81 |
| Complex Constructed | 68 |

Of the constructed corners, 64 were built in 2025 or 2026, after the 2024 orthos were flown.

The backlog table gives the ramp-level picture:

- 1,024 ramps DOT publishes as Non-Compliant sit at 608 corners not shown as rebuilt.
- 961 non-compliant ramps are at corners rebuilt after their survey.

So for about two in five Brownsville ramps, the graph carries a measurement of a ramp that may no longer exist.

### Sample, measures and claims

| Part | Sample | Measures | Raters | What a result lets the project claim |
|---|---|---|---|---|
| A. Survey still current | 30 corners drawn at random from Planned, Complex Planned and Not Assigned, with every ramp at each (about 55 ramps) | All of the ramp table in section 2. Field values paired with survey values. | Two measurers. A second measurer repeats 1 corner in 5 blind, for an ICC on slopes and κ on categories. | "At corners not rebuilt since 2018, the survey's running slope is within ±X points of a hand measurement for 95% of ramps (limits of agreement), and the 8.3% pass or fail call agrees on k of n (interval)." A wide X says the survey slopes should be shown as screening values only. |
| B. Rebuilt corners | 25 corners drawn from Constructed and Complex Constructed, with all ramps (about 45). Oversample the 64 built after March 2024. | Same as A | Same | "Of ramps DOT shows rebuilt since the survey, k of n pass 8.3% running and 2.1% cross slope by hand." This decides whether the graph should drop survey slopes at rebuilt corners or mark them superseded. |
| C. Missing and not required | 10 corners with a crossing and no surveyed ramp node in the graph, plus 5 Not Required | Is there a ramp? Is the crossing usable? | Same | "No missing ramps in 15 corners (upper bound 18%)" is weak by itself. It mainly guards against the worst kind of error. |
| D. Crossing rule | Every crossing at the 65 corners in A to C | Does a usable ramp serve it at both ends? | Same | Repeats the 0.988 precision of the imagery check, now on the ground. It tests whether the ramp exists and is usable, which the imagery could not. |
| E. Sidewalks | 40 block faces: 20 at random, 20 where `incline` is over 4% or `width` under 1.5 m | Clear width at the narrowest point, obstructions, surface changes, slope at 3 points | Two measurers | "Graph width overstates clear width by a median of Y m. Incline agrees with a hand rise over run within Z." This extends the Staten Island result (rank correlation 0.42). |
| F. Routes | 12 origin and destination pairs chosen with the co-researchers from real trip ends (clinic, school, subway entrance with an elevator, library, NYCHA campus), where the wheelchair route and the shortest pedestrian route differ. Both routes travelled, 24 traversals, unlabelled and in random order. | Tannert's three classes, Type I and Type II errors, every barrier logged with a photograph, time taken, 1 to 5 confidence before and after | Two wheelchair users, at least one manual, travelling independently. A walking companion carries a phone with GPS. | "k of 12 profile routes passable by a manual wheelchair user per both raters (interval). Type II errors j." Each Type II error is a defect with a location to fix. Each passable pedestrian route that the profile avoided is a Type I error, and it tells you the cost of the ramp rule. |

Pre-register before anyone goes out: the sample (seeded draw from the frame), the protocol, the pass criteria for routes, and the analysis. Publish the field sheets and photographs with the result. Keep the field team blind to the survey values. Print sheets with ramp position only, as the crossing check did with its left panel.

### People

- A field lead and a second measurer for parts A to E. Paying one wheelchair-using co-researcher for the field sessions improves what gets noticed, such as lips and flares.
- Two wheelchair-using route raters for part F, recruited through a disability-led organisation and paid as experts.
- One reviewer from outside the project for the protocol. A licensed PE or someone who has worked on DOT's ramp programme is what city staff will recognise; the settlement's Monitor must be a PE.

### Time and cost

These are estimates from the published audit times (MAPS 18 to 28 min per route; CANVAS 17 min per segment) and Tannert's scale. They are not quotes.

| Item | Estimate |
|---|---|
| Preparation: sample draw, sheets, protocol, partner agreement, review | 3 to 5 days, plus partner lead time (weeks, unknown) |
| Parts A to D: about 125 ramps at about 10 min each, plus walking and repeats | 5 field days for a pair |
| Part E: 40 block faces | 1 to 2 field days |
| Part F: 24 traversals of about 1 km at about 45 min each with the checklist | 3 days per rater, about 18 to 20 hours each |
| Analysis and write-up | 3 to 5 days |
| Paid co-researcher time | 80 to 100 hours across three people, at a rate agreed with the partner (floor $17.00 an hour), plus accessible transport |
| Equipment | Digital level of about ±0.1°, tape, a 10 ft straightedge if following MnDOT, vests. Prices not checked. |

Do it between April and October, in dry weather. Snow and leaves hide lips and warning surfaces.

### What it will not let the project claim

- Anything about other neighbourhoods.
- A compliance finding about any ramp. The survey's own description, DOT's process and PROWAG's refusal to codify tolerances all leave compliance to the city.
- Ponding, unless someone goes out after rain.
- Error rates below a few percent. That would take hundreds of sites.

## Open questions

- Mapillary and KartaView coverage and capture dates in Brownsville (the Mapillary API needs a token).
- Whether NYC's 2026 orthoimagery exists yet (capture is every 2 years; the metadata lists up to 2024).
- The settlement Monitor's reports (possibly on PACER) and DOT's QA consultant's method and error rate. Either would let the audit compare with DOT's own verification.
- Not read: tile2net's evaluation numbers, NYCWalks' methods, PEDS, ULIP accuracy, phone inclinometer accuracy, NIDILRR participatory guidance, and published pay rates for disability co-researchers.
- Whether CIDNY's earlier surveys for the court are public and whether they cover Brownsville.
- Which ethics review route the project will use, and whether a disability-led organisation will partner. Nobody was contacted.
- Whether the demo should drop or flag survey slopes at the 589 rebuilt corners before or after part B reports.
