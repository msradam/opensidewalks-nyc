"""Build the ramp backlog tables from the cached raw files. No network.

Inputs: raw/ (written by fetch.py) and the two cached Socrata CSVs in
research_notes/evidence/raw/. Outputs sit beside this script. METHOD.md has
the definitions; this file is the executable version of them. It also writes
result.json, the headline figures and the ten largest districts on each measure.
"""

import glob
import json
from datetime import UTC, date, datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd

HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
EVIDENCE = HERE.parent / "evidence"
PROGRESS_CSV = EVIDENCE / "raw" / "e7gc-ub6z_rows.csv"
SURVEY_CSV = EVIDENCE / "raw" / "ufzp-rrqu_rows.csv"
EARLIER_JOIN = EVIDENCE / "ramps" / "program_progress_join.json"

BOROUGHS = {1: "Manhattan", 2: "Bronx", 3: "Brooklyn", 4: "Queens", 5: "Staten Island"}
BUILT = {
    "Constructed",
    "Complex Constructed",
}  # the statuses in which DOT says the corner was built
EARLIEST_PLAUSIBLE_YEAR = (
    1990  # the ADA was enacted in 1990; an earlier year is called impossible
)
CAVEAT = (
    'The survey dataset description says: "measurements shown are not indicative of whether a '
    "particular ramp is compliant with design and construction standards pursuant to the Americans "
    'with Disabilities Act (ADA). DOT applies additional parameters in its compliance assessment". '
    "DOT's classifications in the compliance layer date from 2020 (layer last edited 2020-12-28) and "
    "the survey measurements mostly from 2018."
)


def say(*a):
    print(*a, flush=True)


def load_compliance():
    rows = []
    for f in sorted(glob.glob(str(RAW / "compliance" / "page_*.json"))):
        for ft in json.loads(Path(f).read_text())["features"]:
            a = ft["attributes"]
            a["lon"], a["lat"] = ft["geometry"]["x"], ft["geometry"]["y"]
            rows.append(a)
    c = pd.DataFrame(rows)
    expected = json.loads(Path(RAW / "compliance_count.json").read_text())["count"]
    assert len(c) == expected, (len(c), expected)
    assert c.OBJECTID.is_unique and c.RAMPID.is_unique
    return c


def classify_date(s, first_survey, retrieved):
    """Return (class, date or None) for one Construction_End_Date string."""
    if s == "":
        return "blank", None
    try:
        d = datetime.strptime(s, "%Y/%m/%d").date()  # noqa: DTZ007 (a calendar day, no time zone)
    except ValueError:
        return "unparseable", None
    if d.year < EARLIEST_PLAUSIBLE_YEAR:
        return "impossible_year", None
    if d > retrieved:
        return "future", None
    if d < first_survey:
        return (
            "before_survey_programme",
            d,
        )  # a usable date: construction came before any survey
    return "ok", d


def assign_district(points, path, key):
    """Point in polygon, then nearest polygon for the few points outside every polygon."""
    poly = gpd.read_file(path)[[key, "geometry"]]
    assert poly[key].is_unique
    j = gpd.sjoin(points, poly, how="left", predicate="intersects")
    assert not j.OBJECTID.duplicated().any()
    out = j.set_index("OBJECTID")[key]
    miss = j[j[key].isna()][["OBJECTID", "geometry"]].to_crs(2263)
    info = {"polygons": len(poly), "ramps_outside_every_polygon": len(miss)}
    if len(miss):
        # ponytail: nearest polygon in feet (EPSG:2263); ties broken by first match
        n = gpd.sjoin_nearest(
            miss, poly.to_crs(2263), distance_col="ft"
        ).drop_duplicates("OBJECTID")
        out.loc[n.OBJECTID.values] = n[key].values
        info["assigned_to_nearest_polygon_max_distance_ft"] = round(
            float(n.ft.max()), 1
        )
    assert out.notna().all()
    return out.astype(int), info


def main():
    retrieved = json.loads(Path(RAW / "retrieved.json").read_text())
    progress_retrieved = (
        (EVIDENCE / "raw" / "e7gc-ub6z_rows.retrieved").read_text().strip()
    )
    progress_day = date.fromisoformat(progress_retrieved[:10])

    # ---- compliance layer: one row per surveyed ramp -------------------------------------
    c = load_compliance()
    say(f"compliance layer rows: {len(c)}, distinct corners: {c.CORNERID.nunique()}")
    c["corner"] = c.CORNERID.astype("int64").astype(str)
    c["survey_date"] = pd.to_datetime(c.GEOCYCLORAMA_DATE, unit="ms", utc=True).dt.date
    first_survey, last_survey = c.survey_date.min(), c.survey_date.max()
    say(f"survey dates in the layer: {first_survey} to {last_survey}")
    say(
        "survey year counts:",
        pd.Series([d.year for d in c.survey_date])
        .value_counts()
        .sort_index()
        .to_dict(),
    )

    # Cross-check the layer's date and corner against the published survey CSV, by ramp ID.
    s = pd.read_csv(
        SURVEY_CSV,
        dtype=str,
        keep_default_na=False,
        usecols=["CornerID", "RampID", "GeoCyclora"],
    )
    x = c.assign(RampID=c.RAMPID.astype(str)).merge(
        s, on="RampID", how="outer", indicator=True
    )
    both = x[x._merge == "both"]
    survey_check = {
        "ramp_ids_in_both": len(both),
        "ramp_ids_only_in_layer": int((x._merge == "left_only").sum()),
        "ramp_ids_only_in_survey_csv": int((x._merge == "right_only").sum()),
        "survey_date_equal": int(
            (
                pd.to_datetime(both.GeoCyclora, format="%m/%d/%Y").dt.date
                == both.survey_date
            ).sum()
        ),
        "corner_id_equal": int((both.corner == both.CornerID).sum()),
    }
    say("layer versus ufzp-rrqu survey CSV:", survey_check)

    # ---- progress: one row per corner ----------------------------------------------------
    p = pd.read_csv(PROGRESS_CSV, dtype=str, keep_default_na=False)
    raw_ids = p.CornerID
    id_norm = {
        "progress_ids_all_digits": bool(raw_ids.str.fullmatch(r"\d+").all()),
        "progress_ids_with_whitespace": int((raw_ids != raw_ids.str.strip()).sum()),
        "progress_id_lengths": {
            str(k): int(v) for k, v in raw_ids.str.len().value_counts().items()
        },
        "layer_id_type": "integer",
        "layer_id_lengths": {
            str(k): int(v) for k, v in c.corner.str.len().value_counts().items()
        },
        "normalisation": "both sides cast to the decimal string of the integer; no padding or trimming was needed",
    }
    p["corner"] = raw_ids.str.strip().astype("int64").astype(str)
    p = p.rename(
        columns={
            "Construction_Status_Value": "status",
            "Construction_End_Date": "end_raw",
        }
    )
    classes = {
        v: classify_date(v, first_survey, progress_day) for v in p.end_raw.unique()
    }
    p["date_class"] = p.end_raw.map(lambda v: classes[v][0])
    p["end_date"] = p.end_raw.map(lambda v: classes[v][1])
    say(
        "progress corners:",
        len(p),
        "duplicate corner IDs:",
        int(p.corner.duplicated().sum()),
    )
    say("progress status by date class (corners):")
    say(pd.crosstab(p.status, p.date_class).to_string())

    # ---- join on corner ID ---------------------------------------------------------------
    m = c.merge(
        p[["corner", "status", "end_raw", "date_class", "end_date"]],
        on="corner",
        how="left",
        validate="m:1",
    )
    assert len(m) == len(c)
    m["status"] = m.status.fillna("No progress record")
    m["date_class"] = m.date_class.fillna("no_record")
    layer_corners = set(c.corner)
    prog_corners = set(p.corner)
    unmatched_prog = p[~p.corner.isin(layer_corners)]
    join = {
        "key": "corner ID (layer CORNERID, progress CornerID)",
        "id_normalisation": id_norm,
        "layer_ramps": len(c),
        "layer_ramps_with_progress_record": int(
            (m.status != "No progress record").sum()
        ),
        "layer_distinct_corners": len(layer_corners),
        "layer_distinct_corners_with_progress_record": len(
            layer_corners & prog_corners
        ),
        "progress_corners": len(p),
        "progress_corners_with_a_surveyed_ramp": len(prog_corners & layer_corners),
        "progress_corners_without_a_surveyed_ramp_by_status": unmatched_prog.status.value_counts().to_dict(),
        "duplicate_corner_ids_in_progress": int(p.corner.duplicated().sum()),
        "duplicate_ramp_ids_in_layer": int(c.RAMPID.duplicated().sum()),
        "ramps_per_corner_in_layer": {
            str(k): int(v)
            for k, v in c.corner.value_counts().value_counts().sort_index().items()
        },
    }
    join["share_of_ramps_matched"] = round(
        join["layer_ramps_with_progress_record"] / len(c), 6
    )
    join["share_of_layer_corners_matched"] = round(
        join["layer_distinct_corners_with_progress_record"] / len(layer_corners), 6
    )
    join["share_of_progress_corners_matched"] = round(
        join["progress_corners_with_a_surveyed_ramp"] / len(p), 6
    )
    say(
        "join:",
        json.dumps(
            {k: v for k, v in join.items() if not isinstance(v, dict)}, indent=1
        ),
    )

    # ---- corner state per ramp -----------------------------------------------------------
    built = m.status.isin(BUILT)
    usable = m.date_class.isin(["ok", "before_survey_programme"])
    # end_date is a date, None (unusable or blank) or NaN (no progress record)
    pairs = list(zip(m.end_date, m.survey_date))
    after = usable & pd.Series(
        [isinstance(e, date) and e > sd for e, sd in pairs], index=m.index
    )
    same_day = usable & pd.Series(
        [isinstance(e, date) and e == sd for e, sd in pairs], index=m.index
    )
    m["state"] = m.status
    m.loc[built & after, "state"] = "Rebuilt after survey"
    m.loc[built & usable & ~after, "state"] = "Constructed on or before survey date"
    m.loc[built & ~usable, "state"] = "Constructed, date unusable"
    assert not (built & (m.date_class == "blank")).any(), (
        "a built corner with no date needs a rule"
    )
    REBUILT_STATES = ["Rebuilt after survey", "Constructed, date unusable"]
    m["not_rebuilt"] = ~m.state.isin(REBUILT_STATES)
    m["nc"] = m.COMPLIANCY_TOTAL == "Non-Compliant"
    m["backlog"] = m.nc & m.not_rebuilt
    # COMPLIANCY_STATUS is the field DOT draws on its Survey Assessment Map; "Pending" is
    # DOT's "Pending Technical Review". COMPLIANCY_TOTAL is the stricter, unpublished roll-up.
    m["status_nc"] = m.COMPLIANCY_STATUS == "Non-Compliant"
    m["status_pending"] = m.COMPLIANCY_STATUS == "Pending"
    m["backlog_status_field"] = m.status_nc & m.not_rebuilt
    m["backlog_status_pending"] = m.status_pending & m.not_rebuilt
    # Per-district count columns and the flag each one sums. Each also gets a _corners column.
    # The last equals backlog_ramps, repeated under a name parallel to the other two.
    MEASURES = {
        "compliancy_status_noncompliant_not_since_rebuilt": "backlog_status_field",
        "compliancy_status_pending_not_since_rebuilt": "backlog_status_pending",
        "compliancy_total_noncompliant_not_since_rebuilt": "backlog",
    }
    backlog_states = [s for s in m.state.unique() if s not in REBUILT_STATES]

    say("COMPLIANCY_TOTAL:", m.COMPLIANCY_TOTAL.value_counts().to_dict())
    say("COMPLIANCY_STATUS:", m.COMPLIANCY_STATUS.value_counts().to_dict())
    say("ramps by DOT progress status of their corner:")
    by_status = (
        pd.DataFrame(
            {
                "progress_corners": p.status.value_counts(),
                "surveyed_ramps": m.status.value_counts(),
                "noncompliant_ramps": m[m.nc].status.value_counts(),
                "backlog_ramps": m[m.backlog].status.value_counts(),
                "status_field_noncompliant_ramps": m[m.status_nc].status.value_counts(),
                "status_field_noncompliant_not_since_rebuilt": m[
                    m.backlog_status_field
                ].status.value_counts(),
                "status_field_pending_ramps": m[m.status_pending].status.value_counts(),
                "status_field_pending_not_since_rebuilt": m[
                    m.backlog_status_pending
                ].status.value_counts(),
            }
        )
        .fillna(0)
        .astype(int)
    )
    say(by_status.to_string())
    say("ramps by corner state:")
    by_state = (
        pd.DataFrame(
            {
                "surveyed_ramps": m.state.value_counts(),
                "noncompliant_ramps": m[m.nc].state.value_counts(),
                "status_field_noncompliant_ramps": m[m.status_nc].state.value_counts(),
                "status_field_pending_ramps": m[m.status_pending].state.value_counts(),
            }
        )
        .fillna(0)
        .astype(int)
    )
    say(by_state.to_string())

    # ---- malformed dates -----------------------------------------------------------------
    treatment = {
        "unparseable": "not usable; a built corner keeps its status, so it counts as rebuilt",
        "impossible_year": "not usable; a built corner keeps its status, so it counts as rebuilt",
        "future": "not usable; a built corner keeps its status, so it counts as rebuilt",
        "before_survey_programme": "usable; it precedes every survey date, so the corner is not rebuilt since the survey",
    }
    bad = p[p.date_class.isin(treatment)]
    ramps_at = (
        m[m.date_class.isin(treatment)]
        .groupby(["end_raw", "status"])
        .agg(
            surveyed_ramps=("OBJECTID", "size"),
            noncompliant_ramps=("nc", "sum"),
            backlog_ramps=("backlog", "sum"),
        )
    )
    bad_list = (
        bad.groupby(["end_raw", "date_class", "status"])
        .size()
        .rename("corners")
        .reset_index()
        .merge(ramps_at.reset_index(), on=["end_raw", "status"], how="left")
        .fillna(0)
    )
    for k in ["surveyed_ramps", "noncompliant_ramps", "backlog_ramps"]:
        bad_list[k] = bad_list[k].astype(int)
    bad_list["treatment"] = bad_list.date_class.map(treatment)
    bad_list = bad_list.rename(
        columns={"end_raw": "Construction_End_Date"}
    ).sort_values(["date_class", "Construction_End_Date"])
    bad_list.to_csv(HERE / "malformed_dates.csv", index=False)
    date_stats = {
        "first_survey_date": str(first_survey),
        "last_survey_date": str(last_survey),
        "progress_retrieval_date_used_as_future_cutoff": str(progress_day),
        "earliest_plausible_year": EARLIEST_PLAUSIBLE_YEAR,
        "corners_by_date_class": p.date_class.value_counts().to_dict(),
        "distinct_values_by_class": bad.groupby("date_class")
        .end_raw.nunique()
        .to_dict(),
        "ramps_by_date_class": m.date_class.value_counts().to_dict(),
        "noncompliant_ramps_by_date_class": m[m.nc].date_class.value_counts().to_dict(),
        "treatment": treatment,
        "values_unparseable_impossible_or_future": sorted(
            bad[bad.date_class != "before_survey_programme"].end_raw.unique()
        ),
        "built_corners_with_no_date": int(
            (p.status.isin(BUILT) & (p.date_class == "blank")).sum()
        ),
        "unbuilt_status_corners_with_a_date": p[
            ~p.status.isin(BUILT) & (p.date_class != "blank")
        ]
        .status.value_counts()
        .to_dict(),
        "noncompliant_ramps_built_same_day_as_survey": int(
            (m.nc & built & same_day).sum()
        ),
    }
    say("date classes (corners):", date_stats["corners_by_date_class"])
    say(
        "unparseable, impossible or future values:",
        date_stats["values_unparseable_impossible_or_future"],
    )

    # ---- headline and its sensitivity ----------------------------------------------------
    nc = int(m.nc.sum())
    headline = int(m.backlog.sum())
    on_or_before = int(
        (m.nc & (m.state == "Constructed on or before survey date")).sum()
    )
    unusable_built = int((m.nc & (m.state == "Constructed, date unusable")).sum())
    pre_programme_built = int(
        (m.nc & built & (m.date_class == "before_survey_programme")).sum()
    )
    unbuilt_dated_after = int((m.backlog & ~built & after).sum())
    no_record = int((m.backlog & (m.status == "No progress record")).sum())
    headlines = {
        "surveyed_ramps": len(m),
        "noncompliant_ramps": nc,
        "compliant_ramps": int(m.COMPLIANCY_TOTAL.str.startswith("Compliant").sum()),
        "tbd_ramps": int((m.COMPLIANCY_TOTAL == "TBD").sum()),
        "backlog_noncompliant_at_corners_not_since_rebuilt": headline,
        "backlog_share_of_noncompliant": round(headline / nc, 4),
        "backlog_share_of_surveyed": round(headline / len(m), 4),
        "noncompliant_at_corners_rebuilt_after_survey": int(
            (m.nc & (m.state == "Rebuilt after survey")).sum()
        ),
        "noncompliant_at_built_corners_with_unusable_date": unusable_built,
        "backlog_by_corner_state": m[m.backlog].state.value_counts().to_dict(),
        "backlog_by_borough": {
            BOROUGHS[k]: int(v)
            for k, v in m[m.backlog].BOROUGH.value_counts().sort_index().items()
        },
        "backlog_distinct_corners": int(m[m.backlog].corner.nunique()),
    }
    for name, col in [
        ("running_slope", "RAMP_RUNNING_SLOPE_CHECK"),
        ("cross_slope", "RAMP_CROSS_SLOPE_CHECK"),
        ("counter_slope", "COUNTER_SLOPE_CHECK"),
    ]:
        m[f"fail_{name}"] = m[col] == "Non-Compliant"
        headlines[f"{name}_check_noncompliant"] = int(m[f"fail_{name}"].sum())
        headlines[f"{name}_check_noncompliant_at_corners_not_since_rebuilt"] = int(
            (m[f"fail_{name}"] & m.not_rebuilt).sum()
        )
    headlines["compliancy_status_field_noncompliant"] = int(m.status_nc.sum())
    headlines["compliancy_status_field_noncompliant_at_corners_not_since_rebuilt"] = (
        int(m.backlog_status_field.sum())
    )
    headlines["compliancy_status_field_pending"] = int(m.status_pending.sum())
    headlines["compliancy_status_field_pending_at_corners_not_since_rebuilt"] = int(
        m.backlog_status_pending.sum()
    )
    headlines["surveyed_ramps_at_corners_not_since_rebuilt"] = int(m.not_rebuilt.sum())
    # A corner with ramps in two classes is in both corner counts, so these do not add up.
    either = m.backlog_status_field | m.backlog_status_pending
    headlines["distinct_corners_not_since_rebuilt"] = {
        **{name: int(m[m[flag]].corner.nunique()) for name, flag in MEASURES.items()},
        "compliancy_status_noncompliant_or_pending_not_since_rebuilt": int(
            m[either].corner.nunique()
        ),
        "any_surveyed_ramp_not_since_rebuilt": int(m[m.not_rebuilt].corner.nunique()),
    }
    headlines["compliancy_status_noncompliant_or_pending_not_since_rebuilt"] = int(
        either.sum()
    )
    headlines["percent_of_surveyed_ramps"] = {
        "compliancy_status_noncompliant": round(100 * m.status_nc.mean(), 1),
        "compliancy_status_pending": round(100 * m.status_pending.mean(), 1),
        "compliancy_total_noncompliant": round(100 * m.nc.mean(), 1),
    }
    headlines["not_since_rebuilt_compliancy_total_by_compliancy_status"] = {
        t: g.COMPLIANCY_STATUS.value_counts().to_dict()
        for t, g in m[m.not_rebuilt].groupby("COMPLIANCY_TOTAL")
    }
    headlines["compliancy_total_by_compliancy_status"] = {
        t: g.COMPLIANCY_STATUS.value_counts().to_dict()
        for t, g in m.groupby("COMPLIANCY_TOTAL")
    }
    date_dependence = {
        "backlog_ramps_in_only_because_construction_date_is_on_or_before_survey_date": on_or_before,
        "share_of_backlog_that_depends_on_the_date_comparison": round(
            on_or_before / headline, 4
        ),
        "noncompliant_ramps_at_built_corners": int((m.nc & built).sum()),
        "of_which_date_after_survey": int((m.nc & built & after).sum()),
        "of_which_date_on_or_before_survey": on_or_before,
        "of_which_date_unusable": unusable_built,
    }
    sensitivity = {
        "headline": headline,
        "status_only_ignoring_all_dates": headline - on_or_before,
        "unusable_dates_at_built_corners_counted_as_not_rebuilt": headline
        + unusable_built,
        "pre_programme_dates_at_built_corners_counted_as_unusable_so_rebuilt_on_status": headline
        - pre_programme_built,
        "unbuilt_status_corners_with_a_date_after_survey_counted_as_rebuilt": headline
        - unbuilt_dated_after,
        "ramps_with_no_progress_record_left_out": headline - no_record,
    }
    # Progress is per corner: a rebuilt corner excludes every ramp surveyed at it, though the
    # public file does not say how many of them were rebuilt.
    at_corner = m.groupby("corner").OBJECTID.transform("size")
    rebuilt = ~m.not_rebuilt
    corner_limits = {
        "surveyed_ramps_at_corners_rebuilt_after_survey": int(rebuilt.sum()),
        "of_which_at_corners_with_more_than_one_surveyed_ramp": int(
            (rebuilt & (at_corner > 1)).sum()
        ),
        "corners_rebuilt_after_survey": int(m[rebuilt].corner.nunique()),
        "of_which_with_more_than_one_surveyed_ramp": int(
            m[rebuilt & (at_corner > 1)].corner.nunique()
        ),
        "corners_with_some_ramps_rebuilt_after_survey_and_some_not": int(
            len(set(m[rebuilt].corner) & set(m[m.not_rebuilt].corner))
        ),
    }
    say("headlines:", json.dumps(headlines, indent=1))
    say("per-corner limits:", json.dumps(corner_limits, indent=1))
    say("date dependence:", json.dumps(date_dependence, indent=1))
    say("sensitivity:", json.dumps(sensitivity, indent=1))

    # ---- districts -----------------------------------------------------------------------
    pts = gpd.GeoDataFrame(
        m[["OBJECTID"]], geometry=gpd.points_from_xy(m.lon, m.lat), crs=4326
    )
    m = m.set_index("OBJECTID")
    m["council_district"], info_cc = assign_district(
        pts, RAW / "council_districts_872g-cjhh.geojson", "coundist"
    )
    m["community_district"], info_cd = assign_district(
        pts, RAW / "community_districts_5crt-au7u.geojson", "boro_cd"
    )
    info_cd["ramps_where_dot_borough_differs_from_district_borough"] = int(
        (m.community_district // 100 != m.BOROUGH).sum()
    )
    say("council district assignment:", info_cc)
    say("community district assignment:", info_cd)

    def slug(s):
        return s.lower().replace(",", "").replace(" ", "_")

    def tabulate(key):
        g = m.groupby(key)
        t = pd.DataFrame(
            {
                "surveyed_ramps": g.size(),
                "noncompliant_ramps": g.nc.sum(),
                "backlog_ramps": g.backlog.sum(),
            }
        )
        for st in sorted(backlog_states):
            t[f"backlog_{slug(st)}"] = (
                (m.backlog & (m.state == st)).groupby(m[key]).sum()
            )
        t["noncompliant_rebuilt_after_survey"] = (
            (m.nc & (m.state == "Rebuilt after survey")).groupby(m[key]).sum()
        )
        t["noncompliant_constructed_date_unusable"] = (
            (m.nc & (m.state == "Constructed, date unusable")).groupby(m[key]).sum()
        )
        for name in ["running_slope", "cross_slope", "counter_slope"]:
            t[f"{name}_check_noncompliant"] = g[f"fail_{name}"].sum()
            t[f"{name}_check_noncompliant_not_since_rebuilt"] = (
                (m[f"fail_{name}"] & m.not_rebuilt).groupby(m[key]).sum()
            )
        for name, flag in MEASURES.items():
            t[name] = g[flag].sum()
            t[f"{name}_corners"] = m[m[flag]].groupby(key).corner.nunique()
        t = t.fillna(0).astype(int)  # a district with no such ramp has no corner count
        assert (
            t.compliancy_total_noncompliant_not_since_rebuilt == t.backlog_ramps
        ).all()
        state_cols = [f"backlog_{slug(st)}" for st in backlog_states]
        assert (t[state_cols].sum(axis=1) == t.backlog_ramps).all()
        return t

    m["all"] = "CITYWIDE"
    total = tabulate("all")
    assert int(total.backlog_ramps.iloc[0]) == headline

    cc = tabulate("council_district")

    # A council district can cross a borough line. The borough is DOT's BOROUGH field for the
    # district's ramps; where more than one occurs, each is listed with its ramp count.
    def boroughs(b):
        n = b.value_counts()
        return (
            BOROUGHS[n.index[0]]
            if len(n) == 1
            else "; ".join(f"{BOROUGHS[k]} {v}" for k, v in n.items())
        )

    boro = m.groupby("council_district").BOROUGH.agg(boroughs)
    cc.insert(0, "borough", boro)
    cd = tabulate("community_district")
    cd.insert(0, "borough", [BOROUGHS[k // 100] for k in cd.index])
    cd.insert(
        1,
        "district_type",
        [
            "community district" if k % 100 <= 18 else "joint interest area"
            for k in cd.index
        ],
    )
    # A corner whose ramps fall in two districts is counted in each, so the corner columns can
    # sum to more than the CITYWIDE row, which holds the citywide distinct count.
    corner_cols = [f"{name}_corners" for name in MEASURES]
    top_ten = {}
    for t, name in [(cc, "council"), (cd, "community")]:
        sums = t.drop(columns=[x for x in ("borough", "district_type") if x in t]).sum()
        assert (sums.drop(corner_cols) == total.iloc[0].drop(corner_cols)).all()
        extra = sums[corner_cols] - total.iloc[0][corner_cols]
        assert (extra >= 0).all()
        say(
            f"{name} districts: corners counted in more than one district:",
            extra.to_dict(),
        )
        top_ten[name] = {
            measure: [
                {
                    f"{name}_district": int(k),
                    "borough": r.borough,
                    "ramps": int(r[measure]),
                    "corners": int(r[f"{measure}_corners"]),
                    "surveyed_ramps": int(r.surveyed_ramps),
                }
                for k, r in t.sort_values(measure, ascending=False, kind="stable")
                .head(10)
                .iterrows()
            ]
            for measure in MEASURES
        }
        out = pd.concat([t, total.assign(borough="All")]).fillna("")
        out.index.name = f"{name}_district"
        out.to_csv(HERE / f"backlog_by_{name}_district.csv")
        say(
            f"wrote backlog_by_{name}_district.csv: {len(t)} districts plus a CITYWIDE row"
        )
        say(t.backlog_ramps.sort_values(ascending=False).head(5).to_string())

    # ---- comparison with the earlier join ------------------------------------------------
    old = json.loads(Path(EARLIER_JOIN).read_text())
    mine_status = m.status.replace("No progress record", "NaN").value_counts().to_dict()
    comparison = {
        "earlier_file": "research_notes/evidence/ramps/program_progress_join.json",
        "ramps_by_corner_status_identical": mine_status
        == old["ramps_by_corner_status"],
        "ramps_by_corner_status_here": mine_status,
        "ramps_at_built_corners": {
            "earlier": old["ramps_at_corner_constructed"],
            "here": int(built.sum()),
        },
        "ramps_at_built_corners_with_date_after_survey": {
            "earlier": old["ramps_at_corner_constructed_after_their_survey_date"],
            "here": int((built & after).sum()),
        },
        "earlier_test": "running slope over 1:12 on the raw measurement (this project's threshold)",
        "earlier_over_1_12": old["ramps_over_1:12"],
        "earlier_over_1_12_not_yet_built": old["of_which_corner_not_yet_built"],
        "nearest_equivalent_here": "DOT's RAMP_RUNNING_SLOPE_CHECK",
        "here_running_slope_check_noncompliant": headlines[
            "running_slope_check_noncompliant"
        ],
        "here_running_slope_check_noncompliant_not_since_rebuilt": headlines[
            "running_slope_check_noncompliant_at_corners_not_since_rebuilt"
        ],
    }
    say("comparison with earlier join:", json.dumps(comparison, indent=1))

    summary = {
        "caveat": CAVEAT,
        "definitions": {
            "surveyed_ramp": "one row of the DOT compliance layer",
            "noncompliant": "COMPLIANCY_TOTAL == 'Non-Compliant' in the DOT compliance layer",
            "corner_rebuilt_since_survey": (
                "the corner's Construction_Status_Value in e7gc-ub6z is 'Constructed' or 'Complex Constructed' "
                "and its Construction_End_Date is later than the ramp's survey date (GEOCYCLORAMA_DATE); "
                "a built corner whose date is unusable counts as rebuilt on its status alone"
            ),
            "backlog": "noncompliant ramps whose corner is not rebuilt since survey, including ramps with no progress record",
            "published_field": (
                "COMPLIANCY_STATUS is the field DOT draws on its Survey Assessment Map (Compliant, "
                "Non-Compliant, Pending = 'Pending Technical Review'); COMPLIANCY_TOTAL is an unpublished "
                "roll-up with no public description. Keys named compliancy_status_* use the published field; "
                "noncompliant_* and backlog_* keys with no field in the name use COMPLIANCY_TOTAL"
            ),
        },
        "headlines": headlines,
        "per_corner_limits": corner_limits,
        "status_counts": by_status.to_dict("index"),
        "corner_state_counts": by_state.to_dict("index"),
        "date_dependence": date_dependence,
        "headline_under_other_treatments": sensitivity,
        "join": join,
        "malformed_dates": date_stats,
        "layer_versus_survey_csv": survey_check,
        "district_assignment": {
            "method": "point in polygon on the ramp location from the compliance layer; neither DOT source has district fields",
            "council": info_cc,
            "community": info_cd,
        },
        "comparison_with_earlier_join": comparison,
        "sources": {
            "compliance_layer": {
                "url": retrieved["compliance_layer"]["url"],
                "retrieved": retrieved["compliance_layer"],
                "data_last_edited": datetime.fromtimestamp(
                    json.loads(Path(RAW / "compliance_layer0.json").read_text())[
                        "editingInfo"
                    ]["dataLastEditDate"]
                    / 1000,
                    UTC,
                ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            "program_progress": {
                "url": "https://data.cityofnewyork.us/d/e7gc-ub6z",
                "retrieved": progress_retrieved,
                "file": "research_notes/evidence/raw/e7gc-ub6z_rows.csv",
            },
            "ramp_survey": {
                "url": "https://data.cityofnewyork.us/d/ufzp-rrqu",
                "retrieved": (EVIDENCE / "raw" / "ufzp-rrqu_rows.retrieved")
                .read_text()
                .strip(),
                "file": "research_notes/evidence/raw/ufzp-rrqu_rows.csv",
                "use": "cross-check of survey date and corner ID only",
            },
            "council_districts": retrieved["council_districts"],
            "community_districts": retrieved["community_districts"],
        },
    }
    (HERE / "backlog_summary.json").write_text(
        json.dumps(summary, indent=1, default=int) + "\n"
    )
    csvs = "backlog_by_council_district.csv and backlog_by_community_district.csv"
    result = {
        "status": "not published; a working table built from NYC DOT public data",
        "caveat": CAVEAT,
        "surveyed_ramps": {
            "value": len(m),
            "source": f"{csvs}, CITYWIDE row, surveyed_ramps; backlog_summary.json headlines.surveyed_ramps",
        },
        "headlines_ramps_at_corners_not_shown_as_rebuilt_since_survey": {
            measure: {
                "ramps": int(total[measure].iloc[0]),
                "corners": int(total[f"{measure}_corners"].iloc[0]),
                "source": (
                    f"{csvs}, CITYWIDE row, columns {measure} and {measure}_corners; "
                    "backlog_summary.json headlines; run.log"
                ),
            }
            for measure in MEASURES
        },
        "lead_with": "compliancy_status_noncompliant_not_since_rebuilt (the field DOT publishes)",
        "top_ten_council_districts": {
            "source": "backlog_by_council_district.csv, ranked by each measure's ramp column",
            **top_ten["council"],
        },
        "top_ten_community_districts": {
            "source": "backlog_by_community_district.csv, ranked by each measure's ramp column",
            **top_ten["community"],
        },
    }
    (HERE / "result.json").write_text(json.dumps(result, indent=1) + "\n")
    say("wrote backlog_summary.json, malformed_dates.csv, result.json")


if __name__ == "__main__":
    main()
