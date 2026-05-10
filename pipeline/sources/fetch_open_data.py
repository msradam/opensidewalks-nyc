"""
NYC Open Data pipeline for Ariadne.

Fetches two files consumed by the web app:

  data/nyc-comfort.json  . Public resource locations (Point GeoJSON)
  data/nyc-pois.json        . Named places for fuzzy-match geocoding (flat JSON)

─────────────────────────────────────────────────────────────────────────────
COMFORT RESOURCE SOURCES
─────────────────────────────────────────────────────────────────────────────
Source              Dataset ID    Approx.   Resource types
─────────────────── ────────────  ────────  ──────────────────────────────────
NYPL Refinery       (direct API)    ~91     cool_indoor warm_indoor quiet_indoor
                                            wifi_power bathroom seating
Brooklyn Pub Lib    (vendor JSON)   ~60     same as NYPL (best-effort; may 403)
Queens Library      kh3d-xhq7        68     same as NYPL; has per-day hours
NYCHA facilities    crns-fw6u       416     cool_indoor warm_indoor seating;
                                            senior_center or community_center
                                            depending on program_type
DHS drop-in ctr     bmxf-3rd4         8     cool_indoor warm_indoor bathroom
                                            seating shelter_24h (if 24h noted)
Parks indoor pools  y5rm-wagw        13     pool_indoor cool_indoor warm_indoor
                                            bathroom; amenities: showers
Public restrooms    i7jb-7jku       975     bathroom; has hours_of_operation
LinkNYC kiosks      n6c5-95xh     2,238     linknyc wifi_power; 24/7; live status
Food pantries       ji82-xba5       560     food_pantry
Senior centers      cqc8-am9x       312     senior_center cool_indoor warm_indoor
                                            quiet_indoor seating; per-day hours
Harm reduction      nk7g-qeep       506     harm_reduction; per-day hours
NYC H+H hospitals   q6fj-vxf8        78     medical (+ cool/warm for acute care)
Mental health       ji82-xba5     1,254     mental_health
Community centers   ji82-xba5       201     community_center cool_indoor warm_indoor

─────────────────────────────────────────────────────────────────────────────
GEOCODER POI SOURCES
─────────────────────────────────────────────────────────────────────────────
Source              Method        Approx.   Description
─────────────────── ────────────  ────────  ──────────────────────────────────
OpenStreetMap       Overpass API  ~23,000   Named places, transit stations,
                                            parks, amenities, buildings

─────────────────────────────────────────────────────────────────────────────
OUTPUTS
─────────────────────────────────────────────────────────────────────────────
  data/nyc-comfort.json   GeoJSON FeatureCollection, Point geometry,
                             properties: id source name address resource_types
                             amenities hours_today is_temporarily_closed borough

  data/nyc-pois.json         JSON list, each item:
                             {name, lat, lng, type, category}
                             sorted by category priority for fuzzy-match ranking

  data/fetch_summary.json    Machine-readable run report:
                             timestamp, per-source counts, totals, errors

─────────────────────────────────────────────────────────────────────────────
USAGE
─────────────────────────────────────────────────────────────────────────────
  python fetch_open_data.py                       # fetch everything
  python fetch_open_data.py --section comfort     # comfort resources only
  python fetch_open_data.py --section pois        # geocoder POIs only
  python fetch_open_data.py --sources linknyc,food_pantries  # specific sources
  python fetch_open_data.py --dry-run             # print plan, no network calls
  python fetch_open_data.py --output-dir /tmp/out # write to a different directory

─────────────────────────────────────────────────────────────────────────────
ATTRIBUTIONS
─────────────────────────────────────────────────────────────────────────────
  NYC Open Data (Socrata) . cityofnewyork.us open data portal, public domain
  NYPL Refinery API       . data.nypl.org, CC BY 2.0
  OpenStreetMap           . openstreetmap.org, ODbL 1.0
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any

import requests

# ── Paths ──────────────────────────────────────────────────────────────────
# Resolve to the repo root: pipeline/sources/fetch_open_data.py → repo/
BASE       = pathlib.Path(__file__).resolve().parent.parent.parent
DATA_DIR   = BASE / "data"
COMFORT_OUT = DATA_DIR / "nyc-comfort.json"
POIS_OUT    = DATA_DIR / "nyc-pois.json"
SUMMARY_OUT = DATA_DIR / "fetch_summary.json"

# ── Shared constants ────────────────────────────────────────────────────────
UA       = {"User-Agent": "opensidewalks-nyc/0.3 (research; github.com/msradam/opensidewalks-nyc)"}
NYC_BBOX = (40.4774, 40.9176, -74.2591, -73.7004)   # (min_lat, max_lat, min_lng, max_lng)

BORO_FROM_LONG  = {
    "manhattan": "MN", "brooklyn": "BK", "queens": "QN",
    "bronx": "BX", "staten island": "SI",
}
BORO_FROM_SHORT = {"MN": "MN", "BK": "BK", "QN": "QN", "BX": "BX", "SI": "SI"}


# ── Shared helpers ──────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr)


def _in_nyc(lat: float, lng: float) -> bool:
    return NYC_BBOX[0] <= lat <= NYC_BBOX[1] and NYC_BBOX[2] <= lng <= NYC_BBOX[3]


def _coords(row: dict,
            lat_keys: tuple[str, ...] = ("latitude",),
            lng_keys: tuple[str, ...] = ("longitude",)) -> tuple[float, float] | None:
    """Extract (lat, lng) from a Socrata row, falling back to GeoJSON location fields."""
    lat = lng = None
    for k in lat_keys:
        if row.get(k) is not None:
            try: lat = float(row[k]); break
            except (TypeError, ValueError): pass
    for k in lng_keys:
        if row.get(k) is not None:
            try: lng = float(row[k]); break
            except (TypeError, ValueError): pass
    # GeoJSON location column fallback
    if lat is None or lng is None:
        for loc_key in ("location", "location_1", "geocoded_column"):
            loc = row.get(loc_key)
            if isinstance(loc, dict):
                coords = loc.get("coordinates")
                if coords and len(coords) == 2:
                    try: lng, lat = float(coords[0]), float(coords[1]); break
                    except (TypeError, ValueError): pass
    if lat is None or lng is None:
        return None
    if not _in_nyc(lat, lng):
        return None
    return lat, lng


def _socrata(dataset_id: str, params: dict, label: str) -> list[dict]:
    """Fetch up to 5,000 rows from a NYC Open Data Socrata dataset."""
    url = f"https://data.cityofnewyork.us/resource/{dataset_id}.json"
    r = requests.get(url, params={"$limit": 5000, **params}, timeout=90, headers=UA)
    r.raise_for_status()
    rows: list[dict] = r.json()
    _log(f"  {label}: {len(rows)} rows ({dataset_id})")
    return rows


def _polygon_centroid(geojson_polygon: dict) -> tuple[float, float] | None:
    """Mean of exterior ring. Sufficient accuracy for NYC pool polygons."""
    try:
        ring = geojson_polygon["coordinates"][0]
        lngs = [p[0] for p in ring]
        lats = [p[1] for p in ring]
        return sum(lats) / len(lats), sum(lngs) / len(lngs)
    except (KeyError, IndexError, ZeroDivisionError, TypeError):
        return None


def _feat(source_id: str, source_label: str, lat: float, lng: float,
          name: str, address: str, resource_types: list[str],
          amenities: list[str] | None = None, hours: Any = "unknown",
          closed: bool = False, borough: str = "?",
          extra: dict | None = None) -> dict:
    """Build a canonical comfort Feature dict."""
    props: dict[str, Any] = {
        "id": source_id,
        "source": source_label,
        "name": name,
        "address": address,
        "resource_types": resource_types,
        "amenities": amenities or [],
        "hours_today": hours,
        "is_temporarily_closed": closed,
        "borough": borough,
    }
    if extra:
        props.update(extra)
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lng, lat]},
        "properties": props,
    }


# ═══════════════════════════════════════════════════════════════════════════
# COMFORT RESOURCE FETCH FUNCTIONS
# Each returns list[dict] of GeoJSON Features.
# On network/parse error: log and return [] (never raise).
# ═══════════════════════════════════════════════════════════════════════════

def fetch_nypl() -> list[dict]:
    """NYPL Refinery API. Manhattan, Bronx, Staten Island branches.
    Endpoint: https://refinery.nypl.org/api/nypl/locations/v1.0/locations
    Structured hours, amenities list, live is_temporarily_closed flag.
    """
    _log("NYPL Refinery ...")
    r = requests.get("https://refinery.nypl.org/api/nypl/locations/v1.0/locations",
                     timeout=60, headers=UA)
    r.raise_for_status()
    feats: list[dict] = []
    for loc in r.json().get("locations", []):
        geo = loc.get("geolocation") or {}
        c = geo.get("coordinates") if isinstance(geo, dict) else None
        if c and len(c) == 2:
            lng, lat = float(c[0]), float(c[1])
        elif isinstance(geo, dict) and "latitude" in geo:
            lat, lng = float(geo["latitude"]), float(geo["longitude"])
        else:
            continue
        if not _in_nyc(lat, lng): continue
        region = loc.get("region", "")
        borough = region if region in BORO_FROM_SHORT else "MN"
        amenities = [a["name"] for a in (loc.get("amenities") or []) if a.get("name")]
        feats.append(_feat(
            source_id=f"nypl:{loc.get('slug') or loc.get('id')}",
            source_label="NYPL",
            lat=lat, lng=lng,
            name=loc.get("name") or "NYPL branch",
            address=loc.get("street_address") or "",
            resource_types=["cool_indoor", "warm_indoor", "quiet_indoor", "wifi_power", "bathroom", "seating"],
            amenities=amenities,
            hours=loc.get("hours_today") or {},
            closed=bool(loc.get("is_temporarily_closed")),
            borough=borough,
        ))
    _log(f"  → {len(feats)} NYPL branches")
    return feats


def fetch_bpl() -> list[dict]:
    """Brooklyn Public Library. Vendor JSON endpoint (best-effort; may 403).
    Endpoint: https://www.bklynlibrary.org/api/locations/v1/map
    """
    _log("Brooklyn Public Library ...")
    try:
        r = requests.get("https://www.bklynlibrary.org/api/locations/v1/map",
                         timeout=30, headers={**UA, "Referer": "https://www.bklynlibrary.org/locations"})
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        _log(f"  BPL skipped: {e}")
        return []
    rows = payload if isinstance(payload, list) else (
        payload.get("branches") or payload.get("locations") or [])
    feats: list[dict] = []
    for b in rows:
        lat = b.get("lat") or b.get("latitude")
        lng = b.get("lng") or b.get("longitude")
        if lat is None or lng is None: continue
        try: lat_f, lng_f = float(lat), float(lng)
        except (TypeError, ValueError): continue
        if not _in_nyc(lat_f, lng_f): continue
        slug = (b.get("branch_id") or b.get("id") or b.get("slug")
                or (b.get("name") or "").lower().replace(" ", "-"))
        feats.append(_feat(
            source_id=f"bpl:{slug}",
            source_label="BPL",
            lat=lat_f, lng=lng_f,
            name=b.get("name") or "BPL branch",
            address=b.get("street_address") or b.get("address") or "",
            resource_types=["cool_indoor", "warm_indoor", "quiet_indoor", "wifi_power", "bathroom", "seating"],
            hours=b.get("hours_today") or "unknown",
            borough="BK",
        ))
    _log(f"  → {len(feats)} BPL branches")
    return feats


def fetch_queens_library() -> list[dict]:
    """Queens Public Library. Socrata kh3d-xhq7.
    Has per-day open hours (mn/tu/we/th/fr/sa/su fields).
    """
    _log("Queens Library (kh3d-xhq7) ...")
    rows = _socrata("kh3d-xhq7", {}, "Queens Library")
    DAY_MAP = [("Mon","mn"),("Tue","tu"),("Wed","we"),("Thu","th"),
               ("Fri","fr"),("Sat","sa"),("Sun","su")]
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        hours = {d: row[k] for d, k in DAY_MAP if row.get(k)}
        feats.append(_feat(
            source_id=f"qpl:{row.get('id') or (row.get('name') or '').lower().replace(' ', '-')}",
            source_label="Queens Library",
            lat=lat_f, lng=lng_f,
            name=row.get("name") or "Queens Library branch",
            address=f"{row.get('address', '')} {row.get('city', '')}".strip(),
            resource_types=["cool_indoor", "warm_indoor", "quiet_indoor", "wifi_power", "bathroom", "seating"],
            hours=hours or "unknown",
            borough="QN",
        ))
    _log(f"  → {len(feats)} Queens Library branches")
    return feats


def fetch_nycha() -> list[dict]:
    """NYCHA Community Facilities. Socrata crns-fw6u.
    Assigns senior_center or community_center based on program_type.
    """
    _log("NYCHA community facilities (crns-fw6u) ...")
    rows = _socrata("crns-fw6u",
                    {"$where": "borough in ('Manhattan','Brooklyn','Queens','Bronx','Staten Island')"},
                    "NYCHA")
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        program = (row.get("program_type") or "").strip().lower()
        types = ["cool_indoor", "warm_indoor", "seating"]
        if "senior" in program:
            types += ["senior_center", "quiet_indoor"]
        else:
            types += ["community_center"]
        slug = ((row.get("sponsor") or row.get("development") or "")
                .lower().replace(" ", "-"))
        feats.append(_feat(
            source_id=f"nycha:{slug}-{lat_f:.4f}",
            source_label="NYCHA",
            lat=lat_f, lng=lng_f,
            name=row.get("sponsor") or row.get("development") or "NYCHA facility",
            address=row.get("address") or "",
            resource_types=types,
            hours="unknown",
            closed=(row.get("status") or "").strip().lower() == "vacant",
            borough=BORO_FROM_LONG.get((row.get("borough") or "").lower(), "?"),
            extra={"program_type": (row.get("program_type") or "").strip()},
        ))
    _log(f"  → {len(feats)} NYCHA facilities")
    return feats


def fetch_dhs_dropins() -> list[dict]:
    """DHS drop-in centers. Socrata bmxf-3rd4.
    Provides showers and meals; 24h status detected from comments field.
    """
    _log("DHS drop-in centers (bmxf-3rd4) ...")
    rows = _socrata("bmxf-3rd4", {}, "DHS drop-ins")
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        comments = row.get("comments") or ""
        types = ["cool_indoor", "warm_indoor", "bathroom", "seating"]
        if "24" in comments.lower():
            types.append("shelter_24h")
        feats.append(_feat(
            source_id=f"dhs:{(row.get('center_name') or '').lower().replace(' ', '-')}",
            source_label="DHS",
            lat=lat_f, lng=lng_f,
            name=row.get("center_name") or "Drop-in center",
            address=row.get("address") or "",
            resource_types=types,
            amenities=["showers", "meals"],
            hours=comments[:120],
            borough=BORO_FROM_LONG.get((row.get("borough") or "").lower(), "?"),
        ))
    _log(f"  → {len(feats)} DHS drop-in centers")
    return feats


def fetch_parks_pools() -> list[dict]:
    """NYC Parks indoor pools. Socrata y5rm-wagw.
    Polygon geometry; centroid computed by coordinate averaging.
    """
    _log("Parks indoor pools (y5rm-wagw) ...")
    rows = _socrata("y5rm-wagw", {"$where": "location='Indoor'"}, "Parks pools")
    BORO_CODE = {"M": "MN", "B": "BK", "Q": "QN", "X": "BX", "R": "SI"}
    feats: list[dict] = []
    for row in rows:
        geom_key = next((k for k in ("polygon", "the_geom", "geom") if k in row), None)
        if not geom_key: continue
        centroid = _polygon_centroid(row[geom_key])
        if not centroid: continue
        lat_f, lng_f = centroid
        if not _in_nyc(lat_f, lng_f): continue
        feats.append(_feat(
            source_id=f"pool:{row.get('gispropnum') or (row.get('name') or '').lower().replace(' ', '-')}",
            source_label="NYC Parks",
            lat=lat_f, lng=lng_f,
            name=(row.get("name") or "Parks pool") + " (indoor)",
            address="",
            resource_types=["cool_indoor", "warm_indoor", "pool_indoor", "bathroom"],
            amenities=["locker_rooms", "showers"],
            hours="check nycgovparks.org",
            borough=BORO_CODE.get((row.get("borough") or "").upper(), "?"),
        ))
    _log(f"  → {len(feats)} indoor pools")
    return feats


def fetch_restrooms() -> list[dict]:
    """Public restrooms. Socrata i7jb-7jku.
    Filtered to status='Operational'. Includes hours_of_operation and ADA notes.
    """
    _log("Public restrooms (i7jb-7jku) ...")
    rows = _socrata("i7jb-7jku", {"$where": "status='Operational'"}, "Public Restrooms")
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        feats.append(_feat(
            source_id=f"restroom:{(row.get('facility_name') or '').lower().replace(' ', '-')}-{lat_f:.4f}",
            source_label="NYC Public Restrooms",
            lat=lat_f, lng=lng_f,
            name=row.get("facility_name") or "Public restroom",
            address=row.get("location_type") or "",
            resource_types=["bathroom"],
            hours=row.get("hours_of_operation") or "unknown",
            extra={"accessibility": row.get("accessibility") or ""},
        ))
    _log(f"  → {len(feats)} restrooms")
    return feats


def fetch_linknyc() -> list[dict]:
    """LinkNYC kiosks. Socrata n6c5-95xh.
    Free wifi, phone calls, tablet, and USB charging. Filtered to wifi_status='up'.
    Live status updated daily by the city.
    """
    _log("LinkNYC kiosks (n6c5-95xh) ...")
    rows = _socrata("n6c5-95xh", {"$where": "wifi_status='up'"}, "LinkNYC")
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        addr = row.get("address") or ""
        if not addr:
            c1, c2 = row.get("cross_street_1") or "", row.get("cross_street_2") or ""
            addr = f"{c1} & {c2}".strip(" &") if c1 else ""
        amenities = ["free_wifi"]
        if (row.get("tablet_status") or "").lower() == "operational":
            amenities.append("tablet")
        if (row.get("phone_status") or "").lower() == "operational":
            amenities.append("free_phone")
        feats.append(_feat(
            source_id=f"linknyc:{row.get('site_id') or f'{lat_f:.5f}'}",
            source_label="LinkNYC",
            lat=lat_f, lng=lng_f,
            name="LinkNYC Kiosk",
            address=addr,
            resource_types=["linknyc", "wifi_power"],
            amenities=amenities,
            hours="24/7",
            borough=BORO_FROM_SHORT.get((row.get("boro") or "").upper(), "?"),
        ))
    _log(f"  → {len(feats)} LinkNYC kiosks")
    return feats


def fetch_food_pantries() -> list[dict]:
    """Food pantries and soup kitchens. NYC Facilities Database (ji82-xba5).
    Filter: facsubgrp = 'SOUP KITCHENS AND FOOD PANTRIES' (~600 records).
    No hours data available in this source; advise calling ahead.
    """
    _log("Food pantries (ji82-xba5) ...")
    rows = _socrata("ji82-xba5",
                    {"$where": "facsubgrp='SOUP KITCHENS AND FOOD PANTRIES'"},
                    "Facilities DB / food")
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        uid = row.get("uid") or (row.get("facname") or "").lower().replace(" ", "-")
        feats.append(_feat(
            source_id=f"food:{uid}-{lat_f:.4f}",
            source_label="NYC Facilities",
            lat=lat_f, lng=lng_f,
            name=row.get("facname") or "Food pantry",
            address=f"{row.get('address', '')} {row.get('zipcode', '')}".strip(),
            resource_types=["food_pantry"],
            hours="call ahead",
            borough=BORO_FROM_SHORT.get((row.get("boro") or "").upper(), "?"),
        ))
    _log(f"  → {len(feats)} food pantries / soup kitchens")
    return feats


def fetch_senior_centers() -> list[dict]:
    """Senior (Older Adult) centers. NYC Aging, Socrata cqc8-am9x.
    Filter: providertype = 'OLDER ADULT CENTER CONTRACTS'.
    Has structured per-day open/close hours (monhouropen, monhourclose, etc.).
    """
    _log("Senior centers (cqc8-am9x) ...")
    rows = _socrata("cqc8-am9x",
                    {"$where": "providertype='OLDER ADULT CENTER CONTRACTS'"},
                    "NYC Aging / senior centers")
    DAY_MAP = [
        ("Mon", "monhouropen",   "monhourclose"),
        ("Tue", "tuehouropen",   "tuehourclose"),
        ("Wed", "wedhoursopen",  "wedhoursclose"),
        ("Thu", "thurhoursopen", "thurhoursclose"),
        ("Fri", "frihouropen",   "frihourclose"),
        ("Sat", "satopentime",   "satclosetime"),
        ("Sun", "sunhouropen",   "sunhourclose"),
    ]
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        hours = {d: f"{row[o]}-{row[c]}" for d, o, c in DAY_MAP
                 if row.get(o) and row.get(c)}
        uid = row.get("bin") or (row.get("programname") or "").lower().replace(" ", "-")
        feats.append(_feat(
            source_id=f"senior:{uid}-{lat_f:.4f}",
            source_label="NYC Aging",
            lat=lat_f, lng=lng_f,
            name=row.get("programname") or row.get("sponsorname") or "Senior center",
            address=row.get("programaddress") or "",
            resource_types=["senior_center", "cool_indoor", "warm_indoor", "quiet_indoor", "seating"],
            amenities=["meals"],
            hours=hours or "call ahead",
            borough=BORO_FROM_LONG.get((row.get("borough") or "").lower(), "?"),
        ))
    _log(f"  → {len(feats)} senior centers")
    return feats


def fetch_harm_reduction() -> list[dict]:
    """Harm reduction / syringe service programs. DOHMH Health Map, Socrata nk7g-qeep.
    Includes syringe exchange, HIV testing, naloxone distribution, and related services.
    Has structured per-day hours.
    """
    _log("Harm reduction sites (nk7g-qeep) ...")
    rows = _socrata("nk7g-qeep", {}, "DOHMH harm reduction")
    DAY_MAP = [("Mon","monday"),("Tue","tuesday"),("Wed","wednesday"),
               ("Thu","thursday"),("Fri","friday"),("Sat","saturday"),("Sun","sunday")]
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        hours = {d: v for d, k in DAY_MAP
                 if (v := (row.get(k) or "").strip())
                 and v.lower() not in ("", "closed", "n/a")}
        fid = row.get("facilityid") or (row.get("facility_name") or "").lower().replace(" ", "-")
        feats.append(_feat(
            source_id=f"harm:{fid}-{lat_f:.4f}",
            source_label="DOHMH",
            lat=lat_f, lng=lng_f,
            name=row.get("facility_name") or "Harm reduction site",
            address=row.get("address") or "",
            resource_types=["harm_reduction"],
            hours=hours or "call ahead",
            borough=BORO_FROM_LONG.get((row.get("borough") or "").lower(), "?"),
        ))
    _log(f"  → {len(feats)} harm reduction sites")
    return feats


def fetch_hospitals() -> list[dict]:
    """NYC Health + Hospitals public hospital system. Socrata q6fj-vxf8.
    Covers 11 acute care hospitals + diagnostic/treatment centers.
    Private hospitals not included (no open data source with complete coverage).
    """
    _log("NYC H+H hospitals (q6fj-vxf8) ...")
    rows = _socrata("q6fj-vxf8", {}, "NYC H+H hospitals")
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        ftype = (row.get("facility_type") or "").lower()
        types = ["medical"]
        if "acute" in ftype or "hospital" in ftype:
            types += ["cool_indoor", "warm_indoor"]
        fid = row.get("facilityid") or (row.get("facility_name") or "").lower().replace(" ", "-")
        feats.append(_feat(
            source_id=f"hospital:{fid}-{lat_f:.4f}",
            source_label="NYC H+H",
            lat=lat_f, lng=lng_f,
            name=row.get("facility_name") or "NYC Health + Hospitals",
            address=row.get("cross_streets") or "",
            resource_types=types,
            hours="24/7 emergency",
            borough=BORO_FROM_LONG.get((row.get("borough") or "").lower(), "?"),
        ))
    _log(f"  → {len(feats)} NYC H+H facilities")
    return feats


def _fetch_facilities_db(facsubgrp: str, resource_types: list[str],
                         label: str, id_prefix: str) -> list[dict]:
    """Generic fetcher for NYC Facilities Database (ji82-xba5) by facsubgrp."""
    rows = _socrata("ji82-xba5", {"$where": f"facsubgrp='{facsubgrp}'"}, f"Facilities DB / {label}")
    feats: list[dict] = []
    for row in rows:
        coords = _coords(row)
        if not coords: continue
        lat_f, lng_f = coords
        uid = row.get("uid") or (row.get("facname") or "").lower().replace(" ", "-")
        feats.append(_feat(
            source_id=f"{id_prefix}:{uid}-{lat_f:.4f}",
            source_label="NYC Facilities",
            lat=lat_f, lng=lng_f,
            name=row.get("facname") or label,
            address=f"{row.get('address', '')} {row.get('zipcode', '')}".strip(),
            resource_types=resource_types,
            hours="call ahead",
            borough=BORO_FROM_SHORT.get((row.get("boro") or "").upper(), "?"),
        ))
    _log(f"  → {len(feats)} {label}")
    return feats


def fetch_mental_health() -> list[dict]:
    """Mental health clinics. NYC Facilities Database (ji82-xba5).
    Filter: facsubgrp = 'MENTAL HEALTH' (~1,266 records).
    Includes crisis respite centers, outpatient clinics, continuing day treatment.
    """
    _log("Mental health clinics (ji82-xba5) ...")
    return _fetch_facilities_db("MENTAL HEALTH", ["mental_health"], "mental health", "mh")


def fetch_community_centers() -> list[dict]:
    """Community and recreation centers. NYC Facilities Database (ji82-xba5).
    Filter: facsubgrp = 'COMMUNITY CENTERS AND COMMUNITY PROGRAMS' (~223 records).
    """
    _log("Community centers (ji82-xba5) ...")
    return _fetch_facilities_db(
        "COMMUNITY CENTERS AND COMMUNITY PROGRAMS",
        ["community_center", "cool_indoor", "warm_indoor", "seating"],
        "community centers", "cc",
    )


# ── Source registry: name → fetch function ──────────────────────────────────
# This is the authoritative list of comfort resource sources.
# To add a new source: write a fetch_*() function above and register it here.
COMFORT_SOURCES: dict[str, Any] = {
    "nypl":              fetch_nypl,
    "bpl":               fetch_bpl,
    "queens_library":    fetch_queens_library,
    "nycha":             fetch_nycha,
    "dhs_dropins":       fetch_dhs_dropins,
    "parks_pools":       fetch_parks_pools,
    "restrooms":         fetch_restrooms,
    "linknyc":           fetch_linknyc,
    "food_pantries":     fetch_food_pantries,
    "senior_centers":    fetch_senior_centers,
    "harm_reduction":    fetch_harm_reduction,
    "hospitals":         fetch_hospitals,
    "mental_health":     fetch_mental_health,
    "community_centers": fetch_community_centers,
}


# ═══════════════════════════════════════════════════════════════════════════
# GEOCODER POI FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_pois() -> list[dict]:
    """Named places from OpenStreetMap via Overpass API.
    Returns a flat list of {name, lat, lng, type, category} sorted by
    category priority (borough > transit > neighborhood > park > amenity > …).
    Used for client-side fuzzy-match geocoding (Fuse.js).
    """
    _log("OpenStreetMap POIs (Overpass) ...")
    BBOX = "40.4774,-74.2591,40.9176,-73.7004"
    AMENITY = (
        "library,school,university,hospital,museum,community_centre,"
        "theatre,cinema,cafe,bar,restaurant,pub,pharmacy,bank,"
        "post_office,police,fire_station,townhall,marketplace,"
        "ferry_terminal,bus_station"
    )
    query = f"""
[out:json][timeout:300];
(
  node["place"]["name"]({BBOX});
  node["railway"="station"]["name"]({BBOX});
  node["public_transport"="station"]["name"]({BBOX});
  node["aeroway"~"aerodrome|terminal"]["name"]({BBOX});
  node["tourism"]["name"]({BBOX});
  node["amenity"~"^({AMENITY})$"]["name"]({BBOX});
  way["leisure"~"park|playground|garden"]["name"]({BBOX});
  way["tourism"]["name"]({BBOX});
  way["amenity"~"^({AMENITY})$"]["name"]({BBOX});
  way["building"]["name"]["tourism"!~"."]({BBOX});
  way["shop"="mall"]["name"]({BBOX});
  relation["place"]["name"]({BBOX});
  relation["leisure"="park"]["name"]({BBOX});
  relation["boundary"="administrative"]["admin_level"~"^(7|8|9|10)$"]["name"]({BBOX});
);
out center tags;
"""
    r = requests.post("https://overpass-api.de/api/interpreter",
                      data={"data": query}, timeout=600,
                      headers={"User-Agent": UA["User-Agent"]})
    r.raise_for_status()

    CATEGORY_ORDER = {k: i for i, k in enumerate([
        "borough", "transit", "neighborhood", "park",
        "amenity_priority", "poi", "amenity", "building", "other",
    ])}

    def _classify(tags: dict) -> tuple[str, str]:
        place = tags.get("place")
        if place in ("city", "borough"):                   return place, "borough"
        if place in ("suburb", "quarter", "neighbourhood"): return place, "neighborhood"
        if place:                                           return place, "neighborhood"
        if tags.get("railway") == "station":               return "station", "transit"
        if tags.get("public_transport") == "station":      return "station", "transit"
        if tags.get("aeroway") in ("aerodrome", "terminal"): return "airport", "transit"
        if tags.get("tourism") == "museum":                return "museum", "poi"
        if tags.get("tourism") == "attraction":            return "attraction", "poi"
        if tags.get("tourism"):                            return tags["tourism"], "poi"
        if tags.get("leisure") in ("park", "playground", "garden"):
            return tags["leisure"], "park"
        if tags.get("amenity") in ("library", "hospital", "university", "community_centre"):
            return tags["amenity"], "amenity_priority"
        if tags.get("amenity"):                            return tags["amenity"], "amenity"
        if tags.get("shop") == "mall":                     return "mall", "poi"
        if tags.get("boundary") == "administrative":       return "admin", "neighborhood"
        if tags.get("building"):                           return "building", "building"
        return "other", "other"

    items: list[dict] = []
    seen: set[tuple[str, int, int]] = set()
    for el in r.json().get("elements", []):
        tags = el.get("tags") or {}
        name = tags.get("name")
        if not name: continue
        if el["type"] == "node":
            lat, lng = el.get("lat"), el.get("lon")
        else:
            center = el.get("center") or {}
            lat, lng = center.get("lat"), center.get("lon")
        if lat is None or lng is None: continue
        key = (name.lower(), round(lat * 1e3), round(lng * 1e3))
        if key in seen: continue
        seen.add(key)
        t, cat = _classify(tags)
        items.append({
            "name": name,
            "lat": round(float(lat), 6),
            "lng": round(float(lng), 6),
            "type": t,
            "category": cat,
        })

    items.sort(key=lambda x: (CATEGORY_ORDER.get(x["category"], 99), x["name"]))
    _log(f"  → {len(items)} OSM POIs")
    return items


# ═══════════════════════════════════════════════════════════════════════════
# OSM ADDRESS POINTS  →  data/nyc-addresses.json
# ═══════════════════════════════════════════════════════════════════════════
#
# Pulls every node/way in NYC tagged with both addr:housenumber and
# addr:street, builds a normalized address string, and writes a flat
# Fuse.js-compatible JSON. Borough-chunked to stay under Overpass payload
# limits.
#
# Output schema is identical to nyc-pois.json so the same Fuse adapter can
# index either: {name, lat, lng, type, category}. Category="address" lets
# the geocoder de-prioritise these against named places (a query for
# "central park" should never resolve to "5 Central Park West").

ADDRESS_BBOXES: list[tuple[str, str, str]] = [
    # (borough_name, suffix_for_display, Overpass bbox "min_lat,min_lng,max_lat,max_lng")
    ("Manhattan",     "Manhattan",     "40.6815,-74.0479,40.8820,-73.9070"),
    ("Bronx",         "Bronx",         "40.7855,-73.9339,40.9176,-73.7654"),
    ("Brooklyn",      "Brooklyn",      "40.5707,-74.0420,40.7395,-73.8334"),
    ("Queens",        "Queens",        "40.5418,-73.9626,40.8007,-73.7004"),
    ("Staten Island", "Staten Island", "40.4774,-74.2591,40.6515,-74.0492"),
]

# Title-case a street name while preserving NYC conventions.
_STREET_LOWERS = {"of", "the", "and", "at"}
_STREET_KEEP_UPPER = {"jfk", "fdr", "lic", "nyc", "moma"}


def _title_street(s: str) -> str:
    out: list[str] = []
    for i, w in enumerate(s.split()):
        wl = w.lower()
        if wl in _STREET_KEEP_UPPER:
            out.append(wl.upper())
        elif i > 0 and wl in _STREET_LOWERS:
            out.append(wl)
        else:
            out.append(w[:1].upper() + w[1:].lower() if w else w)
    return " ".join(out)


def _address_string(housenumber: str, street: str, borough: str) -> str:
    h = housenumber.strip()
    s = _title_street(street.strip())
    return f"{h} {s}, {borough}"


def fetch_osm_addresses() -> list[dict]:
    """Address points from OpenStreetMap via Overpass API, chunked by borough.

    Returns a flat list of {name, lat, lng, type, category} where
    category="address" so the geocoder can rank these below named places.

    Deduplicates by (lowercase address, 4-decimal-place coord bucket). Two
    addresses within ~11m of each other collapse to one entry.
    """
    _log("OpenStreetMap addresses (Overpass, borough-chunked) ...")
    seen: set[tuple[str, int, int]] = set()
    items: list[dict] = []

    for boro_name, display_suffix, bbox in ADDRESS_BBOXES:
        _log(f"  {boro_name} ...")
        query = f"""
[out:json][timeout:300];
(
  node["addr:housenumber"]["addr:street"]({bbox});
  way["addr:housenumber"]["addr:street"]({bbox});
);
out center tags;
"""
        try:
            r = requests.post(
                "https://overpass-api.de/api/interpreter",
                data={"data": query}, timeout=600,
                headers={"User-Agent": UA["User-Agent"]},
            )
            r.raise_for_status()
        except requests.RequestException as e:
            _log(f"    fetch failed: {e}")
            continue

        added = 0
        for el in r.json().get("elements", []):
            tags = el.get("tags") or {}
            hn = tags.get("addr:housenumber")
            st = tags.get("addr:street")
            if not hn or not st:
                continue
            if el["type"] == "node":
                lat, lng = el.get("lat"), el.get("lon")
            else:
                center = el.get("center") or {}
                lat, lng = center.get("lat"), center.get("lon")
            if lat is None or lng is None:
                continue
            if not _in_nyc(lat, lng):
                continue
            name = _address_string(hn, st, display_suffix)
            key = (name.lower(), round(lat * 1e4), round(lng * 1e4))
            if key in seen:
                continue
            seen.add(key)
            items.append({
                "name": name,
                "lat": round(float(lat), 6),
                "lng": round(float(lng), 6),
                "type": "address",
                "category": "address",
            })
            added += 1
        _log(f"    +{added:,} addresses (running total: {len(items):,})")

    items.sort(key=lambda x: x["name"])
    _log(f"  → {len(items):,} OSM addresses")
    return items


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NYC open data for Ariadne (comfort resources + geocoder POIs).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Sources: " + ", ".join(COMFORT_SOURCES),
    )
    parser.add_argument("--section", choices=["all", "comfort", "pois", "addresses"], default="all",
                        help="Which data to fetch (default: all)")
    parser.add_argument("--sources", metavar="SRC,...",
                        help="Comma-separated list of comfort source names to run "
                             "(overrides --section for comfort; implies --section=comfort). "
                             "Available: " + ", ".join(COMFORT_SOURCES))
    parser.add_argument("--output-dir", metavar="DIR", type=pathlib.Path, default=DATA_DIR,
                        help="Directory to write output files (default: data/)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without making any network calls")
    args = parser.parse_args()

    out_dir: pathlib.Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # Resolve which comfort sources to run
    if args.sources:
        requested = [s.strip() for s in args.sources.split(",")]
        unknown = [s for s in requested if s not in COMFORT_SOURCES]
        if unknown:
            parser.error(f"Unknown source(s): {', '.join(unknown)}. "
                         f"Available: {', '.join(COMFORT_SOURCES)}")
        comfort_sources = {k: COMFORT_SOURCES[k] for k in requested}
        run_comfort = True
        run_pois = False
        run_addresses = False
    else:
        comfort_sources = COMFORT_SOURCES
        run_comfort   = args.section in ("all", "comfort")
        run_pois      = args.section in ("all", "pois")
        run_addresses = args.section in ("all", "addresses")

    if args.dry_run:
        _log("DRY RUN. No network calls will be made")
        if run_comfort:
            _log(f"Would fetch comfort sources: {', '.join(comfort_sources)}")
            _log(f"  → {out_dir / 'nyc-comfort.json'}")
        if run_pois:
            _log("Would fetch OpenStreetMap POIs via Overpass")
            _log(f"  → {out_dir / 'nyc-pois.json'}")
        if run_addresses:
            _log("Would fetch OpenStreetMap addresses via Overpass (borough-chunked)")
            _log(f"  → {out_dir / 'nyc-addresses.json'}")
        return

    summary: dict[str, Any] = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "sources": {},
        "errors": [],
    }

    # ── Comfort resources ──────────────────────────────────────────────────
    if run_comfort:
        _log("=== Comfort resources ===")
        all_feats: list[dict] = []
        for name, fn in comfort_sources.items():
            try:
                feats = fn()
                all_feats.extend(feats)
                summary["sources"][name] = {"count": len(feats), "status": "ok"}
            except Exception as e:
                _log(f"  ERROR {name}: {e}")
                summary["sources"][name] = {"count": 0, "status": "error", "error": str(e)}
                summary["errors"].append({"source": name, "error": str(e)})

        out_path = out_dir / "nyc-comfort.json"
        out_path.write_text(json.dumps({"type": "FeatureCollection", "features": all_feats}))
        size_mb = out_path.stat().st_size / 1_000_000
        summary["comfort_total"] = len(all_feats)
        _log(f"\nComfort resources: {len(all_feats):,} features → {out_path} ({size_mb:.1f} MB)")

    # ── Geocoder POIs ──────────────────────────────────────────────────────
    if run_pois:
        _log("\n=== Geocoder POIs ===")
        try:
            items = fetch_pois()
            out_path = out_dir / "nyc-pois.json"
            out_path.write_text(json.dumps(items))
            size_mb = out_path.stat().st_size / 1_000_000
            summary["sources"]["overpass_pois"] = {"count": len(items), "status": "ok"}
            summary["pois_total"] = len(items)
            _log(f"\nGeocoder POIs: {len(items):,} items → {out_path} ({size_mb:.1f} MB)")
        except Exception as e:
            _log(f"  ERROR overpass_pois: {e}")
            summary["sources"]["overpass_pois"] = {"count": 0, "status": "error", "error": str(e)}
            summary["errors"].append({"source": "overpass_pois", "error": str(e)})

    # ── OSM addresses ──────────────────────────────────────────────────────
    if run_addresses:
        _log("\n=== OSM addresses ===")
        try:
            items = fetch_osm_addresses()
            out_path = out_dir / "nyc-addresses.json"
            out_path.write_text(json.dumps(items))
            size_mb = out_path.stat().st_size / 1_000_000
            summary["sources"]["overpass_addresses"] = {"count": len(items), "status": "ok"}
            summary["addresses_total"] = len(items)
            _log(f"\nOSM addresses: {len(items):,} items → {out_path} ({size_mb:.1f} MB)")
        except Exception as e:
            _log(f"  ERROR overpass_addresses: {e}")
            summary["sources"]["overpass_addresses"] = {"count": 0, "status": "error", "error": str(e)}
            summary["errors"].append({"source": "overpass_addresses", "error": str(e)})

    # ── Summary ────────────────────────────────────────────────────────────
    summary_path = out_dir / "fetch_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    _log(f"\nSummary → {summary_path}")
    if summary["errors"]:
        _log(f"Errors ({len(summary['errors'])}): "
             + ", ".join(e["source"] for e in summary["errors"]))


if __name__ == "__main__":
    main()
