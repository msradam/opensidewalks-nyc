"""Download the raw inputs for the ramp backlog table into raw/, once.

Every response is saved as received. A file that already exists is never
requested again, so rerunning this script after an interruption only fetches
what is missing. Stdlib plus requests, no token.
"""

import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
LAYER = (
    "https://services.arcgis.com/wmZOI9vyUBq1zTZx/arcgis/rest/services/"
    "CMT_SURVEY_COMPLIANCY_TOLERANCE_PROD/FeatureServer"
)
PAGE = 2000  # the layer's maxRecordCount
PAUSE = 1.0  # seconds between requests
SOCRATA = "https://data.cityofnewyork.us"
DISTRICTS = {
    "council": "872g-cjhh",
    "community": "5crt-au7u",
}  # DCP, found via the catalogue API

session = requests.Session()
session.headers["User-Agent"] = (
    "opensidewalks-nyc research (ramp backlog table, one-off, cached)"
)


def now():
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def get(url, params=None):
    """GET with patient retries. A throttle or server error is waited out, not reported as data."""
    delay = 5
    for attempt in range(1, 8):
        try:
            r = session.get(url, params=params, timeout=120)
            if r.status_code == 200:
                # ArcGIS reports failures as HTTP 200 with an "error" object.
                if not (r.text.lstrip().startswith("{") and '"error"' in r.text[:200]):
                    return r
                print(
                    f"  attempt {attempt}: service error body {r.text[:200]!r}",
                    flush=True,
                )
            else:
                print(f"  attempt {attempt}: HTTP {r.status_code}", flush=True)
        except requests.RequestException as e:
            print(f"  attempt {attempt}: {e}", flush=True)
        time.sleep(delay)
        delay = min(delay * 2, 120)
    sys.exit(
        f"BLOCKED: {url} still failing after 7 attempts. Rerun later; saved pages are kept."
    )


def save(path, text):
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_text(text)
    json.loads(text)  # a truncated body must not be mistaken for a saved page
    tmp.rename(path)


def fetch_once(path, url, params=None):
    """Return True if a request was made."""
    if path.exists():
        return False
    print(f"GET {path.name}", flush=True)
    save(path, get(url, params).text)
    time.sleep(PAUSE)
    return True


def main():
    pages = RAW / "compliance"
    pages.mkdir(parents=True, exist_ok=True)
    log_path = RAW / "retrieved.json"
    log = json.loads(log_path.read_text()) if log_path.exists() else {}
    started = now()

    fetch_once(RAW / "compliance_service.json", LAYER, {"f": "json"})
    fetch_once(RAW / "compliance_layer0.json", f"{LAYER}/0", {"f": "json"})
    fetch_once(
        RAW / "compliance_count.json",
        f"{LAYER}/0/query",
        {"where": "1=1", "returnCountOnly": "true", "f": "json"},
    )
    total = json.loads((RAW / "compliance_count.json").read_text())["count"]
    oid = json.loads((RAW / "compliance_layer0.json").read_text())["objectIdField"]

    fetched = 0
    for i, offset in enumerate(range(0, total, PAGE)):
        made = fetch_once(
            pages / f"page_{i:04d}.json",
            f"{LAYER}/0/query",
            {
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": 4326,
                "orderByFields": oid,
                "resultOffset": offset,
                "resultRecordCount": PAGE,
                "f": "json",
            },
        )
        fetched += made
    print(
        f"compliance layer: {total} rows expected, {fetched} pages fetched this run",
        flush=True,
    )
    if fetched:
        log.setdefault("compliance_layer", {"url": LAYER, "first_request": started})
        log["compliance_layer"]["last_request"] = now()
        log["compliance_layer"]["expected_rows"] = total

    for name, dsid in DISTRICTS.items():
        a = fetch_once(
            RAW / f"{name}_districts_{dsid}.geojson",
            f"{SOCRATA}/resource/{dsid}.geojson",
            {"$limit": 5000},
        )
        b = fetch_once(
            RAW / f"{name}_districts_{dsid}.meta.json",
            f"{SOCRATA}/api/views/{dsid}.json",
        )
        if a or b:
            log[f"{name}_districts"] = {
                "url": f"{SOCRATA}/resource/{dsid}.geojson",
                "retrieved": now(),
            }

    log_path.write_text(json.dumps(log, indent=1) + "\n")
    print("done", flush=True)


if __name__ == "__main__":
    main()
