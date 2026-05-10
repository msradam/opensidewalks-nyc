#!/usr/bin/env python3
"""
Restructure data/nyc-addresses.json (raw OSM-tagged points, ~1.4M entries,
~170 MB) into a compact street-keyed index for fast in-browser geocoding.

Why:
  Fuse.js over 1.4M items takes seconds to construct and tens of MB of heap.
  Real address queries are structured. "<housenum> <street>, <borough>" -
  so we index by street and do O(log n) housenum lookup per street.

Output: data/nyc-streets.json
  [
    ["adams street", "Brooklyn", [[123, 40.6951, -73.9890], [125, ...], ...]],
    ["atlantic avenue", "Brooklyn", [...]],
    ...
  ]
  Sorted by street key. Housenums sorted numerically. Coordinates rounded
  to 6 decimal places (~0.1 m precision).

Usage:
  uv run python pipeline/sources/build_address_index.py
"""
from __future__ import annotations

import json
import pathlib
import re
import sys
from collections import defaultdict

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
RAW_PATH = REPO_ROOT / "data" / "nyc-addresses.json"
OUT_PATH = REPO_ROOT / "data" / "nyc-streets.json"

# Same normalisation rules as the geocoder so keys match at lookup time.
_STREET_NORMS = [
    (re.compile(r"\bst\.?$|\bst\.?(?=\s)", re.I), "street"),
    (re.compile(r"\bave?\.?$|\bave?\.?(?=\s)", re.I), "avenue"),
    (re.compile(r"\bblvd\.?$|\bblvd\.?(?=\s)", re.I), "boulevard"),
    (re.compile(r"\brd\.?$|\brd\.?(?=\s)", re.I), "road"),
    (re.compile(r"\bdr\.?$|\bdr\.?(?=\s)", re.I), "drive"),
    (re.compile(r"\bpl\.?$|\bpl\.?(?=\s)", re.I), "place"),
    (re.compile(r"\bpkwy\.?$|\bpkwy\.?(?=\s)", re.I), "parkway"),
    (re.compile(r"\bln\.?$|\bln\.?(?=\s)", re.I), "lane"),
    (re.compile(r"\bct\.?$|\bct\.?(?=\s)", re.I), "court"),
    (re.compile(r"\bter\.?$|\bter\.?(?=\s)", re.I), "terrace"),
    (re.compile(r"\btpke?\.?$|\btpke?\.?(?=\s)", re.I), "turnpike"),
    (re.compile(r"\bsq\.?$|\bsq\.?(?=\s)", re.I), "square"),
]


def normalize_street(s: str) -> str:
    s = s.strip().lower()
    for rx, repl in _STREET_NORMS:
        s = rx.sub(repl, s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def parse_housenum(hn: str) -> tuple[int, str]:
    """Return (numeric_part, suffix) so housenums sort sensibly. '85-26' → (8526, '').
    Bare letters like 'A' return (0, 'A')."""
    m = re.match(r"\s*(\d+(?:-\d+)?)([A-Za-z]?)\s*$", hn.strip())
    if not m:
        return (0, hn.strip())
    digits = m.group(1).replace("-", "")
    return (int(digits) if digits.isdigit() else 0, m.group(2).upper())


# Parse "85-26 123rd Street, Queens" → ("85-26", "123rd Street", "Queens")
_NAME_RX = re.compile(r"^\s*(\S+)\s+(.+?),\s*([^,]+)\s*$")


def main() -> None:
    if not RAW_PATH.exists():
        print(f"error: {RAW_PATH} not found. Run --section addresses first.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {RAW_PATH} ({RAW_PATH.stat().st_size / 1e6:.1f} MB)…", file=sys.stderr)
    raw = json.load(RAW_PATH.open())
    print(f"  {len(raw):,} raw entries", file=sys.stderr)

    # street_norm + borough → list of (housenum, lat, lng)
    streets: dict[tuple[str, str], list[tuple[int, str, float, float]]] = defaultdict(list)
    skipped = 0
    for e in raw:
        m = _NAME_RX.match(e["name"])
        if not m:
            skipped += 1
            continue
        hn_raw, street_display, borough = m.group(1), m.group(2), m.group(3)
        street_key = normalize_street(street_display)
        if not street_key:
            skipped += 1
            continue
        hn_num, hn_suffix = parse_housenum(hn_raw)
        streets[(street_key, borough)].append((hn_num, hn_suffix, e["lat"], e["lng"]))

    print(f"  unique (street, borough): {len(streets):,}", file=sys.stderr)
    print(f"  skipped (parse-fail): {skipped:,}", file=sys.stderr)

    # Build output: sorted by street key, housenums sorted numerically + suffix
    out: list[list] = []
    for (street_key, borough), entries in streets.items():
        entries.sort(key=lambda e: (e[0], e[1]))
        # Compact: [housenum_int, lat, lng]. Drop suffix unless non-empty
        # (rare; saves space in the common case)
        compact = []
        for hn_num, hn_suffix, lat, lng in entries:
            if hn_suffix:
                compact.append([hn_num, hn_suffix, round(lat, 6), round(lng, 6)])
            else:
                compact.append([hn_num, round(lat, 6), round(lng, 6)])
        out.append([street_key, borough, compact])

    out.sort(key=lambda r: (r[0], r[1]))

    OUT_PATH.write_text(json.dumps(out, separators=(",", ":")))
    size_mb = OUT_PATH.stat().st_size / 1e6
    print(f"\nWrote {OUT_PATH} ({size_mb:.1f} MB, {len(out):,} (street, borough) entries)", file=sys.stderr)


if __name__ == "__main__":
    main()
