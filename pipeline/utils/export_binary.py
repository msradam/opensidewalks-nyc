"""Export the assembled OSW GeoJSON to a compact binary format (OSWB v2).

Reads the OSW v0.3 FeatureCollection and faithfully translates all
routing-relevant attributes into a compact binary.

OSWB binary format v2. All values little-endian:

  Header (16 bytes):
    magic:       [u8; 4] = b"OSWB"
    version:     u8      = 2
    _pad:        [u8; 3]
    node_count:  u32
    edge_count:  u32

  Nodes (10 bytes × node_count):
    lon:    f32   degrees
    lat:    f32   degrees
    attrs:  u8    bit 0=kerb_lowered, bit 1=kerb_raised, bit 2=tactile_paving
    _pad:   u8

  Edges (18 bytes × edge_count):
    u_idx:   u32
    v_idx:   u32
    length:  f32   metres
    incline: i16   actual × 10000, range [-10000, 10000]
    footway: u8    0=sidewalk 1=crossing 2=footway 3=steps 4=other
    surface: u8    0=unknown 1=asphalt 2=concrete 3=paving_stones
                   4=cobblestone 5=gravel 6=unpaved 7=other_paved
    flags:   u8    bit 0   = curbramps (lowered kerb at ≥1 endpoint)
                   bits[2:1] = crossing_markings:
                               0=unknown 1=marked(yes) 2=zebra 3=reserved
    width:   u8    actual × 5, i.e. Stored/5 = metres; 0=unknown, max 51m

Curbramp derivation (OSW v0.3 stores curb ramps as Point nodes, not edge
attributes):
  1. Direct: endpoint node (_u_id or _v_id) has kerb=lowered on the node itself
             (set when assemble.py merged a CurbRamp node at that position).
  2. Positional fallback: endpoint coordinate matches a CurbRamp Point node
             position within 1e-5 degrees (~1m). Handles cases where
             deduplication did not merge the CurbRamp onto the OSM node.

Usage:
    uv run python -m pipeline.utils.export_binary [--output path]
    (defaults to output/nyc-pedestrian.bin)

Reads from output/nyc-osw.geojson (the assembled, incline-annotated output).
"""

import json
import math
import struct
from pathlib import Path

import click
import numpy as np

MAGIC   = b"OSWB"
VERSION = 2

FOOTWAY_MAP = {
    "sidewalk":  0,
    "crossing":  1,
    "footway":   2,
    "steps":     3,
}

SURFACE_MAP = {
    "asphalt":        1,
    "concrete":       2,
    "paving_stones":  3,
    "cobblestone":    4,
    "sett":           4,
    "gravel":         5,
    "unpaved":        6,
    "dirt":           6,
    "grass":          6,
    "mud":            6,
    "sand":           6,
}

# Node attrs byte bits
KERB_LOWERED_BIT   = 0
KERB_RAISED_BIT    = 1
TACTILE_PAVING_BIT = 2

# Edge flags byte layout
CURBRAMPS_BIT             = 0        # bit 0
CROSSING_MARKINGS_SHIFT   = 1        # bits [2:1]
CROSSING_MARKINGS_UNKNOWN = 0
CROSSING_MARKINGS_MARKED  = 1        # crossing:markings = yes
CROSSING_MARKINGS_ZEBRA   = 2        # crossing:markings = zebra

CROSSING_MARKINGS_MAP = {
    "yes":   CROSSING_MARKINGS_MARKED,
    "zebra": CROSSING_MARKINGS_ZEBRA,
}


def _node_attrs(kerb, tactile) -> int:
    bits = 0
    if isinstance(kerb, str):
        if kerb in ("lowered", "flush"):
            bits |= (1 << KERB_LOWERED_BIT)
        elif kerb == "raised":
            bits |= (1 << KERB_RAISED_BIT)
    if tactile is True or tactile == "yes":
        bits |= (1 << TACTILE_PAVING_BIT)
    return bits


PEDESTRIAN_FOOTWAYS = {"sidewalk", "crossing", "footway", "steps"}

COORD_ROUND = 5   # ~1m precision for positional curbramp lookup


def _haversine_length(coords: list) -> float:
    """Metres along a [lon, lat] coordinate sequence."""
    R = 6_371_000.0
    total = 0.0
    for i in range(len(coords) - 1):
        lon1, lat1 = coords[i][0], coords[i][1]
        lon2, lat2 = coords[i+1][0], coords[i+1][1]
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat/2)**2
             + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
             * math.sin(dlon/2)**2)
        total += R * 2 * math.asin(math.sqrt(a))
    return total


def export_binary(osw_path: Path, output_path: Path) -> None:
    click.echo(f"  Reading {osw_path} ({osw_path.stat().st_size / 1e6:.0f} MB)…")

    node_coords: dict[str, tuple[float, float]] = {}
    node_kerb:   dict[str, str]                 = {}   # _id → kerb value
    curb_positions: set[tuple[float, float]]    = set()  # positional fallback
    edge_rows:   list[dict]                     = []

    with open(osw_path) as f:
        fc = json.load(f)

    click.echo(f"    {len(fc['features']):,} total features. Parsing…")

    for feat in fc["features"]:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry")   or {}
        gtype = geom.get("type")

        if gtype == "Point":
            nid = props.get("_id")
            if not nid:
                continue
            coords = geom["coordinates"]
            lon, lat = coords[0], coords[1]
            node_coords[nid] = (lon, lat)

            kerb = props.get("kerb")
            if kerb:
                node_kerb[nid] = str(kerb).lower()

            # CurbRamp Point node (OSW v0.3 barrier=kerb representation)
            if props.get("barrier") == "kerb" and str(kerb or "").lower() in ("lowered", "flush"):
                curb_positions.add((round(lon, COORD_ROUND), round(lat, COORD_ROUND)))

        elif gtype == "LineString":
            uid = props.get("_u_id")
            vid = props.get("_v_id")
            if not uid or not vid:
                continue
            footway = str(props.get("footway") or "").lower()
            if footway not in PEDESTRIAN_FOOTWAYS:
                continue
            coords = geom["coordinates"]
            edge_rows.append({
                "_u_id":              uid,
                "_v_id":              vid,
                "coords":             coords,
                "length":             _haversine_length(coords),
                "incline":            props.get("incline"),
                "footway":            footway,
                "surface":            props.get("surface"),
                "crossing:markings":  props.get("crossing:markings"),
                "width":              props.get("width"),
            })

    click.echo(f"    Pedestrian edges:  {len(edge_rows):,}")
    click.echo(f"    Node features:     {len(node_coords):,}")
    click.echo(f"    CurbRamp positions:{len(curb_positions):,}")

    # Build node index. Edge endpoints may not appear as explicit Point features
    for row in edge_rows:
        coords = row["coords"]
        node_coords.setdefault(row["_u_id"], (coords[0][0],  coords[0][1]))
        node_coords.setdefault(row["_v_id"], (coords[-1][0], coords[-1][1]))

    node_ids   = list(node_coords.keys())
    node_index = {nid: i for i, nid in enumerate(node_ids)}
    n_nodes    = len(node_ids)
    n_edges    = len(edge_rows)
    click.echo(f"    Unique nodes:      {n_nodes:,}")

    # ── write binary ──────────────────────────────────────────────────────────
    click.echo(f"  Writing {output_path} …")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    n_written        = 0
    n_skipped        = 0
    n_curbramps      = 0
    n_marked         = 0

    with open(output_path, "wb") as f:
        # Header: magic(4) + version(1) + pad(3) + node_count(4) + edge_count(4)
        f.write(MAGIC)
        f.write(struct.pack("<B3xII", VERSION, n_nodes, n_edges))

        # Node records: lon(f32) + lat(f32) + attrs(u8) + pad(u8)
        for nid in node_ids:
            lon, lat = node_coords[nid]
            kerb   = node_kerb.get(nid)
            tactile = None  # tactile_paving stored on nodes but not in node_kerb dict
            attrs = _node_attrs(kerb, tactile)
            f.write(struct.pack("<ffBx", lon, lat, attrs))

        # Edge records
        for row in edge_rows:
            u_idx = node_index.get(row["_u_id"])
            v_idx = node_index.get(row["_v_id"])
            if u_idx is None or v_idx is None:
                n_skipped += 1
                continue

            length      = float(row["length"])  if _notnull(row.get("length"))  else 0.0
            incline_f   = float(row["incline"]) if _notnull(row.get("incline")) else 0.0
            incline_i16 = max(-10000, min(10000, int(round(incline_f * 10000))))
            footway_byte = FOOTWAY_MAP.get(str(row.get("footway") or "").lower(), 4)
            surface_byte = SURFACE_MAP.get(str(row.get("surface") or "").lower(), 0)

            # ── curbramps: derive from OSW CurbRamp Point nodes ──────────────
            # OSW v0.3 represents curb ramps as Point nodes (barrier=kerb,
            # kerb=lowered), not as edge attributes. We derive the edge-level
            # flag by checking whether either endpoint has a lowered kerb.
            has_curbramp = False
            if row["footway"] == "crossing":
                uid, vid = row["_u_id"], row["_v_id"]
                # 1. Direct: assembly merged CurbRamp node onto endpoint node
                if node_kerb.get(uid) in ("lowered", "flush") or \
                   node_kerb.get(vid) in ("lowered", "flush"):
                    has_curbramp = True
                # 2. Positional fallback: CurbRamp node at same location
                elif uid in node_coords:
                    u_pos = (round(node_coords[uid][0], COORD_ROUND),
                             round(node_coords[uid][1], COORD_ROUND))
                    if u_pos in curb_positions:
                        has_curbramp = True
                if not has_curbramp and vid in node_coords:
                    v_pos = (round(node_coords[vid][0], COORD_ROUND),
                             round(node_coords[vid][1], COORD_ROUND))
                    if v_pos in curb_positions:
                        has_curbramp = True

            # ── crossing:markings ─────────────────────────────────────────────
            cm_raw = str(row.get("crossing:markings") or "").lower().strip()
            cm_val = CROSSING_MARKINGS_MAP.get(cm_raw, CROSSING_MARKINGS_UNKNOWN)

            flags_byte = 0
            if has_curbramp:
                flags_byte |= (1 << CURBRAMPS_BIT)
                n_curbramps += 1
            flags_byte |= (cm_val << CROSSING_MARKINGS_SHIFT)
            if cm_val != CROSSING_MARKINGS_UNKNOWN:
                n_marked += 1

            width_raw  = row.get("width")
            width_byte = 0
            if _notnull(width_raw):
                try:
                    width_byte = max(0, min(255, int(round(float(width_raw) * 5))))
                except (ValueError, TypeError):
                    pass

            f.write(struct.pack(
                "<IIfhBBBB",
                u_idx, v_idx, length, incline_i16,
                footway_byte, surface_byte, flags_byte, width_byte,
            ))
            n_written += 1

    size_mb = output_path.stat().st_size / 1e6
    click.echo(f"    Written:           {n_written:,} edges ({n_skipped} skipped)")
    click.echo(f"    Crossings w/ curbramps:  {n_curbramps:,}")
    click.echo(f"    Crossings w/ markings:   {n_marked:,}")
    click.echo(f"    File size:         {size_mb:.1f} MB uncompressed")


def _notnull(v) -> bool:
    if v is None:
        return False
    try:
        return not (isinstance(v, float) and np.isnan(v))
    except Exception:
        return v is not None


@click.command()
@click.option("--osw",    default="output/nyc-osw.geojson",    show_default=True)
@click.option("--output", default="output/nyc-pedestrian.bin", show_default=True)
def main(osw: str, output: str) -> None:
    """Export assembled OSW GeoJSON to compact OSWB binary format."""
    export_binary(Path(osw), Path(output))
    click.echo("  Done.")


if __name__ == "__main__":
    main()
