"""Draw the stratified random sample of crossings and render one rating sheet
per crossing over NYC's 2018 orthoimagery (the year of the ramp survey).

Frame: crossings of the v0.3.2 graph with exactly two ends (97.5% of all).
Strata: the five boroughs, PER_BOROUGH each, seed SEED.
Sheet: left panel is the imagery with the crossing's two ends marked A and B
and nothing else; right panel is the same view with the DOT survey's ramp
positions (as surveyed, not as snapped to the graph) drawn as magenta squares.
No rule's verdict is drawn.

usage: sample.py TABLES_PKL GROUPS_PKL RAW_RAMPS_GEOJSON OUTDIR
"""
import io, json, math, pickle, sys, time
from pathlib import Path
import numpy as np, pandas as pd, requests
from PIL import Image, ImageDraw, ImageFont
FONT = ImageFont.load_default(size=22)
from scipy.spatial import cKDTree

SEED, PER_BOROUGH, Z = 20261002, 40, 20
TILES = "https://maps.nyc.gov/xyz/1.0.0/photo/2018/{z}/{x}/{y}.png8"
out = Path(sys.argv[4]); (out / "sheets").mkdir(parents=True, exist_ok=True)
cache = out / "tiles"; cache.mkdir(exist_ok=True)

_, N, E = pickle.load(open(sys.argv[1], "rb"))
edges, ends, cr = pickle.load(open(sys.argv[2], "rb"))
xy = N.set_index("_id")[["lon", "lat"]]
frame = cr[(cr.ends == 2) & cr.borough.isin(["MN", "BK", "QN", "BX", "SI"])]
rng = np.random.default_rng(SEED)
picked = pd.concat([frame[frame.borough == b].loc[sorted(rng.choice(frame.index[frame.borough == b], PER_BOROUGH, replace=False))]
                    for b in ["BK", "BX", "MN", "QN", "SI"]])

ramps = json.load(open(sys.argv[3]))["features"]
R = pd.DataFrame([{**f["properties"], "lon": f["geometry"]["coordinates"][0], "lat": f["geometry"]["coordinates"][1]} for f in ramps])
EAST = 111320 * math.cos(math.radians(40.7))
rtree = cKDTree(np.c_[R.lon * EAST, R.lat * 111320])


def tile_xy(lon, lat):
    n = 2 ** Z
    return (lon + 180) / 360 * n, (1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n


def tile(x, y):
    p = cache / f"{Z}_{x}_{y}.png"
    if not p.exists():
        r = requests.get(TILES.format(z=Z, x=x, y=y), timeout=60)
        r.raise_for_status()
        p.write_bytes(r.content); time.sleep(0.15)
    return Image.open(p).convert("RGB")


def view(lon, lat, side_m, px):
    """Imagery square of side_m centred on lon, lat, and a lon/lat -> pixel function."""
    cx, cy = tile_xy(lon, lat)
    half = side_m / 2 / (40075016.7 * math.cos(math.radians(lat)) / 2 ** Z)   # in tiles
    x0, x1, y0, y1 = math.floor(cx - half), math.floor(cx + half), math.floor(cy - half), math.floor(cy + half)
    mosaic = Image.new("RGB", ((x1 - x0 + 1) * 256, (y1 - y0 + 1) * 256))
    for tx in range(x0, x1 + 1):
        for ty in range(y0, y1 + 1):
            mosaic.paste(tile(tx, ty), ((tx - x0) * 256, (ty - y0) * 256))
    box = [(cx - half - x0) * 256, (cy - half - y0) * 256, (cx + half - x0) * 256, (cy + half - y0) * 256]
    img = mosaic.crop([round(v) for v in box]).resize((px, px), Image.LANCZOS)
    def to_px(lo, la):
        tx, ty = tile_xy(lo, la)
        return ((tx - (cx - half)) / (2 * half) * px, (ty - (cy - half)) / (2 * half) * px)
    return img, to_px


rows = []
for k, (cid, c) in enumerate(picked.iterrows(), 1):
    en = ends[ends.crossing == cid].node.tolist()
    # A is the western end (southern on a tie), so both raters name them alike.
    en.sort(key=lambda n: (xy.lon[n], xy.lat[n]))
    (alon, alat), (blon, blat) = xy.loc[en[0]], xy.loc[en[1]]
    length = math.hypot((alon - blon) * EAST, (alat - blat) * 111320)
    side = max(36.0, length + 22.0)
    PX = 800
    img, to_px = view((alon + blon) / 2, (alat + blat) / 2, side, PX)
    left = img.copy(); right = img.copy()
    for panel, with_ramps in ((left, False), (right, True)):
        d = ImageDraw.Draw(panel)
        for label, (lo, la) in (("A", (alon, alat)), ("B", (blon, blat))):
            x, y = to_px(lo, la); r = 1.2 / side * PX     # 1.2 m ring: where the graph puts the end
            d.ellipse([x - r, y - r, x + r, y + r], outline=(0, 255, 255), width=2)
            d.text((x + r + 4, y - r - 24), label, fill=(0, 255, 255), font=FONT,
                   stroke_width=2, stroke_fill=(0, 0, 0))
        ax, ay = to_px(alon, alat); bx, by = to_px(blon, blat)
        # The crossing as drawn in OSM, middle third only, so the kerbs stay clear.
        d.line([(ax + (bx - ax) / 3, ay + (by - ay) / 3), (ax + 2 * (bx - ax) / 3, ay + 2 * (by - ay) / 3)], fill=(255, 255, 0), width=1)
        if with_ramps:
            near = sorted(set(rtree.query_ball_point([alon * EAST, alat * 111320], 25) + rtree.query_ball_point([blon * EAST, blat * 111320], 25)))
            for j in near:
                x, y = to_px(R.lon[j], R.lat[j]); s = 0.45 / side * PX
                d.rectangle([x - s, y - s, x + s, y + s], outline=(255, 0, 255), width=2)
    sheet = Image.new("RGB", (2 * PX + 6, PX + 18), (0, 0, 0))
    sheet.paste(left, (0, 18)); sheet.paste(right, (PX + 6, 18))
    ImageDraw.Draw(sheet).text((4, 3), f"crossing {k:03d}   view {side:.0f} m wide   left: imagery, ends A and B   right: same, with surveyed ramp positions (magenta)   north is up", fill=(255, 255, 255))
    sheet.save(out / "sheets" / f"{k:03d}.jpg", quality=90)
    rows.append({"n": k, "crossing": int(cid), "borough": c.borough, "edges": int(c.edges), "length_m": round(length, 1),
                 "a_node": en[0], "a_lon": alon, "a_lat": alat, "b_node": en[1], "b_lon": blon, "b_lat": blat, "view_m": round(side, 1)})
    print(k, c.borough, round(length, 1), flush=True)
pd.DataFrame(rows).to_csv(out / "sample.csv", index=False)
json.dump({"seed": SEED, "per_borough": PER_BOROUGH, "frame": "v0.3.2 crossings with exactly two ends",
           "frame_size_by_borough": frame.borough.value_counts().to_dict(), "all_crossings": int(len(cr)),
           "crossings_by_end_count": cr.ends.value_counts().sort_index().to_dict(),
           "imagery": TILES, "zoom": Z}, open(out / "sample_meta.json", "w"), indent=1)
