"""Draw a stratified sample of disagreements over the city's 2024 orthoimagery for a hand check.

Each sheet is one place: where the reference router (ORS on raw OSM) first uses something this
graph's wheelchair profile refuses. Overlays are neutral, so a rater who is not told the cause
can describe what is there: the reference route (orange), this graph's route if any (blue),
this graph's surveyed ramp positions (white squares) and the marked point (yellow ring).

Imagery: NYC OTI, NYC Orthos 2024 (flown 14 to 24 March 2024, 6 inch), CC BY 4.0 per
github.com/CityOfNewYork/nyc-geo-metadata Metadata_AerialImagery.md.

usage: python sheets.py GRAPH_NPZ PAIRS_DETAIL ORS_ARM_A OURS_PKL OSW_NODES_GEOJSON OUT_DIR [PER_CAUSE]
"""
import gzip, json, math, pickle, sys, time
from pathlib import Path
import numpy as np, requests
from PIL import Image, ImageDraw, ImageFont
from scipy.spatial import cKDTree
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from compare.graph import EAST, NORTH, Graph

SEED, Z, SIDE_M, PX = 20261003, 20, 70, 1000   # zoom 21 is listed by the service and returns 404
TILES = "https://tiles.arcgis.com/tiles/yG5s3afENB5iO9fj/arcgis/rest/services/NYC_Orthos_2024/MapServer/tile/{z}/{y}/{x}"
npz, detail, arm_a, ours_pkl, nodes_geojson, out = sys.argv[1:7]
PER = int(sys.argv[7]) if len(sys.argv) > 7 else 12
out = Path(out); (out / "sheets").mkdir(parents=True, exist_ok=True); cache = out / "tiles"; cache.mkdir(exist_ok=True)
FONT = ImageFont.load_default(size=20)

def tile_xy(lon, lat):
    n = 2 ** Z
    return (lon + 180) / 360 * n, (1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n

def tile(x, y):
    p = cache / f"{Z}_{x}_{y}.jpg"
    if not p.exists():
        r = requests.get(TILES.format(z=Z, x=x, y=y), timeout=60); r.raise_for_status()
        p.write_bytes(r.content); time.sleep(0.1)
    return Image.open(p).convert("RGB")

def view(lon, lat):
    cx, cy = tile_xy(lon, lat)
    half = SIDE_M / 2 / (40075016.7 * math.cos(math.radians(lat)) / 2 ** Z)
    x0, x1, y0, y1 = math.floor(cx - half), math.floor(cx + half), math.floor(cy - half), math.floor(cy + half)
    mosaic = Image.new("RGB", ((x1 - x0 + 1) * 256, (y1 - y0 + 1) * 256))
    for tx in range(x0, x1 + 1):
        for ty in range(y0, y1 + 1):
            mosaic.paste(tile(tx, ty), ((tx - x0) * 256, (ty - y0) * 256))
    box = [(cx - half - x0) * 256, (cy - half - y0) * 256, (cx + half - x0) * 256, (cy + half - y0) * 256]
    img = mosaic.crop([round(v) for v in box]).resize((PX, PX), Image.LANCZOS)
    def to_px(lo, la):
        tx, ty = tile_xy(lo, la)
        return ((tx - (cx - half)) / (2 * half) * PX, (ty - (cy - half)) / (2 * half) * PX)
    return img, to_px

g = Graph(npz)
recs = [json.loads(l) for l in gzip.open(detail, "rt")]
cand = [r for r in recs if r["verdict"].get("cause") and r["verdict"].get("at") and r["snap_apart_m"] <= 25]
rng = np.random.default_rng(SEED)
picked = []
for cause in sorted({r["verdict"]["cause"] for r in cand}):
    pool = [r for r in cand if r["verdict"]["cause"] == cause]
    # spread over areas: shuffle, then take round-robin by area
    by_area = {}
    for i in rng.permutation(len(pool)):
        by_area.setdefault(pool[i]["area"], []).append(pool[i])
    take = []
    while len(take) < PER and any(by_area.values()):
        for a in sorted(by_area):
            if by_area[a] and len(take) < PER:
                take.append(by_area[a].pop())
    picked += take
order = rng.permutation(len(picked))      # sheet numbers carry no information about the cause
picked = [picked[i] for i in order]
want = {r["id"] for r in picked}
ors = {}
for l in gzip.open(arm_a, "rt"):
    r = json.loads(l)
    if r["id"] in want and r["config"] == "rec_i10_k6":
        ors[r["id"]] = r
ours = pickle.load(open(ours_pkl, "rb"))
ramps = [f["geometry"]["coordinates"][:2] for f in json.load(open(nodes_geojson))["features"] if f["properties"].get("barrier") == "kerb"]
rtree = cKDTree(np.array(ramps) * [EAST, NORTH])
key = []
for k, r in enumerate(picked, 1):
    lon, lat = r["verdict"]["at"]
    img, to_px = view(lon, lat)
    d = ImageDraw.Draw(img)
    if ours[r["id"]]["wheelchair"]["edges"]:
        d.line([to_px(*c) for c in g.line(ours[r["id"]]["wheelchair"]["edges"])], fill=(60, 140, 255), width=4)
    d.line([to_px(*c) for c in ors[r["id"]]["coords"]], fill=(255, 140, 0), width=4)
    for i in rtree.query_ball_point([lon * EAST, lat * NORTH], SIDE_M):
        x, y = to_px(*ramps[i]); d.rectangle([x - 6, y - 6, x + 6, y + 6], outline=(255, 255, 255), width=3)
    x, y = to_px(lon, lat); d.ellipse([x - 28, y - 28, x + 28, y + 28], outline=(255, 235, 0), width=4)
    d.rectangle([0, 0, PX, 30], fill=(0, 0, 0)); d.text((8, 4), f"sheet {k:03d}   {SIDE_M} m across   north up   NYC Orthos 2024 (NYC OTI, CC BY 4.0)", fill=(255, 255, 255), font=FONT)
    img.save(out / "sheets" / f"sheet_{k:03d}.jpg", quality=88)
    key.append({"sheet": k, "id": r["id"], "area": r["area"], "status": r["verdict"]["status"], "cause": r["verdict"]["cause"], "detail": r["verdict"]["detail"],
                "at": [lon, lat], "arm_b_matches_ours": r["verdict"].get("arm_b_matches_ours"), "first_barrier": r["routes"]["ors_a_rec_i10_k6"].get("first_barrier")})
json.dump(key, open(out / "key.json", "w"), indent=1)
print(len(key), "sheets")
