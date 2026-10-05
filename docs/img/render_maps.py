# Renders the README maps from a release FlatGeobuf.
# usage: python render_maps.py nyc-osw.fgb OUT.png TITLE "minx,miny,maxx,maxy" [incline|structure]
import sys, geopandas as gpd, matplotlib.pyplot as plt, numpy as np
from matplotlib.lines import Line2D
fgb, out, title = sys.argv[1], sys.argv[2], sys.argv[3]
mode = sys.argv[5] if len(sys.argv) > 5 else "incline"
bbox = tuple(map(float, sys.argv[4].split(",")))
g = gpd.read_file(fgb, bbox=bbox).to_crs(3857)
e = g[g.geom_type == "LineString"]; n = g[g.geom_type == "Point"]; z = g[g.geom_type == "Polygon"]
ped = e[e["highway"].isin(["footway", "steps", "pedestrian"])]
street = e[~e.index.isin(ped.index)]
inc = ped["incline"].abs() * 100
cls = np.select([inc.isna(), inc <= 5, inc <= 8.33], ["#b8b8b8", "#2b7a3d", "#d69a00"], "#c0392b")
x0, y0, x1, y1 = gpd.GeoSeries.from_xy([bbox[0], bbox[2]], [bbox[1], bbox[3]], crs=4326).to_crs(3857).total_bounds
W = 10; fig, ax = plt.subplots(figsize=(W, W * (y1 - y0) / (x1 - x0) + 0.6), dpi=160)
fig.patch.set_facecolor("#fafaf7"); ax.set_facecolor("#fafaf7")
street.plot(ax=ax, color="#dddddd", linewidth=0.6)
if len(z):
    z.plot(ax=ax, facecolor="#d9cfe8", edgecolor="#7a5c99", linewidth=0.8, alpha=0.8)
if mode == "incline":
    ped.plot(ax=ax, color=cls, linewidth=1.3)
    h = [Line2D([], [], color=c, lw=2.5, label=l) for c, l in [("#2b7a3d", "incline up to 5%"), ("#d69a00", "5% to 8.3%"), ("#c0392b", "over 8.3%"), ("#b8b8b8", "no incline")]]
else:
    kind = np.select([ped["footway"] == "sidewalk", ped["footway"] == "crossing", ped["highway"] == "steps", ped["highway"] == "pedestrian"], ["#333333", "#1f6fd1", "#c0392b", "#7a5c99"], "#8a8a8a")
    ped.plot(ax=ax, color=kind, linewidth=2.2)
    h = [Line2D([], [], color=c, lw=2.5, label=l) for c, l in [("#333333", "Sidewalk Edge"), ("#1f6fd1", "Crossing Edge"), ("#8a8a8a", "Footway Edge"), ("#c0392b", "Steps Edge"), ("#7a5c99", "Pedestrian Road Edge"), ("#dddddd", "Road Edge")]]
    h.append(plt.Rectangle((0, 0), 1, 1, facecolor="#d9cfe8", edgecolor="#7a5c99", label="Pedestrian Zone"))
ramps = n[n.get("barrier") == "kerb"] if "barrier" in n else n.iloc[0:0]
ramps.plot(ax=ax, color="#3b2f8f", markersize=3 if mode == "incline" else 22, zorder=3)
ax.set_xlim(x0, x1); ax.set_ylim(y0, y1); ax.set_axis_off()
h.append(Line2D([], [], color="#3b2f8f", marker="o", lw=0, ms=4, label="Curb Ramp Node" if mode != "incline" else "surveyed curb ramp"))
ax.legend(handles=h, loc="lower right", frameon=True, fontsize=9, facecolor="white")
ax.set_title(title, loc="left", fontsize=12)
fig.text(0.01, 0.01, "Map data \u00a9 OpenStreetMap contributors (openstreetmap.org/copyright), ODbL. Also NYC DOT, NYC OTI, NYS GIS, NOAA. OpenSidewalks NYC v0.3.6", fontsize=7, color="#555")
fig.tight_layout(); fig.savefig(out, facecolor=fig.get_facecolor()); print(len(ped), len(ramps), inc.describe().round(1).to_dict())
