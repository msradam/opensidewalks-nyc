"""Stage 1: Acquire raw data from all five sources.

Input:  config/sources.yaml, config/build.yaml
Output: data/raw/{source_id}/ files + data/raw/manifest.json

Caches by content hash. Re-running skips files that haven't changed upstream.
All raw files are stored as-downloaded; no transformation happens here.

OSM data is queried borough-by-borough via OSMnx to manage memory, saved as
GraphML files (one per borough) plus a merged nodes/edges GeoJSON pair.

Socrata sources are paginated via the Socrata REST API (sodapy or raw requests).
MTA ADA data falls back to GTFS stops.txt if the Open Data endpoint is missing.
"""

import hashlib
import io
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import click
import geopandas as gpd
import osmnx as ox
import pandas as pd
import requests
import yaml
from shapely.geometry import mapping, shape
from tqdm import tqdm

from pipeline.utils.provenance import load_manifest, save_manifest, record_source


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _socrata_fetch_all(domain: str, dataset_id: str, app_token: str | None,
                       page_size: int = 10000,
                       bbox: dict | None = None) -> list[dict]:
    """Paginate through a Socrata dataset and return all rows as dicts.

    If bbox is provided (keys: south, west, north, east), adds a server-side
    within_box spatial filter so only features inside the bbox are returned.
    """
    base_url = f"https://{domain}/resource/{dataset_id}.json"
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    extra_params = {}
    if bbox:
        # Socrata within_box: within_box(geom_col, south, west, north, east)
        extra_params["$where"] = (
            f"within_box(the_geom, {bbox['south']}, {bbox['west']}, "
            f"{bbox['north']}, {bbox['east']})"
        )

    rows = []
    offset = 0
    pbar = tqdm(desc=f"  Socrata {dataset_id}", unit=" rows", leave=False)

    while True:
        params = {"$limit": page_size, "$offset": offset, **extra_params}
        resp = requests.get(base_url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        pbar.update(len(batch))
        offset += len(batch)
        if len(batch) < page_size:
            break

    pbar.close()
    return rows


def _rows_to_geojson(rows: list[dict], geometry_field: str) -> dict:
    """Convert Socrata rows (with GeoJSON geometry embedded) to a FeatureCollection."""
    features = []
    for row in rows:
        geom_raw = row.get(geometry_field)
        if geom_raw is None:
            continue
        if isinstance(geom_raw, str):
            geom_raw = json.loads(geom_raw)
        props = {k: v for k, v in row.items() if k != geometry_field
                 and not k.startswith(":@computed")}
        features.append({
            "type": "Feature",
            "geometry": geom_raw,
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Source-specific acquisition functions
# ---------------------------------------------------------------------------

def acquire_boroughs(source_cfg: dict, out_dir: Path, app_token: str | None,
                     manifest: dict, bbox: dict | None = None) -> Path:
    """Fetch NYC borough boundaries.

    When bbox is provided, skips Socrata/OSMnx entirely and synthesises a single
    rectangular polygon covering the study area. Downstream stages only need a
    polygon for borough tagging and region metadata.
    """
    out_file = out_dir / "boroughs.geojson"

    if bbox:
        from shapely.geometry import box as shapely_box
        click.echo(f"  Study area mode: synthesising bbox polygon for boroughs...")
        rect = shapely_box(bbox["west"], bbox["south"], bbox["east"], bbox["north"])
        gdf = gpd.GeoDataFrame(
            [{"boro_name": "study_area", "geometry": rect}],
            geometry="geometry", crs="EPSG:4326",
        )
        raw_bytes = gdf.to_json(indent=2).encode()
        out_file.write_bytes(raw_bytes)
        ch = _sha256_bytes(raw_bytes)
        record_source(manifest, "nyc_boroughs", str(out_file), ch, row_count=1)
        click.echo(f"    Saved synthetic bbox polygon → {out_file}")
        return out_file

    click.echo("  Acquiring NYC borough boundaries...")
    retrieval = source_cfg["retrieval"]
    domain     = retrieval["domain"]
    dataset_id = retrieval["dataset_id"]
    geom_field = retrieval["geometry_field"]

    try:
        rows = _socrata_fetch_all(domain, dataset_id, app_token,
                                  page_size=retrieval.get("page_size", 10))
        if not rows:
            raise ValueError("Empty response from Socrata borough boundaries")
        fc = _rows_to_geojson(rows, geom_field)
        raw_bytes = json.dumps(fc, indent=2).encode()
        out_file.write_bytes(raw_bytes)
        click.echo(f"    Fetched {len(fc['features'])} borough polygons from Socrata")

    except Exception as exc:
        click.echo(f"    Socrata failed ({exc}), falling back to OSMnx geocoding...")
        import time
        fallback_queries = retrieval.get("fallback_queries", [])
        gdfs = []
        for query in fallback_queries:
            for attempt in range(3):
                try:
                    gdf = ox.geocode_to_gdf(query)
                    gdf["boro_name"] = query.split(",")[0].strip()
                    gdfs.append(gdf[["boro_name", "geometry"]])
                    click.echo(f"      Geocoded: {query.split(',')[0].strip()}")
                    break
                except Exception as e:
                    if attempt < 2:
                        click.echo(f"      Retry {attempt+1}/3 for {query}: {e}")
                        time.sleep(2)
                    else:
                        click.echo(f"      Warning: geocode failed for {query}: {e}")

        if not gdfs:
            raise RuntimeError("Could not acquire borough boundaries via any method") from exc

        merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs="EPSG:4326")
        raw_bytes = merged.to_json(indent=2).encode()
        out_file.write_bytes(raw_bytes)
        click.echo(f"    Saved {len(merged)} borough polygons via OSMnx fallback")

    content_hash = _sha256_bytes(raw_bytes)
    record_source(manifest, "nyc_boroughs", str(out_file), content_hash,
                  row_count=len(json.loads(raw_bytes).get("features", [])))
    click.echo(f"    Saved: {out_file}")
    return out_file


def acquire_osm(source_cfg: dict, boroughs_file: Path, out_dir: Path,
                manifest: dict, bbox: dict | None = None) -> Path:
    """Fetch OSM walking infrastructure via OSMnx.

    City-wide mode: queries each borough polygon separately (15-30 min).
    Study area mode: single graph_from_bbox query (seconds).
    """
    retrieval     = source_cfg["retrieval"]
    custom_filter = " ".join(retrieval["custom_filter"].split())
    retain_all    = retrieval.get("retain_all", True)
    simplify      = retrieval.get("simplify", False)

    # Extend OSMnx's default useful_tags_way to preserve pedestrian sub-tags.
    extra_tags = ["footway", "crossing", "surface", "sidewalk", "tactile_paving",
                  "kerb", "foot", "wheelchair"]
    ox.settings.useful_tags_way = list(
        dict.fromkeys(ox.settings.useful_tags_way + extra_tags)
    )

    nodes_file = out_dir / "osm_nodes.geojson"
    edges_file = out_dir / "osm_edges.geojson"

    if bbox:
        graphml_file = out_dir / "osm_study_area.graphml"
        if graphml_file.exists():
            click.echo(f"  OSM study area: cache hit, loading {graphml_file.name}")
            G = ox.load_graphml(graphml_file)
        else:
            click.echo(f"  OSM study area: querying bbox "
                       f"({bbox['south']}, {bbox['west']}) → "
                       f"({bbox['north']}, {bbox['east']})...")
            G = ox.graph_from_bbox(
                (bbox["west"], bbox["south"], bbox["east"], bbox["north"]),
                custom_filter=custom_filter,
                retain_all=retain_all,
                simplify=simplify,
            )
            ox.save_graphml(G, graphml_file)
            click.echo(f"    {len(G.nodes)} nodes, {len(G.edges)} edges → {graphml_file.name}")

        nodes_gdf, edges_gdf = ox.graph_to_gdfs(G)
        nodes_gdf["ext:borough"] = "study_area"
        edges_gdf["ext:borough"] = "study_area"

        combined_nodes = gpd.GeoDataFrame(nodes_gdf, crs="EPSG:4326")
        combined_edges = gpd.GeoDataFrame(edges_gdf, crs="EPSG:4326")

    else:
        click.echo("  Acquiring OSM walking infrastructure (this may take 15-30 min)...")

        boroughs_gdf = gpd.read_file(boroughs_file)
        if boroughs_gdf.crs is None or boroughs_gdf.crs.to_epsg() != 4326:
            boroughs_gdf = boroughs_gdf.to_crs("EPSG:4326")

        name_col = next(
            (c for c in ["boro_name", "BoroName", "name"] if c in boroughs_gdf.columns),
            None
        )
        all_nodes_gdfs = []
        all_edges_gdfs = []

        for idx, row in boroughs_gdf.iterrows():
            boro_name    = str(row[name_col]).replace(" ", "_").lower() if name_col else f"boro_{idx}"
            graphml_file = out_dir / f"osm_{boro_name}.graphml"

            if graphml_file.exists():
                click.echo(f"    {boro_name}: cache hit, loading existing GraphML")
                G = ox.load_graphml(graphml_file)
            else:
                click.echo(f"    {boro_name}: querying OSM...")
                G = ox.graph_from_polygon(
                    row.geometry,
                    custom_filter=custom_filter,
                    retain_all=retain_all,
                    simplify=simplify,
                )
                ox.save_graphml(G, graphml_file)
                click.echo(f"    {boro_name}: {len(G.nodes)} nodes, {len(G.edges)} edges "
                           f"→ {graphml_file.name}")

            nodes_gdf, edges_gdf = ox.graph_to_gdfs(G)
            nodes_gdf["ext:borough"] = boro_name
            edges_gdf["ext:borough"] = boro_name
            all_nodes_gdfs.append(nodes_gdf)
            all_edges_gdfs.append(edges_gdf)

        combined_nodes = gpd.GeoDataFrame(pd.concat(all_nodes_gdfs), crs="EPSG:4326")
        combined_edges = gpd.GeoDataFrame(pd.concat(all_edges_gdfs), crs="EPSG:4326")

    combined_nodes.reset_index().to_file(nodes_file, driver="GeoJSON")
    combined_edges.reset_index().to_file(edges_file, driver="GeoJSON")
    click.echo(f"    Combined: {len(combined_nodes)} nodes, {len(combined_edges)} edges")
    click.echo(f"    Saved: {nodes_file}, {edges_file}")

    ch = _sha256_file(edges_file)
    record_source(manifest, "osm_walk", str(edges_file), ch, row_count=len(combined_edges))
    return edges_file


def acquire_socrata_source(source_id: str, source_cfg: dict, out_dir: Path,
                           app_token: str | None, manifest: dict,
                           bbox: dict | None = None) -> Path:
    """Generic Socrata acquisition: paginate, convert to GeoJSON, save.

    When bbox is provided, adds a within_box server-side spatial filter so only
    features inside the study area are downloaded.
    """
    retrieval  = source_cfg["retrieval"]
    domain     = retrieval["domain"]
    dataset_id = retrieval["dataset_id"]
    page_size  = retrieval.get("page_size", 10000)
    geom_field = retrieval.get("geometry_field", "the_geom")

    bbox_label = f" (bbox-filtered)" if bbox else ""
    click.echo(f"  Acquiring {source_id} from Socrata dataset {dataset_id}{bbox_label}...")
    rows = _socrata_fetch_all(domain, dataset_id, app_token, page_size, bbox=bbox)
    click.echo(f"    Fetched {len(rows)} rows")

    fc = _rows_to_geojson(rows, geom_field)
    raw_bytes = json.dumps(fc, indent=2).encode()

    out_file = out_dir / f"{source_id}.geojson"
    out_file.write_bytes(raw_bytes)

    ch = _sha256_bytes(raw_bytes)
    record_source(manifest, source_id, str(out_file), ch,
                  row_count=len(fc["features"]))
    click.echo(f"    {len(fc['features'])} features with geometry → {out_file.name}")
    return out_file


def acquire_mta_ada(source_cfg: dict, out_dir: Path, app_token: str | None,
                    manifest: dict, bbox: dict | None = None) -> Path | None:
    """Acquire MTA ADA station data.

    Tries NYC Open Data first; falls back to MTA GTFS stops.txt if unavailable.
    This is sidecar/annotation data. Failure is non-fatal (returns None).
    """
    retrieval = source_cfg["retrieval"]
    out_file  = out_dir / "mta_ada_stations.geojson"

    # Attempt 1: NYC Open Data subway stations dataset.
    # drh3-e2fd (MTA Subway Entrances) returns 404/empty on the standard Socrata
    # endpoint as of 2026. Go straight to GTFS fallback which is more reliable.
    click.echo("  Acquiring MTA ADA station data (trying NYC Open Data first)...")
    try:
        primary_id = retrieval.get("primary_dataset_id", "drh3-e2fd")
        domain     = retrieval.get("primary_domain", "data.cityofnewyork.us")
        page_size  = retrieval.get("page_size", 10000)
        rows = _socrata_fetch_all(domain, primary_id, app_token, page_size)
        # Only use the result if it actually returned rows with geometry.
        fc = _rows_to_geojson(rows, retrieval.get("geometry_field", "the_geom"))
        if len(fc["features"]) > 0:
            raw_bytes = json.dumps(fc, indent=2).encode()
            out_file.write_bytes(raw_bytes)
            ch = _sha256_bytes(raw_bytes)
            record_source(manifest, "mta_ada_stations", str(out_file), ch,
                          row_count=len(fc["features"]))
            click.echo(f"    {len(fc['features'])} stations → {out_file.name}")
            return out_file
        click.echo("    NYC Open Data returned no geometry features, trying GTFS...")
    except Exception as exc:
        click.echo(f"    NYC Open Data attempt failed: {exc}, trying GTFS...")

    # Attempt 2: MTA GTFS static feed. Stops.txt with wheelchair_boarding column.
    fallback_url = retrieval.get("fallback_url",
                                 "http://web.mta.info/developers/data/nyct/subway/google_transit.zip")
    try:
        click.echo(f"  Falling back to MTA GTFS: {fallback_url}")
        resp = requests.get(fallback_url, timeout=120)
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            if "stops.txt" not in zf.namelist():
                raise FileNotFoundError("stops.txt not in GTFS zip")
            stops_df = pd.read_csv(zf.open("stops.txt"))

        # Filter to ADA-accessible stops (wheelchair_boarding == 1).
        ada_col = next(
            (c for c in stops_df.columns if "wheelchair" in c.lower()),
            None
        )
        if ada_col:
            ada_stops = stops_df[stops_df[ada_col] == 1].copy()
        else:
            ada_stops = stops_df.copy()
            click.echo("    Warning: no wheelchair_boarding column found; keeping all stops")

        features = []
        for _, s in ada_stops.iterrows():
            lat = float(s.get("stop_lat", 0))
            lon = float(s.get("stop_lon", 0))
            if lat == 0 and lon == 0:
                continue
            if bbox and not (bbox["south"] <= lat <= bbox["north"]
                             and bbox["west"] <= lon <= bbox["east"]):
                continue
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "stop_id": str(s.get("stop_id", "")),
                    "name": str(s.get("stop_name", "")),
                    "ada": 1,
                },
            })

        fc = {"type": "FeatureCollection", "features": features}
        raw_bytes = json.dumps(fc, indent=2).encode()
        out_file.write_bytes(raw_bytes)
        ch = _sha256_bytes(raw_bytes)
        record_source(manifest, "mta_ada_stations", str(out_file), ch,
                      row_count=len(features))
        click.echo(f"    {len(features)} ADA stations from GTFS → {out_file.name}")
        return out_file

    except Exception as exc:
        click.echo(f"  Warning: MTA ADA acquisition failed ({exc}). "
                   "Skipping. Annotation will be absent from output.")
        return None


# ---------------------------------------------------------------------------
# DEM acquisition via NYC 2017 1m LiDAR (NY State GIS ImageServer)
# ---------------------------------------------------------------------------

_NYC_DEM_URL = (
    "https://elevation.its.ny.gov/arcgis/rest/services/"
    "NYC_TopoBathymetric_2017_1_meter/ImageServer/exportImage"
)
# The ImageServer rejects responses over ~23 MB (~3000×3000 px is the safe cap).
_NYC_DEM_MAX_PX = 3000


def _fetch_dem_tile(b: dict, label: str, out_file: Path,
                    resolution_m: float) -> bool:
    """Download one DEM tile for bbox b, return True on success."""
    import math
    lat_mid  = (b["north"] + b["south"]) / 2
    width_m  = (b["east"] - b["west"]) * 111319 * math.cos(math.radians(lat_mid))
    height_m = (b["north"] - b["south"]) * 111319
    width_px  = min(int(width_m  / resolution_m), _NYC_DEM_MAX_PX)
    height_px = min(int(height_m / resolution_m), _NYC_DEM_MAX_PX)
    eff_res   = max(width_m / width_px, height_m / height_px)

    click.echo(
        f"  DEM [{label}]: {width_px}×{height_px} px "
        f"(~{eff_res:.1f}m res)..."
    )
    resp = requests.get(
        _NYC_DEM_URL,
        params={
            "bbox":      f"{b['west']},{b['south']},{b['east']},{b['north']}",
            "bboxSR":    "4326",
            "imageSR":   "4326",
            "size":      f"{width_px},{height_px}",
            "format":    "tiff",
            "pixelType": "F32",
            "f":         "image",
        },
        timeout=120,
    )
    resp.raise_for_status()
    if len(resp.content) < 1000 or b"error" in resp.content[:100]:
        raise ValueError(f"Server returned error: {resp.content[:120]}")
    out_file.write_bytes(resp.content)
    return True


def acquire_dem(out_dir: Path, manifest: dict, bbox: dict | None = None,
                resolution_m: float = 1.0) -> list[Path]:
    """Download NYC 2017 1m bare-earth LiDAR DEM (NY State GIS ImageServer).

    Study area mode: single tile clipped to study bbox.
    City-wide mode: one tile per borough (ImageServer caps at ~4000×4000 px;
    tiling keeps effective resolution at 4-6m per borough vs ~12m for one tile).

    Returns list of downloaded tile paths (may be empty on total failure).
    Non-fatal. Missing tiles mean incline is absent for that area.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    if bbox:
        out_file = out_dir / "dem.tif"
        if out_file.exists():
            click.echo(f"  DEM: cache hit → {out_file.name}")
            return [out_file]
        try:
            _fetch_dem_tile(bbox, "study area", out_file, resolution_m)
            size_kb = out_file.stat().st_size // 1024
            ch = _sha256_file(out_file)
            record_source(manifest, "dem_nyc", str(out_file), ch, row_count=0)
            click.echo(f"  DEM: {size_kb} KB → {out_file.name}")
            return [out_file]
        except Exception as exc:
            click.echo(f"  Warning: DEM acquisition failed ({exc}). Incline will be absent.")
            if out_file.exists():
                out_file.unlink()
            return []

    # City-wide: tile per borough using the acquired borough boundaries.
    boroughs_file = out_dir.parent / "nyc_boroughs" / "boroughs.geojson"
    if not boroughs_file.exists():
        click.echo("  Warning: borough boundaries not found; cannot tile DEM. Skipping.")
        return []

    try:
        boroughs_gdf = gpd.read_file(boroughs_file)
    except Exception as exc:
        click.echo(f"  Warning: could not read borough boundaries ({exc}). Skipping DEM.")
        return []

    name_col = next(
        (c for c in ["boro_name", "boroname", "name"] if c in boroughs_gdf.columns), None
    )
    tiles = []
    for _, row in boroughs_gdf.iterrows():
        boro = str(row[name_col]).lower().replace(" ", "_") if name_col else "boro"
        out_file = out_dir / f"dem_{boro}.tif"
        if out_file.exists():
            click.echo(f"  DEM [{boro}]: cache hit")
            tiles.append(out_file)
            continue
        bounds = row.geometry.bounds  # (minx, miny, maxx, maxy)
        b = {"west": bounds[0], "south": bounds[1],
             "east": bounds[2], "north": bounds[3]}
        try:
            import time as _time
            _time.sleep(1)  # brief pause between borough requests
            _fetch_dem_tile(b, boro, out_file, resolution_m)
            size_kb = out_file.stat().st_size // 1024
            ch = _sha256_file(out_file)
            record_source(manifest, f"dem_{boro}", str(out_file), ch, row_count=0)
            click.echo(f"  DEM [{boro}]: {size_kb} KB → {out_file.name}")
            tiles.append(out_file)
        except Exception as exc:
            click.echo(f"  Warning: DEM tile {boro} failed ({exc}). Skipping.")
            if out_file.exists():
                out_file.unlink()

    return tiles


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def run(sources: dict, build_cfg: dict, repo_root: Path) -> None:
    """Stage 1: download raw data for all sources."""
    raw_dir = repo_root / build_cfg["dirs"]["raw"]
    raw_dir.mkdir(parents=True, exist_ok=True)

    app_token = os.environ.get(build_cfg.get("socrata_app_token_env", "SOCRATA_APP_TOKEN"))
    if not app_token:
        click.echo("  Note: SOCRATA_APP_TOKEN not set. Using anonymous Socrata access "
                   "(rate-limited). Set the env var to improve download speed.")

    # Study area: when set, scope all sources to the bbox.
    study_area = build_cfg.get("study_area")
    bbox = study_area["bbox"] if study_area else None
    if bbox:
        click.echo(f"  Study area: {study_area.get('name', 'custom')}. "
                   f"{study_area.get('description', '')} "
                   f"[{bbox['south']}, {bbox['west']} → {bbox['north']}, {bbox['east']}]")

    manifest    = load_manifest(raw_dir)
    source_defs = sources["sources"]

    # Source 1: Borough boundaries.
    boro_out_dir = raw_dir / "nyc_boroughs"
    boro_out_dir.mkdir(exist_ok=True)
    boroughs_file = acquire_boroughs(
        source_defs["nyc_boroughs"], boro_out_dir, app_token, manifest, bbox=bbox
    )

    # Source 2: OSM walking infrastructure.
    osm_out_dir = raw_dir / "osm_walk"
    osm_out_dir.mkdir(exist_ok=True)
    acquire_osm(source_defs["osm_walk"], boroughs_file, osm_out_dir, manifest, bbox=bbox)

    # Source 3: NYC DOT pedestrian ramps.
    ramps_out_dir = raw_dir / "nyc_dot_ramps"
    ramps_out_dir.mkdir(exist_ok=True)
    acquire_socrata_source(
        "nyc_dot_ramps", source_defs["nyc_dot_ramps"],
        ramps_out_dir, app_token, manifest, bbox=bbox
    )

    # Source 4: NYC Planimetric sidewalk polygons.
    planimetric_out_dir = raw_dir / "nyc_planimetric_sidewalks"
    planimetric_out_dir.mkdir(exist_ok=True)
    acquire_socrata_source(
        "nyc_planimetric_sidewalks", source_defs["nyc_planimetric_sidewalks"],
        planimetric_out_dir, app_token, manifest, bbox=bbox
    )

    # Source 5: MTA ADA stations (non-fatal if unavailable).
    mta_out_dir = raw_dir / "mta_ada_stations"
    mta_out_dir.mkdir(exist_ok=True)
    acquire_mta_ada(source_defs["mta_ada_stations"], mta_out_dir, app_token, manifest,
                    bbox=bbox)

    # Source 6: USGS 3DEP DEM raster (non-fatal. Used for incline computation).
    dem_out_dir = raw_dir / "dem_nyc"
    resolution_m = build_cfg.get("dem_resolution_meters", 10)
    acquire_dem(dem_out_dir, manifest, bbox=bbox, resolution_m=resolution_m)

    save_manifest(raw_dir, manifest)
    click.echo(f"\n  Manifest written to {raw_dir / 'manifest.json'}")
    click.echo(f"  Sources acquired: {len(manifest)}")
