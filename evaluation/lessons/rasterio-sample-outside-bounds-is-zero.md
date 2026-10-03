rasterio's sample() returns 0.0, not nodata, for a point outside a raster that has no nodata value.

With several DEM tiles and a first-valid-value-wins loop, the first tile assigns 0.0 to every point outside it. Test bounds before sampling. Evidence: research_notes/evidence/repro_si/verify_dem_fix.json.
