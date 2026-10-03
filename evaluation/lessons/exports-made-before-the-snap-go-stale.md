An export written before a later step edits the canonical file disagrees with it, and nothing checks.

Stage 6 writes the GraphML and routing JSON from the unsnapped file; snap_endpoints.py then moves edge ends and rounds coordinates in the GeoJSON only. Make release assets from the snapped file (scripts/to_graphml.py, scripts/to_routing_json.py) and compare every asset with the canonical one before shipping. Evidence: research_notes/release/city/verify_assets.json.
