Querying OSM one polygon at a time with OSMnx drops every edge that crosses a polygon boundary unless truncate_by_edge=True.

The release's five borough graphs share 13 nodes; every bridge is cut at the borough line. A component count will not show this unless it is broken down by borough. Evidence: research_notes/evidence/borough_gap/truncate_experiment.json, release_interborough_nodes.csv.
