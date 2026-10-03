An OSW file has one edge per travel direction with a signed incline, so an undirected graph, or a DiGraph with a copied reverse edge, keeps the wrong sign for half the edges.

to_graphml.py used nx.Graph (second direction overwrote the first, parallel edges vanished) and Stage 6 added a reverse copy of each edge (overwriting the real reverse edge and opening one-way streets). Use a MultiDiGraph with one edge per OSW edge. Also: nx.read_graphml turns an all-digit edge key into an integer, so keep the OSW ID as an attribute, not the key.
