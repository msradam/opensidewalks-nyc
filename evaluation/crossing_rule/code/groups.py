"""Crossings as the routing layer sees them: crossing edges joined where they
meet at a node no sidewalk or footway reaches, with the nodes where the group
meets the rest of the pedestrian network (its ends).

Same grouping as scripts/osw_to_unweaver.py. crossing_groups(E) returns
(edge table, crossing table): one row per crossing edge with its crossing
number, and one row per crossing with its edge count, end count and borough.
"""
import numpy as np, pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components


def crossing_groups(E):
    cross = ((E.highway == "footway") & (E.footway == "crossing")).values
    walk = E.highway.isin(["footway", "steps"]).values & ~cross
    walk_nodes = pd.Index(np.unique(np.r_[E._u_id.values[walk], E._v_id.values[walk]]))
    C = E[cross]
    inv, ids = pd.factorize(np.r_[C._u_id.values, C._v_id.values])
    m = len(C); u, v = inv[:m], inv[m:]
    inner = ~pd.Index(ids).isin(walk_nodes)
    # Edges are joined through inner nodes only: a bipartite graph of edges
    # and the inner nodes they touch.
    ku, kv = np.flatnonzero(inner[u]), np.flatnonzero(inner[v])
    g = coo_matrix((np.ones(len(ku) + len(kv)), (np.r_[ku, kv], m + np.r_[u[ku], v[kv]])),
                   shape=(m + len(ids), m + len(ids)))
    lab = connected_components(g, directed=False)[1][:m]
    edges = pd.DataFrame({"edge": C.index.values, "crossing": lab, "u": ids[u], "v": ids[v],
                          "u_end": ~inner[u], "v_end": ~inner[v], "borough": C["ext:borough"].values})
    ends = pd.concat([edges.loc[edges.u_end, ["crossing", "u"]].set_axis(["crossing", "node"], axis=1),
                      edges.loc[edges.v_end, ["crossing", "v"]].set_axis(["crossing", "node"], axis=1)]).drop_duplicates()
    crossings = edges.groupby("crossing").agg(edges=("edge", "size"), borough=("borough", "first"))
    crossings["ends"] = ends.groupby("crossing").size().reindex(crossings.index, fill_value=0)
    return edges, ends, crossings
